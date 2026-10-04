mod common;

use std::sync::Arc;
use std::time::Duration;

use bt_core::engine::State;
use bt_core::error::SessionError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use bt_core::peer::handshake::{self, Handshake};
use bt_core::session::{Session, SessionOptions};
use common::{temp_dir, test_data, torrent_bytes, FakeDial, SeederKind};

fn addr(port: u16) -> std::net::SocketAddr {
    std::net::SocketAddr::from(([127, 0, 0, 1], port))
}

async fn wait_for(
    stats: &tokio::sync::watch::Receiver<Vec<bt_core::session::TorrentSummary>>,
    condition: impl Fn(&[bt_core::session::TorrentSummary]) -> bool + 'static,
    seconds: u64,
) -> Vec<bt_core::session::TorrentSummary> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        let snapshot = stats.borrow().clone();
        if condition(&snapshot) {
            return snapshot;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in {seconds}s: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_rejects_duplicates_and_removes_files() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let meta_id = bt_core::hex::encode(&meta.info_hash);
    let data_dir = temp_dir("session-data");
    let out = temp_dir("session-out");
    std::fs::create_dir_all(&out).unwrap();
    let unrelated = out.join("unrelated.txt");
    std::fs::write(&unrelated, "keep me").unwrap();

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = Session::spawn_with_dial(Some(data_dir.clone()), dial, vec![])
        .await
        .unwrap();

    let id = session.add_torrent(&bytes, out.clone()).await.unwrap();
    assert_eq!(id, meta_id);
    assert!(matches!(
        session.add_torrent(&bytes, out.clone()).await,
        Err(SessionError::Duplicate(_))
    ));

    let summaries = wait_for(&session.subscribe(), |s| !s.is_empty(), 10).await;
    assert_eq!(summaries[0].id, id);
    assert_eq!(summaries[0].output_dir, out);

    let detail = session.detail(&id).await.unwrap();
    assert_eq!(detail.info_hash, id);
    assert_eq!(detail.files.len(), 1);
    assert_eq!(detail.files[0].path, "e2e.bin");
    assert!(session.detail("missing").await.is_none());

    session.remove(&id, false).await.unwrap();
    assert!(out.join("e2e.bin").exists());
    assert!(unrelated.exists());

    let id = session.add_torrent(&bytes, out.clone()).await.unwrap();
    session.remove(&id, true).await.unwrap();
    assert!(!out.join("e2e.bin").exists());
    assert!(unrelated.exists());
    assert!(out.exists());

    let summaries = wait_for(&session.subscribe(), |s| s.is_empty(), 10).await;
    assert!(summaries.is_empty());
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_completes() {
    let data: Arc<Vec<u8>> = Arc::new((0..12 * 16384).map(|i| (i % 251) as u8).collect());
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let data_dir = temp_dir("session-pause-data");
    let out = temp_dir("session-pause-out");

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![
            (addr(9201), SeederKind::Good),
            (addr(9202), SeederKind::NeverReads),
        ],
    ));
    let session =
        Session::spawn_with_dial(Some(data_dir.clone()), dial, vec![addr(9201), addr(9202)])
            .await
            .unwrap();

    let id = session.add_torrent(&bytes, out.clone()).await.unwrap();
    let summaries = session.subscribe();
    let added_id = id.clone();
    wait_for(
        &summaries,
        move |s| s.iter().any(|t| t.id == added_id && t.verified_bytes >= 1),
        30,
    )
    .await;

    let paused_id = id.clone();
    session.pause(&id).await.unwrap();
    wait_for(
        &summaries,
        move |s| {
            s.iter()
                .any(|t| t.id == paused_id && t.state == State::Paused)
        },
        10,
    )
    .await;

    session.resume(&id).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = summaries.borrow().clone();
        let entry = snapshot.iter().find(|t| t.id == id).unwrap();
        assert_ne!(entry.state, State::Checking, "resume must not recheck");
        if matches!(entry.state, State::Completed | State::Seeding) {
            assert_eq!(entry.verified_bytes, entry.total_length);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "download did not resume: {:?}",
            entry.state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persists_and_restores() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let data_dir = temp_dir("session-restore");
    let out = temp_dir("session-restore-out");

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = Session::spawn_with_dial(Some(data_dir.clone()), dial, vec![])
        .await
        .unwrap();
    let id = session.add_torrent(&bytes, out.clone()).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !data_dir.join("session.json").exists()
        || !data_dir.join(format!("{id}.torrent")).exists()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "persistence missing"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    session.shutdown().await.unwrap();

    let session2 = Session::spawn(Some(data_dir.clone())).await.unwrap();
    assert!(session2.restore_errors().is_empty());
    let summaries = wait_for(&session2.subscribe(), |s| !s.is_empty(), 10).await;
    assert_eq!(summaries[0].id, id);
    assert_eq!(summaries[0].output_dir, out);
    session2.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skips_corrupted_persistence() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let id = bt_core::hex::encode(&meta.info_hash);
    let data_dir = temp_dir("session-corrupt");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(data_dir.join("session.json"), "{not json at all").unwrap();

    let session = Session::spawn(Some(data_dir.clone())).await.unwrap();
    assert!(session.list().await.is_empty());
    assert_eq!(session.restore_errors().len(), 1);
    session.shutdown().await.unwrap();

    let data_dir2 = temp_dir("session-corrupt-entry");
    std::fs::create_dir_all(&data_dir2).unwrap();
    let session_json = format!(
        "{{\"torrents\":[{{\"id\":\"{id}\",\"file\":\"{id}.torrent\",\"output_dir\":\"{}\",\"paused\":false}},{{\"id\":\"deadbeef\",\"file\":\"deadbeef.torrent\",\"output_dir\":\"{}\",\"paused\":false}}]}}",
        out_display(&temp_dir("session-corrupt-out1")),
        out_display(&temp_dir("session-corrupt-out2")),
    );
    std::fs::write(data_dir2.join("session.json"), session_json).unwrap();
    std::fs::write(data_dir2.join(format!("{id}.torrent")), &bytes).unwrap();
    std::fs::write(data_dir2.join("deadbeef.torrent"), b"corrupt garbage").unwrap();

    let session2 = Session::spawn(Some(data_dir2.clone())).await.unwrap();
    let summaries = session2.list().await;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, id);
    assert_eq!(session2.restore_errors().len(), 1);
    assert!(session2.restore_errors()[0].contains("deadbeef"));
    session2.shutdown().await.unwrap();
}

fn out_display(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebinds_listener_and_applies_upload_limit() {
    let session = Session::spawn_with_options(None, SessionOptions::new(0, 0))
        .await
        .unwrap();
    let mut status = session.listener_status();
    while !status.borrow().active {
        assert!(status.changed().await.is_ok());
    }
    let first = status.borrow().port;
    session.set_upload_limit(2048).await.unwrap();
    session.set_listen_port(0).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = status.borrow().clone();
        if snapshot.active && snapshot.port != first {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "listener never rebound: {snapshot:?}"
        );
        if status.changed().await.is_err() {
            panic!("listener status channel closed");
        }
    }
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_rebind_keeps_original_listener_working() {
    let session = Session::spawn_with_options(None, SessionOptions::new(0, 0))
        .await
        .unwrap();
    let mut status = session.listener_status();
    while !status.borrow().active {
        assert!(status.changed().await.is_ok());
    }
    let original_port = status.borrow().port;

    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let out = temp_dir("rebind-out");
    std::fs::create_dir_all(&out).unwrap();
    session.add_torrent(&bytes, out.clone()).await.unwrap();

    let blocker = std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, 0)).unwrap();
    let taken_port = blocker.local_addr().unwrap().port();
    assert!(session.set_listen_port(taken_port).await.is_err());

    let snapshot = status.borrow().clone();
    assert!(snapshot.active);
    assert_eq!(snapshot.port, original_port);
    assert!(snapshot.error.is_none());

    let mut peer = tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, original_port))
        .await
        .unwrap();
    let request = handshake::encode(&Handshake {
        info_hash: meta.info_hash,
        reserved: [0; 8],
        peer_id: [9u8; 20],
    });
    peer.write_all(&request).await.unwrap();
    let mut reply = [0u8; 68];
    tokio::time::timeout(Duration::from_secs(5), peer.read_exact(&mut reply))
        .await
        .expect("original listener stopped answering")
        .unwrap();
    let echoed = handshake::decode(&reply).unwrap();
    assert_eq!(echoed.info_hash, meta.info_hash);

    session.set_listen_port(0).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = status.borrow().clone();
        if snapshot.active && snapshot.port != original_port {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "listener never rebound to a free port: {snapshot:?}"
        );
        if status.changed().await.is_err() {
            panic!("listener status channel closed");
        }
    }
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_magnet_persists_and_restores() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let info_hash_hex = bt_core::hex::encode(&meta.info_hash);
    let data_dir = temp_dir("magnet-persist-data");
    let out = temp_dir("magnet-persist-out");
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = Session::spawn_with_dial(Some(data_dir.clone()), dial, vec![])
        .await
        .unwrap();
    let uri = format!("magnet:?xt=urn:btih:{info_hash_hex}&dn=e2e.bin");
    let id = session.add_magnet(&uri, out.clone()).await.unwrap();
    assert_eq!(id, info_hash_hex);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let persisted = data_dir.join("session.json");
        if persisted.exists() {
            let text = std::fs::read_to_string(&persisted).unwrap();
            if text.contains("magnet") && text.contains(&uri) {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pending magnet was never persisted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    session.shutdown().await.unwrap();

    let restored = Session::spawn(Some(data_dir.clone())).await.unwrap();
    assert!(
        restored.restore_errors().is_empty(),
        "unexpected restore errors: {:?}",
        restored.restore_errors()
    );
    let summaries = wait_for(&restored.subscribe(), |s| !s.is_empty(), 10).await;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, info_hash_hex);
    assert_eq!(summaries[0].output_dir, out);
    assert_eq!(summaries[0].state, State::FetchingMetadata);
    assert_eq!(summaries[0].name, "e2e.bin");
    restored.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_magnet_is_rejected() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let info_hash_hex = bt_core::hex::encode(&meta.info_hash);
    let uri = format!("magnet:?xt=urn:btih:{info_hash_hex}&dn=e2e.bin");
    let out = temp_dir("magnet-dup-out");

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = Session::spawn_with_dial(None, dial, vec![]).await.unwrap();

    let id = session.add_magnet(&uri, out.clone()).await.unwrap();
    assert_eq!(id, info_hash_hex);
    assert!(matches!(
        session.add_magnet(&uri, out.clone()).await,
        Err(SessionError::Duplicate(_))
    ));

    session.remove(&id, false).await.unwrap();
    session.add_torrent(&bytes, out.clone()).await.unwrap();
    assert!(
        matches!(
            session.add_magnet(&uri, out.clone()).await,
            Err(SessionError::Duplicate(_))
        ),
        "a magnet must be rejected when a torrent with the same info hash exists"
    );
    session.shutdown().await.unwrap();
}

async fn wait_for_dht_status(
    mut rx: tokio::sync::watch::Receiver<bt_core::dht::DhtStatus>,
    condition: impl Fn(&bt_core::dht::DhtStatus) -> bool,
    seconds: u64,
) -> bt_core::dht::DhtStatus {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
    loop {
        let snapshot = rx.borrow().clone();
        if condition(&snapshot) {
            return snapshot;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "dht status condition not met within {seconds}s: {snapshot:?}"
        );
        if rx.changed().await.is_err() {
            panic!("dht status channel closed");
        }
    }
}

fn blocked_udp_port() -> u16 {
    let blocker = std::net::UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, 0)).unwrap();
    let port = blocker.local_addr().unwrap().port();
    std::mem::forget(blocker);
    port
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_spawns_active_and_follows_the_settings() {
    let session = Session::spawn_with_options(None, SessionOptions::new(0, 0))
        .await
        .unwrap();
    let active = wait_for_dht_status(session.dht_status(), |s| s.active, 10).await;
    assert_ne!(active.port, 0, "port 0 must bind an ephemeral port");
    assert_eq!(active.node_count, 0, "no bootstrap hosts means no nodes");

    session.set_dht(true, 0).await.unwrap();
    let first = active.port;
    let rebound =
        wait_for_dht_status(session.dht_status(), |s| s.active && s.port != first, 10).await;
    assert_ne!(rebound.port, first);

    session.set_dht(false, rebound.port).await.unwrap();
    let disabled = wait_for_dht_status(session.dht_status(), |s| !s.active, 10).await;
    assert!(disabled.error.is_none(), "disabling is not an error");
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_bind_failure_degrades_gracefully_and_recovers() {
    let blocked = blocked_udp_port();
    let mut options = SessionOptions::new(0, 0);
    options.dht_port = blocked;
    let session = Session::spawn_with_options(None, options).await.unwrap();
    let failed =
        wait_for_dht_status(session.dht_status(), |s| !s.active && s.error.is_some(), 10).await;
    assert!(
        failed
            .error
            .as_deref()
            .unwrap_or_default()
            .contains(&blocked.to_string()),
        "the status must name the port that could not be bound: {failed:?}"
    );

    session.set_dht(true, 0).await.unwrap();
    let recovered = wait_for_dht_status(session.dht_status(), |s| s.active, 10).await;
    assert_ne!(recovered.port, blocked);
    assert!(recovered.error.is_none());
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_dht_port_binds_the_new_socket_before_swapping() {
    let session = Session::spawn_with_options(None, SessionOptions::new(0, 0))
        .await
        .unwrap();
    let active = wait_for_dht_status(session.dht_status(), |s| s.active, 10).await;
    let current = active.port;

    let blocked = blocked_udp_port();
    let outcome = session.set_dht(true, blocked).await;
    assert!(outcome.is_err(), "binding a taken port must fail");
    let still = session.dht_status().borrow().clone();
    assert!(still.active, "the old socket must keep running");
    assert_eq!(still.port, current);

    let changed = session.set_dht(true, 0).await;
    assert!(changed.is_ok());
    wait_for_dht_status(
        session.dht_status(),
        move |s| s.active && s.port != current,
        10,
    )
    .await;
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_disabled_at_spawn_reports_inactive_without_error() {
    let mut options = SessionOptions::new(0, 0);
    options.dht_enabled = false;
    let session = Session::spawn_with_options(None, options).await.unwrap();
    let snapshot = session.dht_status().borrow().clone();
    assert!(!snapshot.active);
    assert!(snapshot.error.is_none());
    assert_eq!(snapshot.node_count, 0);
    session.set_dht(true, 0).await.unwrap();
    wait_for_dht_status(session.dht_status(), |s| s.active, 10).await;
    session.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_dht_state_is_reported_and_the_session_starts_fresh() {
    let data_dir = temp_dir("dht-corrupt");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(data_dir.join("dht.json"), b"{not valid dht state").unwrap();
    let session = Session::spawn_with_options(Some(data_dir.clone()), SessionOptions::new(0, 0))
        .await
        .unwrap();
    assert!(
        session
            .restore_errors()
            .iter()
            .any(|error| error.contains("corrupt DHT state")),
        "the corrupt dht state must be reported once: {:?}",
        session.restore_errors()
    );
    session.shutdown().await.unwrap();
    std::fs::remove_dir_all(data_dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_dht_state_restores_the_table_through_the_production_path() {
    let data_dir = temp_dir("dht-valid");
    std::fs::create_dir_all(&data_dir).unwrap();
    let node_id = bt_core::dht::NodeId::from_bytes([0x2A; 20]);
    let nodes = vec![bt_core::dht::NodeInfo::new(
        bt_core::dht::NodeId::from_bytes([0x2B; 20]),
        std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(93, 184, 216, 34), 6881),
    )];
    bt_core::dht::save_state(&data_dir.join("dht.json"), &node_id, &nodes).unwrap();
    let session = Session::spawn_with_options(Some(data_dir.clone()), SessionOptions::new(0, 0))
        .await
        .unwrap();
    let restored =
        wait_for_dht_status(session.dht_status(), |s| s.active && s.node_count == 1, 10).await;
    assert_eq!(restored.node_count, 1);
    assert!(session.restore_errors().is_empty());
    session.shutdown().await.unwrap();
    std::fs::remove_dir_all(data_dir).unwrap();
}
