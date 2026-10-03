mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bt_core::engine::{State, TcpDial, Torrent, TorrentOptions};
use bt_core::listener::{ListenerOptions, Registry};
use bt_core::peer_id;
use bt_core::ratelimit::UploadBucket;
use common::{
    temp_dir, test_data, torrent_meta, FakeDial, FakeLeecherDial, LeecherConfig, LeecherReport,
    SeederKind, DATA_LENGTH, PIECE_LENGTH,
};

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn leecher_dial(
    data: &Arc<Vec<u8>>,
    config: LeecherConfig,
) -> (
    Arc<FakeLeecherDial>,
    tokio::sync::mpsc::Receiver<LeecherReport>,
) {
    let meta = torrent_meta(data);
    let (dial, rx) =
        FakeLeecherDial::new(meta.info_hash, data.clone(), meta.info.pieces.len(), config);
    (Arc::new(dial), rx)
}

fn seeder_options(
    bootstrap: Vec<SocketAddr>,
    dial: Arc<dyn bt_core::engine::Dial>,
    uploads: Arc<UploadBucket>,
    choke_interval: Duration,
    optimistic_interval: Duration,
) -> TorrentOptions {
    TorrentOptions {
        bootstrap_peers: bootstrap,
        dial,
        listen_active: Arc::new(AtomicBool::new(false)),
        announce_port: Arc::new(AtomicU16::new(6881)),
        uploads,
        registry: Arc::new(Registry::default()),
        choke_interval,
        optimistic_interval,
        peer_id: *peer_id::session(),
    }
}

async fn spawn_seeder(
    name: &str,
    data: &Arc<Vec<u8>>,
    data_to_write: &[u8],
    options: TorrentOptions,
) -> (Torrent, PathBuf) {
    let meta = torrent_meta(data);
    let dir = temp_dir(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("e2e.bin"), data_to_write).unwrap();
    let torrent = Torrent::spawn_with_options(meta, dir.clone(), options)
        .await
        .unwrap();
    (torrent, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leecher_downloads_full_torrent_from_engine() {
    let data = test_data();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            request_blocks_of_all_pieces: true,
            deadline: Duration::from_secs(15),
            ..LeecherConfig::default()
        },
    );
    let (torrent, dir) = spawn_seeder(
        "seed-full",
        &data,
        &data,
        seeder_options(
            vec![addr(7001)],
            dial,
            Arc::new(UploadBucket::new(0)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    assert!(report.saw_bitfield);
    assert!(report.saw_unchoke);
    assert_eq!(report.blocks.len(), DATA_LENGTH / (16 * 1024));
    let mut reassembled = vec![0u8; DATA_LENGTH];
    for (index, begin, block) in &report.blocks {
        let offset = *index as usize * PIECE_LENGTH + *begin as usize;
        reassembled[offset..offset + block.len()].copy_from_slice(block);
    }
    assert_eq!(reassembled, *data);

    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = stats.borrow().clone();
        if snapshot.session_uploaded as usize >= DATA_LENGTH {
            break;
        }
        assert!(Instant::now() < deadline, "uploaded was never counted");
        assert!(stats.changed().await.is_ok());
    }
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unverified_piece_requests_are_ignored() {
    let data = test_data();
    let only_first_piece = data[..PIECE_LENGTH].to_vec();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            request_blocks_of_all_pieces: true,
            deadline: Duration::from_millis(2_500),
            ..LeecherConfig::default()
        },
    );
    let (torrent, dir) = spawn_seeder(
        "seed-partial",
        &data,
        &only_first_piece,
        seeder_options(
            vec![addr(7002)],
            dial,
            Arc::new(UploadBucket::new(0)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    assert!(report.saw_unchoke);
    assert_eq!(report.blocks, vec![(0, 0, data[..PIECE_LENGTH].to_vec())]);
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_range_requests_are_ignored_without_closing() {
    let data = test_data();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            requests_after_unchoke: vec![
                (99, 0, 16 * 1024),
                (0, 16 * 1024, 16 * 1024),
                (0, 0, 20 * 1024),
            ],
            deadline: Duration::from_millis(2_500),
            ..LeecherConfig::default()
        },
    );
    let (torrent, dir) = spawn_seeder(
        "seed-range",
        &data,
        &data,
        seeder_options(
            vec![addr(7004)],
            dial,
            Arc::new(UploadBucket::new(0)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    assert!(report.saw_unchoke);
    assert!(!report.closed);
    assert!(report.blocks.is_empty());
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_request_closes_connection() {
    let data = test_data();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            requests_after_unchoke: vec![(0, 0, (32 * 1024 + 1) as u32)],
            deadline: Duration::from_millis(5_000),
            ..LeecherConfig::default()
        },
    );
    let (torrent, dir) = spawn_seeder(
        "seed-oversized",
        &data,
        &data,
        seeder_options(
            vec![addr(7003)],
            dial,
            Arc::new(UploadBucket::new(0)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    assert!(report.saw_unchoke);
    assert!(report.closed);
    assert!(report.blocks.is_empty());
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn choked_peer_receives_no_data() {
    let data = test_data();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            send_interested: false,
            request_blocks_of_all_pieces: true,
            deadline: Duration::from_millis(2_000),
            ..LeecherConfig::default()
        },
    );
    let (torrent, dir) = spawn_seeder(
        "seed-choked",
        &data,
        &data,
        seeder_options(
            vec![addr(7005)],
            dial,
            Arc::new(UploadBucket::new(0)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    assert!(!report.saw_unchoke);
    assert!(report.blocks.is_empty());
    assert!(!report.closed);
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outstanding_requests_beyond_limit_are_ignored() {
    let data = test_data();
    let burst: Vec<(u32, u32, u32)> = (0..200).map(|_| (0, 0, 16 * 1024)).collect();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            requests_after_unchoke: burst,
            deadline: Duration::from_millis(3_000),
            ..LeecherConfig::default()
        },
    );
    let (torrent, dir) = spawn_seeder(
        "seed-burst",
        &data,
        &data,
        seeder_options(
            vec![addr(7006)],
            dial,
            Arc::new(UploadBucket::new(0)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    assert!(report.saw_unchoke);
    assert!(
        report.blocks.len() < 200,
        "expected extras to be ignored, got {}",
        report.blocks.len()
    );
    assert!(!report.blocks.is_empty());
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_block_is_not_served() {
    let data = test_data();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            cancel_first: Some((0, 0, 16 * 1024)),
            request_blocks_of_all_pieces: true,
            deadline: Duration::from_secs(10),
            ..LeecherConfig::default()
        },
    );
    let (torrent, dir) = spawn_seeder(
        "seed-cancel",
        &data,
        &data,
        seeder_options(
            vec![addr(7007)],
            dial,
            Arc::new(UploadBucket::new(0)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    assert!(report.saw_unchoke);
    assert!(!report
        .blocks
        .iter()
        .any(|(index, begin, _)| *index == 0 && *begin == 0));
    assert!(report.blocks.iter().any(|(index, _begin, _)| *index == 1));
    assert!(report.blocks.iter().any(|(index, _begin, _)| *index == 2));
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_limit_throttles_serving() {
    let data = test_data();
    let (dial, mut reports) = leecher_dial(
        &data,
        LeecherConfig {
            request_blocks_of_all_pieces: true,
            deadline: Duration::from_secs(20),
            ..LeecherConfig::default()
        },
    );
    let started = Instant::now();
    let (torrent, dir) = spawn_seeder(
        "seed-throttle",
        &data,
        &data,
        seeder_options(
            vec![addr(7008)],
            dial,
            Arc::new(UploadBucket::new(16 * 1024)),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ),
    )
    .await;
    let report = reports.recv().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(report.blocks.len(), 3);
    assert!(
        elapsed >= Duration::from_millis(1_800),
        "48 KiB at 16 KiB/s must take at least ~2s, took {elapsed:?}"
    );
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_engines_transfer_torrents_to_each_other_over_loopback() {
    let seed_a = Arc::new(
        (0..DATA_LENGTH)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>(),
    );
    let seed_b = Arc::new(
        (0..DATA_LENGTH)
            .map(|i| (i % 241) as u8)
            .collect::<Vec<u8>>(),
    );
    let meta_a = torrent_meta(&seed_a);
    let meta_b = torrent_meta(&seed_b);
    let id_a = peer_id::generate();
    let id_b = peer_id::generate();
    let registry_a = Arc::new(Registry::default());
    let registry_b = Arc::new(Registry::default());
    let listener_a = bt_core::listener::spawn(
        ListenerOptions {
            port: 0,
            our_peer_id: id_a,
            ..ListenerOptions::default()
        },
        registry_a.clone(),
    );
    let listener_b = bt_core::listener::spawn(
        ListenerOptions {
            port: 0,
            our_peer_id: id_b,
            ..ListenerOptions::default()
        },
        registry_b.clone(),
    );
    let mut status_a = listener_a.status();
    let mut status_b = listener_b.status();
    while !status_a.borrow().active {
        assert!(status_a.changed().await.is_ok());
    }
    while !status_b.borrow().active {
        assert!(status_b.changed().await.is_ok());
    }
    let port_a = status_a.borrow().port;
    let port_b = status_b.borrow().port;
    let dial = Arc::new(TcpDial::new(Duration::from_secs(5)));

    let dir_a1 = temp_dir("loopback-a1");
    let dir_b1 = temp_dir("loopback-b1");
    let dir_a2 = temp_dir("loopback-a2");
    let dir_b2 = temp_dir("loopback-b2");
    std::fs::create_dir_all(&dir_a1).unwrap();
    std::fs::create_dir_all(&dir_b2).unwrap();
    std::fs::write(dir_a1.join("e2e.bin"), &*seed_a).unwrap();
    std::fs::write(dir_b2.join("e2e.bin"), &*seed_b).unwrap();

    let torrent_a1 = Torrent::spawn_with_options(
        meta_a.clone(),
        dir_a1.clone(),
        TorrentOptions {
            listen_active: Arc::new(AtomicBool::new(true)),
            announce_port: Arc::new(AtomicU16::new(port_a)),
            registry: registry_a.clone(),
            peer_id: id_a,
            choke_interval: Duration::from_millis(150),
            optimistic_interval: Duration::from_millis(300),
            dial: dial.clone(),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();
    let torrent_b1 = Torrent::spawn_with_options(
        meta_a.clone(),
        dir_b1.clone(),
        TorrentOptions {
            bootstrap_peers: vec![addr(port_a)],
            listen_active: Arc::new(AtomicBool::new(true)),
            announce_port: Arc::new(AtomicU16::new(port_b)),
            registry: registry_b.clone(),
            peer_id: id_b,
            choke_interval: Duration::from_millis(150),
            optimistic_interval: Duration::from_millis(300),
            dial: dial.clone(),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();

    let mut stats_b1 = torrent_b1.subscribe();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = stats_b1.borrow().clone();
        if matches!(snapshot.state, State::Completed | State::Seeding)
            && snapshot.verified_bytes == snapshot.total_length
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "b1 never completed: {snapshot:?}"
        );
        assert!(stats_b1.changed().await.is_ok());
    }

    let mut stats_a1 = torrent_a1.subscribe();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = stats_a1.borrow().clone();
        if snapshot.session_uploaded as usize >= DATA_LENGTH && snapshot.state == State::Seeding {
            break;
        }
        assert!(Instant::now() < deadline, "a1 never uploaded: {snapshot:?}");
        assert!(stats_a1.changed().await.is_ok());
    }
    let snapshot = stats_a1.borrow().clone();
    assert!(snapshot
        .peers
        .iter()
        .any(|peer| peer.direction == bt_core::engine::PeerDirection::Incoming));
    assert!(snapshot.ratio > 0.0);

    let torrent_b2 = Torrent::spawn_with_options(
        meta_b.clone(),
        dir_b2.clone(),
        TorrentOptions {
            listen_active: Arc::new(AtomicBool::new(true)),
            announce_port: Arc::new(AtomicU16::new(port_b)),
            registry: registry_b.clone(),
            peer_id: id_b,
            choke_interval: Duration::from_millis(150),
            optimistic_interval: Duration::from_millis(300),
            dial: dial.clone(),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();
    let torrent_a2 = Torrent::spawn_with_options(
        meta_b.clone(),
        dir_a2.clone(),
        TorrentOptions {
            bootstrap_peers: vec![addr(port_b)],
            listen_active: Arc::new(AtomicBool::new(true)),
            announce_port: Arc::new(AtomicU16::new(port_a)),
            registry: registry_a.clone(),
            peer_id: id_a,
            choke_interval: Duration::from_millis(150),
            optimistic_interval: Duration::from_millis(300),
            dial: dial.clone(),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();

    let mut stats_a2 = torrent_a2.subscribe();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = stats_a2.borrow().clone();
        if matches!(snapshot.state, State::Completed | State::Seeding)
            && snapshot.verified_bytes == snapshot.total_length
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a2 never completed: {snapshot:?}"
        );
        assert!(stats_a2.changed().await.is_ok());
    }
    let snapshot_b2 = torrent_b2.subscribe().borrow().clone();
    assert!(snapshot_b2.session_uploaded as usize >= DATA_LENGTH);

    torrent_a1.stop().await.unwrap();
    torrent_b1.stop().await.unwrap();
    torrent_a2.stop().await.unwrap();
    torrent_b2.stop().await.unwrap();
    for dir in [dir_a1, dir_b1, dir_a2, dir_b2] {
        std::fs::remove_dir_all(dir).unwrap();
    }
    listener_a.shutdown();
    listener_b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endgame_requests_last_piece_from_multiple_peers() {
    let data = test_data();
    let meta = torrent_meta(&data);
    let last_piece = (meta.info.pieces.len() - 1) as u32;
    let slow = Duration::from_millis(1_500);
    let (dial, mut reports) = FakeDial::with_reports(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![
            (
                addr(7101),
                SeederKind::Slow {
                    delay: slow,
                    only_piece: Some(last_piece),
                },
            ),
            (
                addr(7102),
                SeederKind::Slow {
                    delay: slow,
                    only_piece: None,
                },
            ),
        ],
    );
    let dir = temp_dir("endgame");
    std::fs::create_dir_all(&dir).unwrap();
    let torrent = Torrent::spawn_with_options(
        meta,
        dir.clone(),
        TorrentOptions {
            bootstrap_peers: vec![addr(7101), addr(7102)],
            dial: Arc::new(dial),
            listen_active: Arc::new(AtomicBool::new(false)),
            announce_port: Arc::new(AtomicU16::new(6881)),
            registry: Arc::new(Registry::default()),
            choke_interval: Duration::from_millis(100),
            optimistic_interval: Duration::from_millis(200),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();

    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let snapshot = stats.borrow().clone();
        if matches!(snapshot.state, State::Completed | State::Seeding)
            && snapshot.verified_bytes == snapshot.total_length
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "download never completed: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    }
    let mut report_a = reports.recv().await.unwrap();
    let mut report_b = reports.recv().await.unwrap();
    if report_a.addr > report_b.addr {
        std::mem::swap(&mut report_a, &mut report_b);
    }
    assert!(
        report_a.last_piece_requests >= 1 && report_b.last_piece_requests >= 1,
        "both seeders must be asked for the last piece: {report_a:?} {report_b:?}"
    );
    assert!(
        report_a.cancels_received + report_b.cancels_received >= 1,
        "the duplicate last-piece request must be cancelled: {report_a:?} {report_b:?}"
    );
    let served_total = report_a.blocks_served + report_b.blocks_served;
    assert!(
        served_total >= 3,
        "all three blocks must have been served: {report_a:?} {report_b:?}"
    );
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
