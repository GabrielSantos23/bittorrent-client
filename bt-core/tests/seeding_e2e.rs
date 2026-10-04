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
    temp_dir, test_data, torrent_meta, torrent_meta_with_piece_length, FakeDial, FakeLeecherDial,
    FakeMetadataLeecherDial, LeecherConfig, LeecherReport, MetadataMode, SeederKind, DATA_LENGTH,
    PIECE_LENGTH,
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
        dht: None,
        ..TorrentOptions::default()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_metadata_fetch_and_download_from_one_peer() {
    let data = test_data();
    let meta = torrent_meta(&data);
    let root_value = bt_core::bencode::decode(&common::torrent_bytes(&data)).unwrap();
    let info_value = match &root_value {
        bt_core::bencode::Value::Dict(entries) => entries.get(&b"info".to_vec()).unwrap().clone(),
        _ => unreachable!(),
    };
    let info_dict = Arc::new(bt_core::bencode::encode(&info_value));
    let dial = Arc::new(FakeDial::with_metadata(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(addr(7201), SeederKind::Good)],
        info_dict.clone(),
    ));
    let magnet = bt_core::magnet::MagnetLink {
        info_hash: meta.info_hash,
        display_name: Some("e2e.bin".to_string()),
        trackers: Vec::new(),
        peers: vec![bt_core::magnet::MagnetPeer {
            host: "127.0.0.1".to_string(),
            port: 7201,
        }],
    };
    let dir = temp_dir("magnet-one");
    let torrent = Torrent::spawn_from_magnet(
        magnet,
        dir.clone(),
        TorrentOptions {
            dial,
            registry: Arc::new(bt_core::listener::Registry::default()),
            choke_interval: Duration::from_millis(100),
            optimistic_interval: Duration::from_millis(200),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();

    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = stats.borrow().clone();
        if matches!(snapshot.state, State::Completed | State::Seeding)
            && snapshot.verified_bytes == snapshot.total_length
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "magnet torrent never completed: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    }
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

fn info_dict_of_with(data: &[u8], piece_length: usize) -> Arc<Vec<u8>> {
    let root_value =
        bt_core::bencode::decode(&common::torrent_bytes_with_piece_length(data, piece_length))
            .unwrap();
    match &root_value {
        bt_core::bencode::Value::Dict(entries) => Arc::new(bt_core::bencode::encode(
            &entries.get(&b"info".to_vec()).unwrap().clone(),
        )),
        _ => unreachable!(),
    }
}

fn magnet_for(info_hash: [u8; 20], peers: &[u16]) -> bt_core::magnet::MagnetLink {
    bt_core::magnet::MagnetLink {
        info_hash,
        display_name: Some("e2e.bin".to_string()),
        trackers: Vec::new(),
        peers: peers
            .iter()
            .map(|port| bt_core::magnet::MagnetPeer {
                host: "127.0.0.1".to_string(),
                port: *port,
            })
            .collect(),
    }
}

async fn wait_for_metadata(torrent: &Torrent, seconds: u64) -> bt_core::engine::Stats {
    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let snapshot = stats.borrow().clone();
        if snapshot.piece_count > 0 {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "metadata never arrived: {:?}",
            snapshot.diag
        );
        assert!(stats.changed().await.is_ok());
    }
}

async fn spawn_magnet_torrent(
    data: &Arc<Vec<u8>>,
    peers: &[u16],
    modes: Vec<(SocketAddr, MetadataMode)>,
    dir: &std::path::Path,
) -> (
    Torrent,
    Option<tokio::sync::mpsc::Receiver<common::SeederReport>>,
) {
    std::fs::create_dir_all(dir).unwrap();
    let meta = torrent_meta_with_piece_length(data, 1024);
    let info_dict = info_dict_of_with(data, 1024);
    let (dial, reports) = FakeDial::with_metadata_modes(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        1024,
        peers.iter().map(|p| (addr(*p), SeederKind::Good)).collect(),
        info_dict,
        modes,
    );
    let torrent = Torrent::spawn_from_magnet(
        magnet_for(meta.info_hash, peers),
        dir.to_path_buf(),
        TorrentOptions {
            dial: Arc::new(dial),
            registry: Arc::new(Registry::default()),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();
    (torrent, Some(reports))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupted_metadata_is_refetched_from_a_good_peer() {
    let data = common::metadata_multi_piece_data();
    let meta = torrent_meta_with_piece_length(&data, 1024);
    let piece_count = meta.info.pieces.len();
    let dir = temp_dir("magnet-corrupt");
    let (torrent, _reports) = spawn_magnet_torrent(
        &data,
        &[7301, 7302],
        vec![
            (addr(7301), MetadataMode::Corrupt(0)),
            (addr(7302), MetadataMode::Good),
        ],
        &dir,
    )
    .await;
    let snapshot = wait_for_metadata(&torrent, 30).await;
    assert_eq!(snapshot.name, "e2e.bin");
    assert_eq!(snapshot.piece_count, piece_count);
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_size_zero_is_never_requested() {
    let data = common::metadata_multi_piece_data();
    let dir = temp_dir("magnet-zero-size");
    let (torrent, _reports) = spawn_magnet_torrent(
        &data,
        &[7311],
        vec![(addr(7311), MetadataMode::ZeroHandshake)],
        &dir,
    )
    .await;
    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let snapshot = stats.borrow().clone();
        if snapshot.diag.extension_handshakes >= 1 {
            assert_eq!(
                snapshot.diag.metadata_requests_sent, 0,
                "a zero metadata_size must never be requested"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "extension handshake never arrived: {:?}",
            snapshot.diag
        );
        assert!(stats.changed().await.is_ok());
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let snapshot = stats.borrow().clone();
    assert_eq!(snapshot.diag.metadata_requests_sent, 0);
    assert_eq!(snapshot.piece_count, 0);
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_size_above_limit_is_never_requested() {
    let data = common::metadata_multi_piece_data();
    let dir = temp_dir("magnet-oversize");
    let (torrent, _reports) = spawn_magnet_torrent(
        &data,
        &[7312],
        vec![(addr(7312), MetadataMode::OversizedHandshake)],
        &dir,
    )
    .await;
    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let snapshot = stats.borrow().clone();
        if snapshot.diag.extension_handshakes >= 1 {
            assert_eq!(
                snapshot.diag.metadata_requests_sent, 0,
                "an over-limit metadata_size must never be requested"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "extension handshake never arrived: {:?}",
            snapshot.diag
        );
        assert!(stats.changed().await.is_ok());
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let snapshot = stats.borrow().clone();
    assert_eq!(snapshot.diag.metadata_requests_sent, 0);
    assert_eq!(snapshot.piece_count, 0);
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicting_metadata_size_stops_requests() {
    let data = common::metadata_multi_piece_data();
    let dir = temp_dir("magnet-conflict-size");
    let (torrent, _reports) = spawn_magnet_torrent(
        &data,
        &[7321],
        vec![(addr(7321), MetadataMode::ConflictingDataSize(1))],
        &dir,
    )
    .await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let snapshot = torrent.subscribe().borrow().clone();
    assert_eq!(
        snapshot.diag.metadata_requests_sent, 4,
        "the conflicting peer must not be re-requested"
    );
    assert_eq!(snapshot.diag.metadata_data_received, 4);
    assert_eq!(
        snapshot.piece_count, 0,
        "the conflicting piece must be dropped"
    );
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_reject_is_retried_on_another_peer() {
    let data = common::metadata_multi_piece_data();
    let meta = torrent_meta_with_piece_length(&data, 1024);
    let piece_count = meta.info.pieces.len();
    let dir = temp_dir("magnet-reject");
    let (torrent, reports) = spawn_magnet_torrent(
        &data,
        &[7331, 7332],
        vec![(addr(7331), MetadataMode::RejectAll)],
        &dir,
    )
    .await;
    let snapshot = wait_for_metadata(&torrent, 30).await;
    assert_eq!(snapshot.piece_count, piece_count);
    torrent.stop().await.unwrap();
    let mut reports = reports.unwrap();
    let mut asked_rejecter = false;
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(10), reports.recv()).await {
            Ok(Some(report)) => {
                eprintln!("DEBUG reject report: {report:?}");
                if report.addr == addr(7331) && report.metadata_requests_received >= 1 {
                    asked_rejecter = true;
                }
            }
            _ => break,
        }
    }
    assert!(
        asked_rejecter,
        "the rejecting peer must have been asked and the work retried elsewhere"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_without_ut_metadata_is_skipped() {
    let data = common::metadata_multi_piece_data();
    let meta = torrent_meta_with_piece_length(&data, 1024);
    let piece_count = meta.info.pieces.len();
    let dir = temp_dir("magnet-no-ut");
    let (torrent, reports) = spawn_magnet_torrent(
        &data,
        &[7341, 7342],
        vec![(addr(7341), MetadataMode::NoUtMetadata)],
        &dir,
    )
    .await;
    let snapshot = wait_for_metadata(&torrent, 30).await;
    assert_eq!(snapshot.piece_count, piece_count);
    torrent.stop().await.unwrap();
    let mut reports = reports.unwrap();
    let mut skipped = false;
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(10), reports.recv()).await {
            Ok(Some(report)) => {
                if report.addr == addr(7341) {
                    skipped = true;
                    assert_eq!(
                        report.metadata_requests_received, 0,
                        "a peer without ut_metadata must never be asked"
                    );
                }
            }
            _ => break,
        }
    }
    assert!(skipped, "the ut_metadata-less peer report is missing");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_and_out_of_range_metadata_pieces_are_ignored() {
    let data = common::metadata_multi_piece_data();
    let meta = torrent_meta_with_piece_length(&data, 1024);
    let piece_count = meta.info.pieces.len();
    let dir = temp_dir("magnet-extras");
    let (torrent, _reports) = spawn_magnet_torrent(
        &data,
        &[7351],
        vec![(addr(7351), MetadataMode::UnsolicitedExtras)],
        &dir,
    )
    .await;
    let snapshot = wait_for_metadata(&torrent, 30).await;
    assert_eq!(snapshot.piece_count, piece_count);
    assert_eq!(
        snapshot.diag.metadata_requests_sent, 4,
        "garbage pieces must not trigger a refetch"
    );
    assert_eq!(
        snapshot.diag.metadata_data_received, 6,
        "four valid pieces plus a duplicate and an out-of-range piece"
    );
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_metadata_pieces_and_rejects_out_of_range_requests() {
    let data = test_data();
    let meta = torrent_meta(&data);
    let info_dict = info_dict_of_with(&data, PIECE_LENGTH);
    let (dial, mut reports) =
        FakeMetadataLeecherDial::new(meta.info_hash, meta.info.pieces.len(), vec![0, 1]);
    let dir = temp_dir("magnet-serve");
    let torrent = Torrent::spawn_with_dial(meta, dir.clone(), vec![addr(7361)], Arc::new(dial))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let report = loop {
        match tokio::time::timeout(Duration::from_millis(200), reports.recv()).await {
            Ok(Some(report)) => break report,
            Ok(None) => panic!("metadata leecher task ended without a report"),
            Err(_) => {
                assert!(Instant::now() < deadline, "metadata leecher never finished");
            }
        }
    };
    assert!(
        report.saw_client_handshake,
        "our extension handshake must arrive before any request"
    );
    assert_eq!(
        report.rejects,
        vec![1],
        "an out-of-range piece must be rejected"
    );
    assert_eq!(report.data.len(), 1);
    assert_eq!(report.data[0].0, 0);
    assert_eq!(report.data[0].1, info_dict.len() as u64);
    assert_eq!(
        report.data[0].2, *info_dict,
        "the served bytes must be the exact info dict"
    );
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
