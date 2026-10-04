mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bt_core::dht::{DhtOptions, DhtPeers};
use bt_core::engine::{State, Torrent, TorrentOptions};
use tokio::sync::mpsc;

const DEBIAN_INFO_HASH: [u8; 20] = [
    0x7a, 0xcf, 0x8f, 0xb5, 0x90, 0xb2, 0x06, 0x0d, 0xd9, 0xc3, 0x14, 0x6e, 0xf7, 0x70, 0x16, 0x9d,
    0x59, 0x34, 0x33, 0xb0,
];

fn spawn_live_dht() -> bt_core::dht::DhtHandle {
    bt_core::dht::spawn(
        DhtOptions::new(0, bt_core::dht::NodeId::random(&bt_core::dht::SystemRandom))
            .with_bootstrap(
                bt_core::dht::DEFAULT_BOOTSTRAP_ROUTERS
                    .iter()
                    .map(|host| host.to_string())
                    .collect(),
            ),
    )
}

async fn wait_for_table(
    status: &tokio::sync::watch::Receiver<bt_core::dht::DhtStatus>,
    minimum: usize,
    seconds: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let snapshot = status.borrow().clone();
        if snapshot.node_count >= minimum {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "table never reached {minimum} nodes within {seconds}s: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires internet access to the public DHT network"]
async fn live_bootstrap_table_size_after_sixty_seconds() {
    let handle = spawn_live_dht();
    let status = handle.status();
    wait_for_table(&status, 1, 30).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    let snapshot = status.borrow().clone();
    println!(
        "LIVE bootstrap: node_count={} after 60s (active={} port={})",
        snapshot.node_count, snapshot.active, snapshot.port
    );
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires internet access to the public DHT network"]
async fn live_get_peers_for_the_debian_info_hash() {
    let handle = spawn_live_dht();
    let status = handle.status();
    wait_for_table(&status, 20, 90).await;
    let (tx, mut rx) = mpsc::channel::<DhtPeers>(1);
    assert!(handle.request_lookup(DEBIAN_INFO_HASH, tx));
    let outcome = tokio::time::timeout(Duration::from_secs(60), rx.recv())
        .await
        .expect("lookup finished within 60s")
        .expect("channel open");
    println!("LIVE get_peers: {} peers found", outcome.peers.len());
    let mut connected = 0usize;
    let mut handshakes = 0usize;
    let connect_timeout = Duration::from_secs(5);
    for peer in &outcome.peers {
        let attempt = tokio::time::timeout(connect_timeout, async {
            let Ok(Ok(mut stream)) =
                tokio::time::timeout(connect_timeout, tokio::net::TcpStream::connect(peer)).await
            else {
                return false;
            };
            connected += 1;
            let handshake = bt_core::peer::handshake::Handshake {
                info_hash: DEBIAN_INFO_HASH,
                reserved: bt_core::extensions::reserved_with_extensions(),
                peer_id: *bt_core::peer_id::session(),
            };
            let bytes = bt_core::peer::handshake::encode(&handshake);
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            if stream.write_all(&bytes).await.is_err() {
                return false;
            }
            let mut reply = [0u8; 68];
            if stream.read_exact(&mut reply).await.is_err() {
                return false;
            }
            bt_core::peer::handshake::decode(&reply).is_ok()
        })
        .await
        .unwrap_or(false);
        if attempt {
            handshakes += 1;
        }
    }
    println!(
        "LIVE get_peers: {} peers found, {} tcp connected, {} valid bt handshakes",
        outcome.peers.len(),
        connected,
        handshakes
    );
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires internet access to the public DHT network"]
async fn live_announce_from_one_instance_is_visible_to_a_second_instance() {
    let mut info_hash = [0u8; 20];
    bt_core::dht::RandomBytes::fill(&bt_core::dht::SystemRandom, &mut info_hash);
    let announce_port = {
        let probe = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        probe.local_addr().unwrap().port()
    };
    let announcer_id = bt_core::dht::NodeId::random(&bt_core::dht::SystemRandom);
    let checker_id = bt_core::dht::NodeId::random(&bt_core::dht::SystemRandom);
    assert_ne!(
        announcer_id, checker_id,
        "the two instances need distinct ids"
    );

    let announcer = bt_core::dht::spawn(
        DhtOptions::new(0, announcer_id)
            .with_bootstrap(
                bt_core::dht::DEFAULT_BOOTSTRAP_ROUTERS
                    .iter()
                    .map(|host| host.to_string())
                    .collect(),
            )
            .with_listener_state(
                Arc::new(std::sync::atomic::AtomicBool::new(true)),
                Arc::new(std::sync::atomic::AtomicU16::new(announce_port)),
            ),
    );
    let announcer_status = announcer.status();
    wait_for_table(&announcer_status, 1, 120).await;
    let (tx, mut rx) = mpsc::channel::<DhtPeers>(1);
    assert!(announcer.request_lookup(info_hash, tx));
    let seeded = tokio::time::timeout(Duration::from_secs(60), rx.recv())
        .await
        .expect("announcer lookup finished within 60s")
        .expect("channel open");
    println!(
        "LIVE announce: announcer lookup saw {} peers for the random info hash",
        seeded.peers.len()
    );
    let announced_at = Instant::now();
    let announcer_snapshot = loop {
        let snapshot = announcer_status.borrow().clone();
        if snapshot.announces_sent > 0 {
            break snapshot;
        }
        assert!(
            Instant::now() < announced_at + Duration::from_secs(30),
            "the announcer never sent an announce_peer: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    println!(
        "LIVE announce: announces_sent={} announcing port {announce_port}",
        announcer_snapshot.announces_sent
    );

    let checker = bt_core::dht::spawn(
        DhtOptions::new(0, checker_id).with_bootstrap(
            bt_core::dht::DEFAULT_BOOTSTRAP_ROUTERS
                .iter()
                .map(|host| host.to_string())
                .collect(),
        ),
    );
    let checker_status = checker.status();
    wait_for_table(&checker_status, 1, 120).await;

    let mut visible: Option<DhtPeers> = None;
    for attempt in 1..=3 {
        let (tx, mut rx) = mpsc::channel::<DhtPeers>(1);
        assert!(checker.request_lookup(info_hash, tx));
        let outcome = tokio::time::timeout(Duration::from_secs(60), rx.recv())
            .await
            .expect("checker lookup finished within 60s")
            .expect("channel open");
        let found = outcome
            .peers
            .iter()
            .any(|peer| peer.port() == announce_port);
        println!(
            "LIVE announce: attempt {attempt} saw {} peers, our announced peer visible: {found}",
            outcome.peers.len()
        );
        if found {
            visible = Some(outcome);
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let checker_snapshot = checker_status.borrow().clone();
    let announcer_snapshot = announcer_status.borrow().clone();
    match visible {
        Some(outcome) => {
            println!(
                "LIVE announce: our peer {} appeared after {} announces",
                outcome.peers.len(),
                announcer_snapshot.announces_sent
            );
        }
        None => panic!(
            "our announced peer never appeared in the checker lookups; announcer={announcer_snapshot:?} checker={checker_snapshot:?}"
        ),
    }
    announcer.shutdown();
    checker.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires internet access to the public DHT network"]
async fn live_magnet_with_no_trackers_gets_metadata_via_dht() {
    let handle = spawn_live_dht();
    let status = handle.status();
    wait_for_table(&status, 20, 90).await;
    let dht_port = status.borrow().port;
    let hex_hash = bt_core::hex::encode(&DEBIAN_INFO_HASH);
    let uri = format!("magnet:?xt=urn:btih:{hex_hash}&dn=debian-13.7.0-amd64-netinst.iso");
    let magnet = bt_core::magnet::parse(&uri).unwrap();
    let dir = common::temp_dir("dht-live-magnet");
    let options = TorrentOptions {
        dht: Some(bt_core::engine::DhtIntegration {
            handle: handle.clone(),
            active: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            port: Arc::new(std::sync::atomic::AtomicU16::new(dht_port)),
        }),
        ..TorrentOptions::default()
    };
    let started = Instant::now();
    let torrent = Torrent::spawn_from_magnet(magnet, dir.clone(), options)
        .await
        .unwrap();
    let metadata = torrent.subscribe_metadata();
    let stats = torrent.subscribe();
    loop {
        if metadata.borrow().is_some() {
            break;
        }
        assert!(
            Instant::now() < started + Duration::from_secs(300),
            "metadata did not arrive within 300s: {:?}",
            stats.borrow()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let elapsed = started.elapsed();
    println!(
        "LIVE magnet: metadata arrived in {:.1}s via DHT peers",
        elapsed.as_secs_f64()
    );
    let _ = torrent.stop().await;
    handle.shutdown();
    let _ = std::fs::remove_dir_all(dir);
    assert!(matches!(
        stats.borrow().state,
        State::Downloading | State::Completed | State::Seeding | State::Checking
    ));
}
