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
