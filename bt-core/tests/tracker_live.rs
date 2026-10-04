use std::collections::BTreeMap;

use bt_core::bencode::{self, Value};
use bt_core::metainfo::MetaInfo;
use bt_core::peer_id;
use bt_core::tracker::{self, AnnounceRequest, Event};
use bt_core::tracker_udp::{UdpConfig, UdpTrackerClient};

const UDP_TRACKER_URL: &str = "udp://tracker.opentrackr.org:1337/announce";

fn debian_with_udp_announce() -> MetaInfo {
    let original = MetaInfo::from_bytes(FIXTURE).unwrap();
    let root = bencode::decode(FIXTURE).unwrap();
    let root = root.as_dict().unwrap();
    let info = root.get("info".as_bytes()).unwrap().clone();
    let mut tiers = vec![Value::List(vec![Value::Bytes(
        UDP_TRACKER_URL.as_bytes().to_vec(),
    )])];
    if let Some(existing) = root.get("announce-list".as_bytes()) {
        if let Some(items) = existing.as_list() {
            for tier in items {
                tiers.push(tier.clone());
            }
        }
    }
    let mut rebuilt = BTreeMap::new();
    rebuilt.insert(b"announce-list".to_vec(), Value::List(tiers));
    rebuilt.insert(b"info".to_vec(), info);
    let raw = bencode::encode(&Value::Dict(rebuilt));
    let meta = MetaInfo::from_bytes(&raw).unwrap();
    assert_eq!(meta.info_hash, original.info_hash);
    meta
}

#[tokio::test]
#[ignore = "requires internet access to a public UDP tracker"]
async fn announces_to_public_udp_tracker() {
    let meta = debian_with_udp_announce();
    let mut client = UdpTrackerClient::connect_tracker(UDP_TRACKER_URL, UdpConfig::default())
        .await
        .unwrap();
    let request = AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *peer_id::session(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length().unwrap(),
        numwant: 50,
        event: Some(Event::Started),
    };
    let response = client.announce(&request, 50).await.unwrap();
    assert!(response.interval > 0);
    println!(
        "udp tracker answered: interval {} s, seeders {}, leechers {}, peers {}",
        response.interval,
        response.complete,
        response.incomplete,
        response.peers.len()
    );
}

const FIXTURE: &[u8] = include_bytes!("fixtures/debian-13.7.0-amd64-netinst.iso.torrent");

#[tokio::test]
#[ignore = "requires internet access to the live Debian tracker"]
async fn announces_to_debian_tracker() {
    let meta = MetaInfo::from_bytes(FIXTURE).unwrap();
    let request = AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *peer_id::session(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length().unwrap(),
        numwant: 50,
        event: Some(Event::Started),
    };
    let client = tracker::http_client().unwrap();
    let outcome = tracker::announce(&client, &meta, &request).await.unwrap();
    assert_eq!(outcome.url, "http://bttracker.debian.org:6969/announce");
    assert!(outcome.response.interval > 0);
    assert!(!outcome.response.peers.is_empty());
}

#[tokio::test]
#[ignore = "requires internet access to the live Debian swarm"]
async fn magnet_metadata_and_download_from_live_swarm() {
    use bt_core::engine::{State, Torrent, TorrentOptions};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const DEBIAN_HASH: &str = "7acf8fb590b2060dd9c3146ef770169d593433b0";
    let mut info_hash = [0u8; 20];
    for (index, chunk) in DEBIAN_HASH.as_bytes().chunks(2).enumerate() {
        info_hash[index] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
    }
    let magnet = crate_magnet(&info_hash);
    let dir = std::env::temp_dir().join(format!("bt-magnet-live-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let torrent = Torrent::spawn_from_magnet(
        magnet,
        dir.clone(),
        TorrentOptions {
            registry: Arc::new(bt_core::listener::Registry::default()),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();

    let mut stats = torrent.subscribe();
    let metadata_deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let snapshot = stats.borrow().clone();
        if snapshot.piece_count > 0 {
            println!(
                "metadata arrived: name {:?}, pieces {}, length {}",
                snapshot.name, snapshot.piece_count, snapshot.total_length
            );
            break;
        }
        println!("metadata diag: {:?}", snapshot.diag);
        assert!(
            Instant::now() < metadata_deadline,
            "metadata never arrived: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    }
    let download_deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let snapshot = stats.borrow().clone();
        if matches!(snapshot.state, State::Downloading) && snapshot.verified_bytes > 0 {
            println!(
                "download started: name {:?}, verified {} bytes",
                snapshot.name, snapshot.verified_bytes
            );
            break;
        }
        assert!(
            Instant::now() < download_deadline,
            "download never started: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    }
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

fn crate_magnet(info_hash: &[u8; 20]) -> bt_core::magnet::MagnetLink {
    bt_core::magnet::MagnetLink {
        info_hash: *info_hash,
        display_name: None,
        trackers: vec![
            "http://bttracker.debian.org:6969/announce".to_string(),
            "udp://tracker.opentrackr.org:1337/announce".to_string(),
        ],
        peers: Vec::new(),
    }
}
