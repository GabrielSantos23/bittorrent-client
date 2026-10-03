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
