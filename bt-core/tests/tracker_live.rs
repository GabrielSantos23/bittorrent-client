use bt_core::metainfo::MetaInfo;
use bt_core::peer_id;
use bt_core::tracker::{self, AnnounceRequest, Event};

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
