mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bt_core::bencode::{self, Value};
use bt_core::engine::{State, Torrent, TorrentOptions, TrackerState};
use bt_core::metainfo::MetaInfo;
use common::{temp_dir, test_data, FakeDial, SeederKind};

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn str_field(value: &str) -> Value {
    Value::Bytes(value.as_bytes().to_vec())
}

fn sha1_of(chunk: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(chunk);
    hasher.finalize().into()
}

fn torrent_with_trackers(announce_list: Vec<Vec<String>>) -> (MetaInfo, Arc<Vec<u8>>) {
    let data = test_data();
    let mut pieces = Vec::new();
    for chunk in data.chunks(common::PIECE_LENGTH) {
        pieces.extend_from_slice(&sha1_of(chunk));
    }
    let mut info = BTreeMap::new();
    info.insert(b"length".to_vec(), Value::Int(data.len() as i64));
    info.insert(b"name".to_vec(), str_field("e2e.bin"));
    info.insert(
        b"piece length".to_vec(),
        Value::Int(common::PIECE_LENGTH as i64),
    );
    info.insert(b"pieces".to_vec(), Value::Bytes(pieces));
    let mut root = BTreeMap::new();
    root.insert(b"info".to_vec(), Value::Dict(info));
    let tiers: Vec<Value> = announce_list
        .iter()
        .map(|tier| {
            Value::List(
                tier.iter()
                    .map(|url| Value::Bytes(url.as_bytes().to_vec()))
                    .collect(),
            )
        })
        .collect();
    root.insert(b"announce-list".to_vec(), Value::List(tiers));
    let raw = bencode::encode(&Value::Dict(root));
    let meta = MetaInfo::from_bytes(&raw).unwrap();
    (meta, data)
}

async fn wait_completed(torrent: &Torrent, seconds: u64) {
    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let snapshot = stats.borrow().clone();
        if matches!(snapshot.state, State::Completed | State::Seeding)
            && snapshot.verified_bytes == snapshot.total_length
        {
            return;
        }
        assert!(Instant::now() < deadline, "never completed: {snapshot:?}");
        assert!(stats.changed().await.is_ok());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merges_and_dedupes_peers_from_two_trackers_in_one_tier() {
    let (tracker_a, _) = common::spawn_fake_http_tracker(common::FakeTrackerResponse::ok(
        1,
        3,
        0,
        &[addr(9301), addr(9302)],
    ))
    .await;
    let (tracker_b, _) = common::spawn_fake_http_tracker(common::FakeTrackerResponse::ok(
        1,
        3,
        0,
        &[addr(9302), addr(9303)],
    ))
    .await;
    let url_a = format!("http://{}/announce", tracker_a.addr);
    let url_b = format!("http://{}/announce", tracker_b.addr);
    let (meta, data) = torrent_with_trackers(vec![vec![url_a.clone(), url_b.clone()]]);

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![
            (addr(9301), SeederKind::Good),
            (addr(9302), SeederKind::Good),
            (addr(9303), SeederKind::Good),
        ],
    ));
    let dir = temp_dir("tracker-merge");
    let dial_ref = dial.clone();
    let torrent = Torrent::spawn_with_options(
        meta,
        dir.clone(),
        TorrentOptions {
            dial: dial_ref,
            registry: Arc::new(bt_core::listener::Registry::default()),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();
    wait_completed(&torrent, 30).await;

    assert_eq!(dial.dial_count(&addr(9301)), 1);
    assert_eq!(dial.dial_count(&addr(9302)), 1);
    assert_eq!(dial.dial_count(&addr(9303)), 1);

    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        let snapshot = stats.borrow().clone();
        let all_ok = snapshot
            .trackers
            .iter()
            .all(|tracker| tracker.state == TrackerState::Ok);
        if snapshot.trackers.len() == 2 && all_ok {
            break snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "trackers never reached ok: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    };
    for tracker in &snapshot.trackers {
        assert!(tracker.last_announce.is_some());
        assert!(tracker.last_error.is_none());
    }

    torrent.stop().await.unwrap();
    tracker_a.shutdown();
    tracker_b.shutdown();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_tracker_does_not_block_the_others() {
    let (tracker_a, _) =
        common::spawn_fake_http_tracker(common::FakeTrackerResponse::failure("no such torrent"))
            .await;
    let (tracker_b, _) =
        common::spawn_fake_http_tracker(common::FakeTrackerResponse::ok(1, 1, 0, &[addr(9401)]))
            .await;
    let url_a = format!("http://{}/announce", tracker_a.addr);
    let url_b = format!("http://{}/announce", tracker_b.addr);
    let (meta, data) = torrent_with_trackers(vec![vec![url_a.clone(), url_b.clone()]]);

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(addr(9401), SeederKind::Good)],
    ));
    let dir = temp_dir("tracker-fail");
    let torrent = Torrent::spawn_with_options(
        meta,
        dir.clone(),
        TorrentOptions {
            dial,
            registry: Arc::new(bt_core::listener::Registry::default()),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();
    wait_completed(&torrent, 30).await;

    let mut stats = torrent.subscribe();
    let deadline = Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        let snapshot = stats.borrow().clone();
        let failed = snapshot
            .trackers
            .iter()
            .find(|tracker| tracker.url == url_a);
        let ok = snapshot
            .trackers
            .iter()
            .find(|tracker| tracker.url == url_b);
        if let (Some(failed), Some(ok)) = (failed, ok) {
            if failed.state == TrackerState::Error
                && failed
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("no such torrent"))
                && ok.state == TrackerState::Ok
            {
                break snapshot;
            }
        }
        assert!(
            Instant::now() < deadline,
            "tracker statuses never settled: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    };
    assert_eq!(snapshot.trackers.len(), 2);

    torrent.stop().await.unwrap();
    tracker_a.shutdown();
    tracker_b.shutdown();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_returns_within_budget_with_silent_udp_tracker() {
    use std::net::Ipv4Addr;

    let silent = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let url = format!("udp://{}/announce", silent.local_addr().unwrap());
    let (meta, _data) = torrent_with_trackers(vec![vec![url]]);
    let dir = temp_dir("udp-shutdown");
    let torrent = Torrent::spawn_with_options(
        meta,
        dir.clone(),
        TorrentOptions {
            registry: Arc::new(bt_core::listener::Registry::default()),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let started = Instant::now();
    torrent.stop().await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(4),
        "stop must not wait for the udp retransmit budget, took {elapsed:?}"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

fn announce_for(meta: &MetaInfo) -> bt_core::tracker::AnnounceRequest {
    bt_core::tracker::AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *bt_core::peer_id::session(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length().unwrap(),
        numwant: 50,
        event: Some(bt_core::tracker::Event::Started),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn endless_tracker_stream_is_capped() {
    use bt_core::tracker;
    let addr = common::spawn_endless_http_tracker().await;
    let (meta, _data) = torrent_with_trackers(vec![vec![format!("http://{addr}/announce")]]);
    let client = tracker::http_client().unwrap();
    let started = Instant::now();
    let result = tracker::http_announce(
        &client,
        &format!("http://{addr}/announce"),
        &announce_for(&meta),
    )
    .await;
    let elapsed = started.elapsed();
    assert!(
        matches!(
            result,
            Err(bt_core::error::TrackerError::ResponseTooLarge(_, _))
        ),
        "endless stream must be capped, got {result:?}"
    );
    assert!(elapsed < Duration::from_secs(5), "cap took {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_content_length_is_rejected_before_reading() {
    use bt_core::tracker;
    let addr = common::spawn_oversized_content_length_tracker().await;
    let (meta, _data) = torrent_with_trackers(vec![vec![format!("http://{addr}/announce")]]);
    let client = tracker::http_client().unwrap();
    let result = tracker::http_announce(
        &client,
        &format!("http://{addr}/announce"),
        &announce_for(&meta),
    )
    .await;
    assert!(matches!(
        result,
        Err(bt_core::error::TrackerError::ResponseTooLarge(2_097_152, _))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redirect_loop_is_bounded() {
    use bt_core::tracker;
    let addr = common::spawn_redirect_loop_http_tracker().await;
    let (meta, _data) = torrent_with_trackers(vec![vec![format!("http://{addr}/announce")]]);
    let client = tracker::http_client().unwrap();
    let started = Instant::now();
    let result = tracker::http_announce(
        &client,
        &format!("http://{addr}/announce"),
        &announce_for(&meta),
    )
    .await;
    assert!(result.is_err(), "redirect loop must fail");
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn endless_fake_raw_read() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = common::spawn_endless_http_tracker().await;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /announce?info_hash=x HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buffer = vec![0u8; 70000];
    let n = stream.read(&mut buffer).await.unwrap();
    println!(
        "FIRST READ {n} BYTES: {:?}",
        String::from_utf8_lossy(&buffer[..n.min(120)])
    );
    assert!(n > 0);
}
