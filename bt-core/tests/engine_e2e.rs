mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bt_core::engine::{State, Torrent};
use common::{temp_dir, test_data, torrent_meta, FakeDial, SeederKind, DATA_LENGTH};

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

async fn wait_for(
    stats: &tokio::sync::watch::Receiver<bt_core::engine::Stats>,
    condition: impl Fn(&bt_core::engine::Stats) -> bool,
    seconds: u64,
) -> bt_core::engine::Stats {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        let snapshot = stats.borrow().clone();
        if condition(&snapshot) {
            return snapshot;
        }
        eprintln!("wait_for tick: {:?}", snapshot);
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in {seconds}s: {:?}",
            snapshot.state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_from_fake_seeders() {
    let data = test_data();
    let meta = torrent_meta(&data);
    let dir = temp_dir("e2e");

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![
            (addr(9001), SeederKind::Good),
            (addr(9002), SeederKind::CorruptOnce),
            (addr(9003), SeederKind::Choking),
            (addr(9004), SeederKind::NeverReads),
        ],
    ));

    let torrent = Torrent::spawn_with_dial(
        meta,
        dir.clone(),
        vec![addr(9001), addr(9002), addr(9003), addr(9004)],
        dial,
    )
    .await
    .unwrap();
    let stats = torrent.subscribe();
    let snapshot = wait_for(&stats, |s| s.state == State::Completed, 60).await;
    assert_eq!(snapshot.verified_bytes, snapshot.total_length);
    assert!(snapshot.session_downloaded >= snapshot.total_length);
    torrent.stop().await.unwrap();
    let written = std::fs::read(dir.join("e2e.bin")).unwrap();
    assert_eq!(written.len(), DATA_LENGTH);
    assert_eq!(written, *data);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_and_resume_continue_without_recheck() {
    let data: Arc<Vec<u8>> = Arc::new((0..12 * 16384).map(|i| (i % 251) as u8).collect());
    let meta = torrent_meta(&data);
    let dir = temp_dir("pause");

    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![
            (addr(9101), SeederKind::Good),
            (addr(9102), SeederKind::CorruptOnce),
            (addr(9103), SeederKind::NeverReads),
        ],
    ));

    let torrent = Torrent::spawn_with_dial(
        meta,
        dir.clone(),
        vec![addr(9101), addr(9102), addr(9103)],
        dial,
    )
    .await
    .unwrap();
    let stats = torrent.subscribe();
    wait_for(&stats, |s| s.verified_bytes > 0, 30).await;

    torrent.pause().await.unwrap();
    wait_for(&stats, |s| s.state == State::Paused, 10).await;

    torrent.resume().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = stats.borrow().clone();
        assert_ne!(snapshot.state, State::Checking, "resume must not recheck");
        if snapshot.state == State::Completed {
            assert_eq!(snapshot.verified_bytes, snapshot.total_length);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "download did not resume: {:?}",
            snapshot.state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
