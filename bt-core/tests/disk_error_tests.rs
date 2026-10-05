mod common;

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bt_core::engine::{FaultOp, FaultyFs, State};
use bt_core::session::{AddOptions, Session, SessionOptions};
use common::{temp_dir, test_data, torrent_bytes, FakeDial, SeederKind};

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

async fn wait_for(
    stats: &tokio::sync::watch::Receiver<Vec<bt_core::session::TorrentSummary>>,
    condition: impl Fn(&[bt_core::session::TorrentSummary]) -> bool,
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

/// Three files of one piece each; total 49152 bytes.
fn multi_file_torrent() -> (Vec<u8>, Arc<Vec<u8>>) {
    use sha1::{Digest, Sha1};
    let piece_length = 16384usize;
    let data: Vec<u8> = (0..49152).map(|i| (i % 251) as u8).collect();
    let mut pieces = Vec::new();
    for chunk in data.chunks(piece_length) {
        let digest: [u8; 20] = Sha1::digest(chunk).into();
        pieces.extend_from_slice(&digest);
    }
    let mut files = Vec::new();
    for name in ["f0", "f1", "f2"] {
        let mut dict = std::collections::BTreeMap::new();
        dict.insert(b"length".to_vec(), bt_core::bencode::Value::Int(16384));
        dict.insert(
            b"path".to_vec(),
            bt_core::bencode::Value::List(vec![bt_core::bencode::Value::Bytes(
                name.as_bytes().to_vec(),
            )]),
        );
        files.push(bt_core::bencode::Value::Dict(dict));
    }
    let mut info = std::collections::BTreeMap::new();
    info.insert(b"files".to_vec(), bt_core::bencode::Value::List(files));
    info.insert(
        b"name".to_vec(),
        bt_core::bencode::Value::Bytes(b"sp".to_vec()),
    );
    info.insert(
        b"piece length".to_vec(),
        bt_core::bencode::Value::Int(16384),
    );
    info.insert(b"pieces".to_vec(), bt_core::bencode::Value::Bytes(pieces));
    let mut root = std::collections::BTreeMap::new();
    root.insert(b"info".to_vec(), bt_core::bencode::Value::Dict(info));
    (
        bt_core::bencode::encode(&bt_core::bencode::Value::Dict(root)),
        Arc::new(data),
    )
}

fn cleanup_dir(dir: &std::path::Path) {
    // On Windows the engine's background tasks can hold file handles open for
    // a short moment after shutdown.
    for _ in 0..20 {
        match std::fs::remove_dir_all(dir) {
            Ok(()) => return,
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

async fn session_with(
    fs: Arc<FaultyFs>,
    dial: Arc<FakeDial>,
    seeder: Option<SocketAddr>,
) -> Session {
    Session::spawn_with_options(
        None,
        SessionOptions {
            fs,
            dial,
            bootstrap_peers: seeder.into_iter().collect(),
            dht_enabled: false,
            ..SessionOptions::new(0, 0)
        },
    )
    .await
    .unwrap()
}

async fn session_with_persistence(
    fs: Arc<FaultyFs>,
    dial: Arc<FakeDial>,
    persistence: std::path::PathBuf,
) -> Session {
    Session::spawn_with_options(
        Some(persistence),
        SessionOptions {
            fs,
            dial,
            dht_enabled: false,
            ..SessionOptions::new(0, 0)
        },
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_full_reports_a_retryable_error_and_resumes_after_the_fault_clears() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let dir = temp_dir("disk-full-out");
    let faulty = Arc::new(FaultyFs::new());
    let seeder = addr(9501);
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(seeder, SeederKind::Good)],
    ));
    let session = session_with(faulty.clone(), dial, Some(seeder)).await;
    // The first piece write is what fails: deterministic, because the only
    // Write operations in a download are the verified piece writes.
    faulty.fail(FaultOp::Write, 1, ErrorKind::StorageFull);
    let id = session
        .add_torrent(&bytes, dir.clone(), AddOptions::default())
        .await
        .unwrap();

    let summaries = session.subscribe();
    let snapshot = wait_for(
        &summaries,
        |s| s.iter().any(|t| t.id == id && t.state == State::Error),
        30,
    )
    .await;
    let summary = snapshot.iter().find(|t| t.id == id).unwrap();
    assert!(
        summary
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("disk full"),
        "the error must name the disk full condition: {:?}",
        summary.error
    );
    assert!(
        summary.error_retryable,
        "a full disk is retryable once the user frees space"
    );

    // The engine stopped requesting while errored and did not ban anyone:
    // clearing the fault and resuming finishes the download from the same
    // seeder.
    faulty.clear_faults();
    session.resume(&id).await.unwrap();
    let snapshot = wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        30,
    )
    .await;
    let summary = snapshot.iter().find(|t| t.id == id).unwrap();
    assert_eq!(summary.verified_bytes, summary.total_length);
    let written = std::fs::read(dir.join("e2e.bin")).unwrap();
    assert_eq!(written, *data);
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permission_denied_and_read_only_filesystem_report_typed_errors() {
    for (kind, port, expected) in [
        (ErrorKind::PermissionDenied, 9511u16, "permission denied"),
        (ErrorKind::ReadOnlyFilesystem, 9512u16, "read-only"),
    ] {
        let data = test_data();
        let bytes = torrent_bytes(&data);
        let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
        let dir = temp_dir("disk-typed-out");
        let faulty = Arc::new(FaultyFs::new());
        let seeder = addr(port);
        let dial = Arc::new(FakeDial::new(
            meta.info_hash,
            data.clone(),
            meta.info.pieces.len(),
            vec![(seeder, SeederKind::Good)],
        ));
        let session = session_with(faulty.clone(), dial, Some(seeder)).await;
        faulty.fail(FaultOp::Write, 1, kind);
        let id = session
            .add_torrent(&bytes, dir.clone(), AddOptions::default())
            .await
            .unwrap();
        let summaries = session.subscribe();
        let snapshot = wait_for(
            &summaries,
            |s| s.iter().any(|t| t.id == id && t.state == State::Error),
            30,
        )
        .await;
        let summary = snapshot.iter().find(|t| t.id == id).unwrap();
        assert!(
            summary
                .error
                .as_deref()
                .unwrap_or_default()
                .contains(expected),
            "expected {expected:?} in the error, got {:?}",
            summary.error
        );
        assert!(summary.error_retryable);
        session.shutdown().await.unwrap();
        cleanup_dir(&dir);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_output_directory_fails_the_add_and_a_failing_directory_creation_retries() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();

    // An output directory that cannot be created fails the add with a
    // typed, retryable storage error.
    let faulty = Arc::new(FaultyFs::new());
    faulty.fail(FaultOp::CreateDirAll, 1, ErrorKind::PermissionDenied);
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = session_with(faulty.clone(), dial, None).await;
    let dir = temp_dir("disk-mkdir-out");
    let result = session
        .add_torrent(&bytes, dir.clone(), AddOptions::default())
        .await;
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("permission denied") || err.contains("output directory"),
        "expected a typed directory error, got: {err}"
    );
    faulty.clear_faults();
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);

    // While a download is in the Error state, resuming recreates the
    // directories it writes into; a failing recreation is reported and the
    // next resume, after the fault clears, finishes the download.
    let faulty = Arc::new(FaultyFs::new());
    let seeder = addr(9521);
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(seeder, SeederKind::Good)],
    ));
    let session = session_with(faulty.clone(), dial, Some(seeder)).await;
    faulty.fail(FaultOp::Write, 1, ErrorKind::StorageFull);
    let dir = temp_dir("disk-retry-out");
    let id = session
        .add_torrent(&bytes, dir.clone(), AddOptions::default())
        .await
        .unwrap();
    let summaries = session.subscribe();
    wait_for(
        &summaries,
        |s| s.iter().any(|t| t.id == id && t.state == State::Error),
        30,
    )
    .await;
    // The retry itself fails while directory creation is broken. Waiting for
    // the message (not just the state) skips the pre-resume disk full error.
    faulty.fail(FaultOp::CreateDirAll, 1, ErrorKind::PermissionDenied);
    session.resume(&id).await.unwrap();
    let snapshot = wait_for(
        &summaries,
        |s| {
            s.iter().any(|t| {
                t.id == id
                    && t.state == State::Error
                    && t.error
                        .as_deref()
                        .unwrap_or_default()
                        .contains("permission denied")
            })
        },
        30,
    )
    .await;
    let summary = snapshot.iter().find(|t| t.id == id).unwrap();
    assert!(
        summary.error_retryable,
        "a failing directory recreation is retryable"
    );
    faulty.clear_faults();
    session.resume(&id).await.unwrap();
    let snapshot = wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        30,
    )
    .await;
    let summary = snapshot.iter().find(|t| t.id == id).unwrap();
    assert_eq!(summary.verified_bytes, summary.total_length);
    let written = std::fs::read(dir.join("e2e.bin")).unwrap();
    assert_eq!(written, *data);
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn free_space_check_blocks_adds_honors_priorities_and_allows_an_override() {
    let (bytes, data) = multi_file_torrent();
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let faulty = Arc::new(FaultyFs::new());
    faulty.set_available_space(Some(1_000));
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = session_with(faulty.clone(), dial, None).await;
    let dir = temp_dir("disk-space-out");

    let err = format!(
        "{}",
        session
            .add_torrent(&bytes, dir.clone(), AddOptions::default())
            .await
            .unwrap_err()
    );
    assert!(
        err.contains("not enough free space") && err.contains("49152") && err.contains("1000"),
        "the error must report needed and available bytes: {err}"
    );

    // Skipped files do not count towards the needed bytes.
    let with_skips = AddOptions {
        file_priorities: vec![(0, bt_core::engine::FilePriority::Skip)],
        ..AddOptions::default()
    };
    let err = format!(
        "{}",
        session
            .add_torrent(&bytes, dir.clone(), with_skips.clone())
            .await
            .unwrap_err()
    );
    assert!(
        err.contains("32768"),
        "one skipped file must reduce the needed bytes: {err}"
    );

    // Enough space for the wanted files: the add goes through.
    faulty.set_available_space(Some(40_000));
    let id = session
        .add_torrent(&bytes, dir.clone(), with_skips.clone())
        .await
        .unwrap();
    session.remove(&id, false).await.unwrap();

    // The check can be overridden explicitly.
    let override_opts = AddOptions {
        skip_free_space_check: true,
        ..AddOptions::default()
    };
    let id = session
        .add_torrent(&bytes, dir.clone(), override_opts)
        .await
        .unwrap();
    assert_eq!(id.len(), 40);
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_recheck_repairs_a_corrupted_piece() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let dir = temp_dir("disk-recheck-out");
    let faulty = Arc::new(FaultyFs::new());
    let seeder = addr(9531);
    // The corrupted piece downloads slowly, so the recheck's dip is
    // observable instead of racing the poll.
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(
            seeder,
            SeederKind::Slow {
                delay: Duration::from_secs(2),
                only_piece: Some(1),
            },
        )],
    ));
    let session = session_with(faulty.clone(), dial, Some(seeder)).await;
    let id = session
        .add_torrent(&bytes, dir.clone(), AddOptions::default())
        .await
        .unwrap();
    let summaries = session.subscribe();
    wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        30,
    )
    .await;

    // Corrupt one byte inside the second piece.
    let file = dir.join("e2e.bin");
    let mut contents = std::fs::read(&file).unwrap();
    contents[16384 + 5] ^= 0xFF;
    std::fs::write(&file, &contents).unwrap();

    session.force_recheck(&id).await.unwrap();
    // The recheck must notice the corruption: the verified bytes dip first.
    wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && t.verified_bytes < t.total_length)
        },
        30,
    )
    .await;
    let snapshot = wait_for(
        &summaries,
        |s| {
            s.iter().any(|t| {
                t.id == id
                    && matches!(t.state, State::Completed | State::Seeding)
                    && t.verified_bytes == t.total_length
            })
        },
        30,
    )
    .await;
    let summary = snapshot.iter().find(|t| t.id == id).unwrap();
    assert_eq!(
        summary.verified_bytes, summary.total_length,
        "the corrupted piece must be re-downloaded and repaired: {summary:?}"
    );
    let repaired = std::fs::read(&file).unwrap();
    assert_eq!(repaired, *data);
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_recheck_rewrites_the_resume_state() {
    let data = test_data();
    let bytes = torrent_bytes(&data);
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes).unwrap();
    let info_hash_hex = bt_core::hex::encode(&meta.info_hash);
    let data_dir = temp_dir("disk-resume-data");
    let dir = temp_dir("disk-resume-out");
    std::fs::create_dir_all(&dir).unwrap();
    // Pre-seed the output file so the initial recheck verifies everything.
    std::fs::write(dir.join("e2e.bin"), data.as_ref()).unwrap();

    // No peers at all: after the corruption nothing can be re-downloaded.
    let faulty = Arc::new(FaultyFs::new());
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = session_with_persistence(faulty.clone(), dial, data_dir.clone()).await;
    let id = session
        .add_torrent(&bytes, dir.clone(), AddOptions::default())
        .await
        .unwrap();
    let summaries = session.subscribe();
    wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        30,
    )
    .await;

    let snapshot_path = data_dir.join(format!("{info_hash_hex}.resume"));
    assert!(
        snapshot_path.exists(),
        "a snapshot must exist after recheck"
    );
    let before = std::fs::read_to_string(&snapshot_path).unwrap();

    // Corrupt one byte inside the first piece and recheck: the piece is
    // lost, and the resume state must be rewritten to match reality.
    let file = dir.join("e2e.bin");
    let mut contents = std::fs::read(&file).unwrap();
    contents[5] ^= 0xFF;
    std::fs::write(&file, &contents).unwrap();

    session.force_recheck(&id).await.unwrap();
    wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && t.state == State::Downloading)
        },
        30,
    )
    .await;
    let after = std::fs::read_to_string(&snapshot_path).unwrap();
    assert_ne!(
        before, after,
        "the resume snapshot must be rewritten after a recheck loses a piece"
    );
    session.shutdown().await.unwrap();
    cleanup_dir(&data_dir);
    cleanup_dir(&dir);
}
