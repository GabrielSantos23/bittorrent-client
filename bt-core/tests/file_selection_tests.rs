mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bt_core::bencode::{self, Value};
use bt_core::engine::{FilePriority, State};
use bt_core::metainfo::MetaInfo;
use bt_core::session::{AddOptions, MagnetOptions, Session};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use common::{temp_dir, FakeDial, SeederKind, PIECE_LENGTH};

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

/// Builds a multi-file torrent plus its deterministic content
/// (`data[i] == (i % 251) as u8`).
fn multi_file_torrent(
    files: &[(&str, u64)],
    piece_length: usize,
    announce: Option<&str>,
) -> (Vec<u8>, Arc<Vec<u8>>) {
    let total: u64 = files.iter().map(|(_, length)| length).sum();
    let data: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    let mut pieces = Vec::new();
    for chunk in data.chunks(piece_length) {
        let digest: [u8; 20] = Sha1::digest(chunk).into();
        pieces.extend_from_slice(&digest);
    }
    let file_list: Vec<Value> = files
        .iter()
        .map(|(name, length)| {
            let mut dict: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
            dict.insert(b"length".to_vec(), Value::Int(*length as i64));
            dict.insert(
                b"path".to_vec(),
                Value::List(vec![Value::Bytes(name.as_bytes().to_vec())]),
            );
            Value::Dict(dict)
        })
        .collect();
    let mut info: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    info.insert(b"files".to_vec(), Value::List(file_list));
    info.insert(b"name".to_vec(), Value::Bytes(b"sel".to_vec()));
    info.insert(b"piece length".to_vec(), Value::Int(piece_length as i64));
    info.insert(b"pieces".to_vec(), Value::Bytes(pieces));
    let mut root: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    root.insert(b"info".to_vec(), Value::Dict(info));
    if let Some(announce) = announce {
        root.insert(
            b"announce".to_vec(),
            Value::Bytes(announce.as_bytes().to_vec()),
        );
    }
    (bencode::encode(&Value::Dict(root)), Arc::new(data))
}

/// Layout spanning three pieces with boundaries:
/// part0 = bytes 0..20000 (pieces 0+1), part1 = bytes 20000..30000 (piece 1),
/// part2 = bytes 30000..46384 (pieces 1+2). Piece length 16384.
const SPANNING_LAYOUT: &[(&str, u64)] = &[("part0", 20000), ("part1", 10000), ("part2", 16384)];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_only_the_selected_file_of_a_multi_file_torrent() {
    let (bytes, data) = multi_file_torrent(SPANNING_LAYOUT, PIECE_LENGTH, None);
    let meta = MetaInfo::from_bytes(&bytes).unwrap();
    assert_eq!(meta.info.pieces.len(), 3);
    let dir = temp_dir("sel-file");
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(addr(9401), SeederKind::Good)],
    ));
    let session = Session::spawn_with_dial(None, dial, vec![addr(9401)])
        .await
        .unwrap();
    let id = session
        .add_torrent(
            &bytes,
            dir.clone(),
            AddOptions {
                paused: false,
                file_priorities: vec![
                    (0, FilePriority::Skip),
                    (1, FilePriority::Normal),
                    (2, FilePriority::Skip),
                ],
            },
        )
        .await
        .unwrap();

    let summaries = session.subscribe();
    let snapshot = wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        30,
    )
    .await;
    let summary = snapshot.iter().find(|t| t.name == "sel").unwrap();
    assert_eq!(
        summary.verified_bytes, PIECE_LENGTH as u64,
        "exactly one piece is verified"
    );
    assert_eq!(summary.wanted_bytes, 10000);
    assert!(
        (summary.progress - 1.0).abs() < 1e-9,
        "progress is relative to the wanted bytes: {}",
        summary.progress
    );

    let detail = session.detail(&id).await.unwrap();
    assert_eq!(detail.files.len(), 3);
    assert_eq!(detail.files[0].priority, FilePriority::Skip);
    assert_eq!(detail.files[1].priority, FilePriority::Normal);
    assert_eq!(detail.files[1].verified_bytes, 10000);
    // The boundary piece covers part0's last 3616 bytes and part2's first 2768.
    assert_eq!(detail.files[0].verified_bytes, 3616);
    assert_eq!(detail.files[2].verified_bytes, 2768);

    // part1 holds exactly its bytes.
    let part1 = std::fs::read(dir.join("sel/part1")).unwrap();
    assert_eq!(part1, data[20000..30000].to_vec());
    // part0 was created on demand for the boundary piece: sparse head, real tail.
    let part0 = std::fs::read(dir.join("sel/part0")).unwrap();
    assert_eq!(part0.len(), 20000);
    assert_eq!(&part0[..16384], &[0u8; 16384][..]);
    assert_eq!(&part0[16384..], &data[16384..20000]);
    // part2 was also created by the boundary piece, but only its head is real.
    let part2 = std::fs::read(dir.join("sel/part2")).unwrap();
    assert_eq!(part2.len(), 16384);
    assert_eq!(&part2[..2768], &data[30000..32768]);
    assert_eq!(&part2[2768..], &[0u8; 16384 - 2768][..]);
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn priority_change_at_runtime_cancels_in_flight_blocks() {
    // part0 = two pieces, part1 = one piece. Piece 0 downloads immediately,
    // piece 1's block is deliberately slow and therefore in flight when the
    // priorities change.
    let (bytes, data) = multi_file_torrent(
        &[
            ("part0", 2 * PIECE_LENGTH as u64),
            ("part1", PIECE_LENGTH as u64),
        ],
        PIECE_LENGTH,
        None,
    );
    let meta = MetaInfo::from_bytes(&bytes).unwrap();
    let dir = temp_dir("sel-cancel");
    let (dial, mut reports) = FakeDial::with_reports(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(
            addr(9411),
            SeederKind::Slow {
                delay: Duration::from_secs(30),
                only_piece: Some(1),
            },
        )],
    );
    let session = Session::spawn_with_dial(None, Arc::new(dial), vec![addr(9411)])
        .await
        .unwrap();
    let id = session
        .add_torrent(
            &bytes,
            dir.clone(),
            AddOptions {
                paused: false,
                file_priorities: vec![(1, FilePriority::Skip)],
            },
        )
        .await
        .unwrap();

    let summaries = session.subscribe();
    wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && t.verified_bytes >= PIECE_LENGTH as u64)
        },
        30,
    )
    .await;
    // Piece 0 is verified while piece 1's request is pending on the seeder.

    session
        .set_file_priorities(&id, vec![(0, FilePriority::Skip)])
        .await
        .unwrap();
    wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        10,
    )
    .await;

    let report = reports
        .recv()
        .await
        .expect("seeder report after disconnect");
    assert!(
        report.cancels_received >= 1,
        "the in-flight block of the piece that became unwanted must be cancelled: {report:?}"
    );
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

/// An HTTP tracker that records every announce path it is asked for.
async fn spawn_recording_tracker() -> (SocketAddr, tokio::sync::mpsc::Receiver<String>) {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut byte = [0u8; 1];
                while let Ok(1) = stream.read(&mut byte).await {
                    buffer.push(byte[0]);
                    if buffer.ends_with(b"\r\n\r\n") || buffer.len() > 16 * 1024 {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&buffer).into_owned();
                if let Some(path) = request.split(' ').nth(1) {
                    let _ = tx.send(path.to_string()).await;
                }
                let body = b"d8:completei1e10:incompletei0e8:intervali1e5:peers0:e";
                let head = format!(
                    "HTTP/1.0 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body).await;
            });
        }
    });
    (addr, rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn announce_left_counts_all_unverified_bytes_and_completed_event_waits_for_everything() {
    let (tracker_addr, mut announces) = spawn_recording_tracker().await;
    // part0 = piece 0 (skipped), part1 = piece 1 (wanted).
    let (bytes, data) = multi_file_torrent(
        &[
            ("part0", PIECE_LENGTH as u64),
            ("part1", PIECE_LENGTH as u64),
        ],
        PIECE_LENGTH,
        Some(&format!("http://{tracker_addr}/announce")),
    );
    let meta = MetaInfo::from_bytes(&bytes).unwrap();
    let dir = temp_dir("sel-announce");
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(addr(9421), SeederKind::Good)],
    ));
    let session = Session::spawn_with_dial(None, dial, vec![addr(9421)])
        .await
        .unwrap();
    let id = session
        .add_torrent(
            &bytes,
            dir.clone(),
            AddOptions {
                paused: false,
                file_priorities: vec![(0, FilePriority::Skip)],
            },
        )
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

    // While part0's piece stays unverified we must not look like a seed:
    // left counts every unverified byte of the torrent.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut saw_partial_left = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), announces.recv()).await {
            Ok(Some(path)) => {
                assert!(
                    !path.contains("event=completed"),
                    "the completed event must wait for the whole torrent: {path}"
                );
                if path.contains("left=16384") {
                    saw_partial_left = true;
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }
    assert!(
        saw_partial_left,
        "announces after the wanted files completed must still report left=16384"
    );

    // Re-enabling part0 downloads its piece; only now is the torrent complete.
    session
        .set_file_priorities(&id, vec![(0, FilePriority::Normal)])
        .await
        .unwrap();
    wait_for(
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    let mut saw_completed_event = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), announces.recv()).await {
            Ok(Some(path)) => {
                if path.contains("event=completed") {
                    assert!(
                        path.contains("left=0"),
                        "a completed announce must not carry bytes left: {path}"
                    );
                    saw_completed_event = true;
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }
    assert!(
        saw_completed_event,
        "the completed event was never announced"
    );
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_after_metadata_lets_the_user_choose_before_any_download() {
    let (bytes, data) = multi_file_torrent(SPANNING_LAYOUT, PIECE_LENGTH, None);
    let meta = MetaInfo::from_bytes(&bytes).unwrap();
    let root_value = bencode::decode(&bytes).unwrap();
    let info_value = match &root_value {
        Value::Dict(entries) => entries.get(&b"info".to_vec()).unwrap().clone(),
        _ => unreachable!(),
    };
    let info_dict = Arc::new(bencode::encode(&info_value));
    let dir = temp_dir("sel-magnet");
    let dial = Arc::new(FakeDial::with_metadata(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(addr(9431), SeederKind::Good)],
        info_dict,
    ));
    let session = Session::spawn_with_dial(None, dial, vec![addr(9431)])
        .await
        .unwrap();
    let uri = format!(
        "magnet:?xt=urn:btih:{}",
        bt_core::hex::encode(&meta.info_hash)
    );
    let id = session
        .add_magnet(
            &uri,
            dir.clone(),
            MagnetOptions {
                paused: false,
                pause_after_metadata: true,
                file_priorities: Vec::new(),
            },
        )
        .await
        .unwrap();

    let summaries = session.subscribe();
    wait_for(
        &summaries,
        |s| s.iter().any(|t| t.id == id && t.state == State::Paused),
        30,
    )
    .await;
    let detail = session
        .detail(&id)
        .await
        .expect("torrent exists after metadata arrives");
    assert_eq!(detail.files.len(), 3, "the file list is known while paused");
    assert!(
        detail.files.iter().all(|file| file.verified_bytes == 0),
        "no data may be downloaded before the user chooses: {detail:?}"
    );

    session
        .set_file_priorities(
            &id,
            vec![
                (0, FilePriority::Skip),
                (1, FilePriority::Normal),
                (2, FilePriority::Skip),
            ],
        )
        .await
        .unwrap();
    session.resume(&id).await.unwrap();
    wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        30,
    )
    .await;
    let part1 = std::fs::read(dir.join("sel/part1")).unwrap();
    assert_eq!(part1, data[20000..30000].to_vec());
    session.shutdown().await.unwrap();
    cleanup_dir(&dir);
}

/// Piece-aligned layout: part0 = piece 0, part1 = piece 1, part2 = piece 2
/// (short last piece). Skipping part0 means its file is never created.
const ALIGNED_LAYOUT: &[(&str, u64)] = &[
    ("part0", PIECE_LENGTH as u64),
    ("part1", PIECE_LENGTH as u64),
    ("part2", 13616),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn priorities_persist_across_restarts() {
    let (bytes, data) = multi_file_torrent(ALIGNED_LAYOUT, PIECE_LENGTH, None);
    let meta = MetaInfo::from_bytes(&bytes).unwrap();
    let data_dir = temp_dir("sel-persist-data");
    let dir = temp_dir("sel-persist-out");
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![],
    ));
    let session = Session::spawn_with_dial(Some(data_dir.clone()), dial, vec![])
        .await
        .unwrap();
    let id = session
        .add_torrent(
            &bytes,
            dir.clone(),
            AddOptions {
                paused: false,
                file_priorities: vec![(0, FilePriority::High), (1, FilePriority::Skip)],
            },
        )
        .await
        .unwrap();
    wait_for(&session.subscribe(), |s| s.iter().any(|t| t.id == id), 10).await;
    session.shutdown().await.unwrap();

    let restored = Session::spawn_with_dial(
        Some(data_dir.clone()),
        Arc::new(FakeDial::new(
            meta.info_hash,
            data.clone(),
            meta.info.pieces.len(),
            vec![],
        )),
        vec![],
    )
    .await
    .unwrap();
    let detail = restored.detail(&id).await.unwrap();
    assert_eq!(detail.files[0].priority, FilePriority::High);
    assert_eq!(detail.files[1].priority, FilePriority::Skip);
    assert_eq!(detail.files[2].priority, FilePriority::Normal);

    // Runtime priority changes persist too.
    restored
        .set_file_priorities(
            &id,
            vec![(1, FilePriority::Normal), (2, FilePriority::Skip)],
        )
        .await
        .unwrap();
    restored.shutdown().await.unwrap();
    let again = Session::spawn_with_dial(
        Some(data_dir.clone()),
        Arc::new(FakeDial::new(
            meta.info_hash,
            data.clone(),
            meta.info.pieces.len(),
            vec![],
        )),
        vec![],
    )
    .await
    .unwrap();
    let detail = again.detail(&id).await.unwrap();
    assert_eq!(detail.files[0].priority, FilePriority::High);
    assert_eq!(detail.files[1].priority, FilePriority::Normal);
    assert_eq!(detail.files[2].priority, FilePriority::Skip);
    again.shutdown().await.unwrap();
    cleanup_dir(&data_dir);
    cleanup_dir(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_works_when_a_skipped_file_was_never_created() {
    let (bytes, data) = multi_file_torrent(ALIGNED_LAYOUT, PIECE_LENGTH, None);
    let meta = MetaInfo::from_bytes(&bytes).unwrap();
    let data_dir = temp_dir("sel-resume-data");
    let dir = temp_dir("sel-resume-out");
    let dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(addr(9451), SeederKind::Good)],
    ));
    let session = Session::spawn_with_dial(Some(data_dir.clone()), dial, vec![addr(9451)])
        .await
        .unwrap();
    let id = session
        .add_torrent(
            &bytes,
            dir.clone(),
            AddOptions {
                paused: false,
                file_priorities: vec![(0, FilePriority::Skip)],
            },
        )
        .await
        .unwrap();
    let summaries = session.subscribe();
    wait_for(
        &summaries,
        |s| {
            s.iter().any(|t| {
                t.id == id
                    && matches!(t.state, State::Completed | State::Seeding)
                    && t.verified_bytes == 16384 + 13616
            })
        },
        30,
    )
    .await;
    assert!(!dir.join("sel/part0").exists());
    session.shutdown().await.unwrap();

    // A snapshot was written despite the missing skipped file.
    let snapshot = data_dir.join(format!("{id}.resume"));
    assert!(snapshot.exists(), "the resume snapshot must be written");

    let restored_dial = Arc::new(FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(addr(9452), SeederKind::Good)],
    ));
    let restored =
        Session::spawn_with_dial(Some(data_dir.clone()), restored_dial, vec![addr(9452)])
            .await
            .unwrap();
    let summaries = restored.subscribe();
    let first = wait_for(&summaries, |s| s.iter().any(|t| t.id == id), 10).await;
    let summary = first.iter().find(|t| t.id == id).unwrap();
    assert_ne!(
        summary.state,
        State::Checking,
        "a missing skipped file must not force a recheck"
    );
    let restored_summary = wait_for(
        &summaries,
        |s| {
            s.iter()
                .any(|t| t.id == id && matches!(t.state, State::Completed | State::Seeding))
        },
        30,
    )
    .await;
    let summary = restored_summary.iter().find(|t| t.id == id).unwrap();
    assert_eq!(
        summary.verified_bytes,
        16384 + 13616,
        "the verified pieces of the wanted files are restored"
    );
    assert!(!dir.join("sel/part0").exists());
    restored.shutdown().await.unwrap();
    cleanup_dir(&data_dir);
    cleanup_dir(&dir);
}
