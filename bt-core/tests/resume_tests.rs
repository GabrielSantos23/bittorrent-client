use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use bt_core::engine::{State, Torrent, TorrentOptions};
use bt_core::metainfo::MetaInfo;
use serde_json::json;
use sha1::{Digest, Sha1};

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

const PIECE_LENGTH: usize = 4;

fn single_file_meta(data: &[u8]) -> MetaInfo {
    let bytes = common_torrent_bytes(data, "e2e.bin");
    MetaInfo::from_bytes(&bytes).unwrap()
}

fn common_torrent_bytes(data: &[u8], name: &str) -> Vec<u8> {
    let mut pieces = Vec::new();
    for chunk in data.chunks(PIECE_LENGTH) {
        let digest: [u8; 20] = Sha1::digest(chunk).into();
        pieces.extend_from_slice(&digest);
    }
    let mut info = std::collections::BTreeMap::new();
    info.insert(
        b"length".to_vec(),
        bt_core::bencode::Value::Int(data.len() as i64),
    );
    info.insert(
        b"name".to_vec(),
        bt_core::bencode::Value::Bytes(name.as_bytes().to_vec()),
    );
    info.insert(
        b"piece length".to_vec(),
        bt_core::bencode::Value::Int(PIECE_LENGTH as i64),
    );
    info.insert(b"pieces".to_vec(), bt_core::bencode::Value::Bytes(pieces));
    let mut root = std::collections::BTreeMap::new();
    root.insert(b"info".to_vec(), bt_core::bencode::Value::Dict(info));
    bt_core::bencode::encode(&bt_core::bencode::Value::Dict(root))
}

fn multi_file_meta(file_lengths: &[u64], hashes: &[[u8; 20]]) -> MetaInfo {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"d4:infod5:filesl");
    for (index, length) in file_lengths.iter().enumerate() {
        raw.extend_from_slice(format!("d6:lengthi{length}e4:pathl9:file{index}.binee").as_bytes());
    }
    raw.extend_from_slice(b"e4:name3:dir12:piece lengthi4e6:pieces");
    raw.extend_from_slice((hashes.len() * 20).to_string().as_bytes());
    raw.push(b':');
    for hash in hashes {
        raw.extend_from_slice(hash);
    }
    raw.extend_from_slice(b"ee");
    MetaInfo::from_bytes(&raw).unwrap()
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("bt-resume-e2e-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_content(meta: &MetaInfo, out: &Path, data: &[u8]) {
    match &meta.info.content {
        bt_core::metainfo::Content::Single { .. } => {
            std::fs::write(out.join(&meta.info.name), data).unwrap();
        }
        bt_core::metainfo::Content::Multi { files } => {
            let root = out.join(&meta.info.name);
            std::fs::create_dir_all(&root).unwrap();
            let mut offset = 0usize;
            for file in files {
                let end = offset + file.length as usize;
                std::fs::write(root.join(file.path.join("/")), &data[offset..end]).unwrap();
                offset = end;
            }
        }
    }
}

fn fingerprint_of(path: &Path) -> serde_json::Value {
    let metadata = std::fs::metadata(path).unwrap();
    let modified = metadata
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap();
    json!({
        "length": metadata.len(),
        "modified_secs": modified.as_secs(),
        "modified_nanos": modified.subsec_nanos(),
    })
}

async fn spawn(meta: MetaInfo, out: &Path, resume_dir: Option<&Path>) -> Torrent {
    let options = TorrentOptions {
        resume_dir: resume_dir.map(|dir| dir.to_path_buf()),
        ..TorrentOptions::default()
    };
    Torrent::spawn_with_options(meta, out.to_path_buf(), options)
        .await
        .unwrap()
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {seconds}s: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn snapshot_entries(dir: &Path) -> Vec<String> {
    let mut entries: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    entries
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_persists_a_complete_snapshot_without_temp_files() {
    let data: Vec<u8> = (0..12 * PIECE_LENGTH).map(|i| (i % 251) as u8).collect();
    let meta = single_file_meta(&data);
    let id = bt_core::hex::encode(&meta.info_hash);
    let out = temp_dir("complete-out");
    let resume_dir = temp_dir("complete-resume");
    write_content(&meta, &out, &data);

    let torrent = spawn(meta.clone(), &out, Some(&resume_dir)).await;
    let stats = torrent.subscribe();
    let finished = wait_for(&stats, |s| matches!(s.state, State::Completed), 20).await;
    assert_eq!(finished.verified_bytes as usize, data.len());

    let path = resume_dir.join(format!("{id}.resume"));
    assert!(path.exists(), "completion must persist the resume file");
    let snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(snapshot["version"], 1);
    assert_eq!(snapshot["piece_count"], 12);
    assert_eq!(snapshot["info_hash"], id);
    let bitfield = snapshot["bitfield"].as_array().unwrap();
    assert_eq!(bitfield.len(), 2);
    assert_eq!(bitfield[1], 0xF0, "12 pieces leave four spare bits");
    assert_eq!(
        snapshot_entries(&resume_dir),
        vec![format!("{id}.resume")],
        "no temp file may survive"
    );
    torrent.stop().await.unwrap();
    cleanup_dir(&out);
    cleanup_dir(&resume_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_valid_resume_hashes_only_the_sample() {
    let data: Vec<u8> = (0..40 * PIECE_LENGTH).map(|i| (i % 251) as u8).collect();
    let meta = single_file_meta(&data);
    let out = temp_dir("sample-out");
    let resume_dir = temp_dir("sample-resume");
    write_content(&meta, &out, &data);

    let first = spawn(meta.clone(), &out, Some(&resume_dir)).await;
    let first_stats = first.subscribe();
    wait_for(
        &first_stats,
        |s| matches!(s.state, State::Completed) && s.startup_pieces_hashed == 40,
        20,
    )
    .await;
    first.stop().await.unwrap();
    drop(first);

    let second = spawn(meta.clone(), &out, Some(&resume_dir)).await;
    let second_stats = second.subscribe();
    let initial = second_stats.borrow().clone();
    assert!(
        initial.resumed_from_saved_state,
        "the second run must report the resume"
    );
    assert_ne!(initial.state, State::Checking);
    let finished = wait_for(
        &second_stats,
        |s| matches!(s.state, State::Completed) && s.startup_pieces_hashed > 0,
        20,
    )
    .await;
    assert_eq!(
        finished.startup_pieces_hashed, 8,
        "40 verified pieces sample down to 8 hashed pieces"
    );
    assert_eq!(finished.verified_bytes as usize, data.len());
    assert!(finished.resume_fallback.is_none());
    second.stop().await.unwrap();
    cleanup_dir(&out);
    cleanup_dir(&resume_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tampered_piece_caught_by_the_sample_falls_back_to_a_full_recheck() {
    let data: Vec<u8> = (0..3 * PIECE_LENGTH).map(|i| (i % 251) as u8).collect();
    let mut tampered = data.clone();
    tampered[5] ^= 0xFF;
    let meta = single_file_meta(&data);
    let out = temp_dir("tamper-out");
    let resume_dir = temp_dir("tamper-resume");
    std::fs::write(out.join("e2e.bin"), &tampered).unwrap();
    let id = bt_core::hex::encode(&meta.info_hash);
    let snapshot = json!({
        "version": 1,
        "info_hash": id,
        "piece_count": 3,
        "bitfield": [0xE0],
        "files": [fingerprint_of(&out.join("e2e.bin"))],
    });
    std::fs::write(
        resume_dir.join(format!("{id}.resume")),
        serde_json::to_vec(&snapshot).unwrap(),
    )
    .unwrap();

    let torrent = spawn(meta, &out, Some(&resume_dir)).await;
    let stats = torrent.subscribe();
    let settled = wait_for(
        &stats,
        |s| {
            s.startup_pieces_hashed > 0
                && matches!(
                    s.state,
                    State::Downloading | State::Completed | State::Seeding | State::Error
                )
        },
        20,
    )
    .await;
    assert!(settled.resumed_from_saved_state);
    assert_eq!(
        settled.startup_pieces_hashed, 6,
        "the 3-piece sample plus the 3-piece full recheck"
    );
    assert_eq!(
        settled.resume_fallback.as_deref(),
        Some("resume sample verification failed")
    );
    assert_eq!(
        settled.verified_bytes as usize,
        2 * PIECE_LENGTH,
        "the full recheck verifies the pieces the tamper did not touch"
    );
    torrent.stop().await.unwrap();
    cleanup_dir(&out);
    cleanup_dir(&resume_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_file_mtime_reverifies_only_overlapping_pieces() {
    let file_a: Vec<u8> = (0..10 * PIECE_LENGTH).map(|i| (i % 249) as u8).collect();
    let file_b: Vec<u8> = (0..10 * PIECE_LENGTH).map(|i| (i % 251) as u8).collect();
    let mut data = file_a.clone();
    data.extend_from_slice(&file_b);
    let mut hashes = Vec::new();
    for chunk in data.chunks(PIECE_LENGTH) {
        hashes.push(Sha1::digest(chunk).into());
    }
    let meta = multi_file_meta(&[40, 40], &hashes);
    let out = temp_dir("mtime-out");
    let resume_dir = temp_dir("mtime-resume");
    write_content(&meta, &out, &data);

    let first = spawn(meta.clone(), &out, Some(&resume_dir)).await;
    let first_stats = first.subscribe();
    wait_for(
        &first_stats,
        |s| matches!(s.state, State::Completed) && s.startup_pieces_hashed == 20,
        20,
    )
    .await;
    first.stop().await.unwrap();
    drop(first);

    let path_b = out.join("dir").join("file1.bin");
    std::fs::write(&path_b, &file_b).unwrap();

    let second = spawn(meta, &out, Some(&resume_dir)).await;
    let second_stats = second.subscribe();
    let finished = wait_for(
        &second_stats,
        |s| matches!(s.state, State::Completed) && s.startup_pieces_hashed > 0,
        20,
    )
    .await;
    assert!(finished.resumed_from_saved_state);
    assert_eq!(
        finished.startup_pieces_hashed, 18,
        "8 sampled pieces of file0 plus the 10 overlapping pieces of file1"
    );
    assert_eq!(finished.verified_bytes as usize, data.len());
    second.stop().await.unwrap();
    cleanup_dir(&out);
    cleanup_dir(&resume_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_piece_count_snapshot_falls_back_to_a_full_recheck() {
    let data: Vec<u8> = (0..6 * PIECE_LENGTH).map(|i| (i % 251) as u8).collect();
    let meta = single_file_meta(&data);
    let id = bt_core::hex::encode(&meta.info_hash);
    let out = temp_dir("wrongcount-out");
    let resume_dir = temp_dir("wrongcount-resume");
    write_content(&meta, &out, &data);
    let snapshot = json!({
        "version": 1,
        "info_hash": id,
        "piece_count": 99,
        "bitfield": [0xFC],
        "files": [fingerprint_of(&out.join("e2e.bin"))],
    });
    std::fs::write(
        resume_dir.join(format!("{id}.resume")),
        serde_json::to_vec(&snapshot).unwrap(),
    )
    .unwrap();

    let torrent = spawn(meta, &out, Some(&resume_dir)).await;
    let stats = torrent.subscribe();
    let settled = wait_for(
        &stats,
        |s| {
            matches!(
                s.state,
                State::Completed | State::Downloading | State::Seeding
            )
        },
        20,
    )
    .await;
    assert!(!settled.resumed_from_saved_state);
    assert!(
        settled
            .resume_fallback
            .as_deref()
            .is_some_and(|reason| reason.contains("99")),
        "the mismatch must be reported once: {:?}",
        settled.resume_fallback
    );
    assert_eq!(settled.startup_pieces_hashed, 6);
    assert_eq!(settled.verified_bytes as usize, data.len());
    torrent.stop().await.unwrap();
    cleanup_dir(&out);
    cleanup_dir(&resume_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_info_hash_snapshot_falls_back_to_a_full_recheck() {
    let data: Vec<u8> = (0..6 * PIECE_LENGTH).map(|i| (i % 251) as u8).collect();
    let meta = single_file_meta(&data);
    let id = bt_core::hex::encode(&meta.info_hash);
    let out = temp_dir("wronghash-out");
    let resume_dir = temp_dir("wronghash-resume");
    write_content(&meta, &out, &data);
    let snapshot = json!({
        "version": 1,
        "info_hash": "0000000000000000000000000000000000000000",
        "piece_count": 6,
        "bitfield": [0xFC],
        "files": [fingerprint_of(&out.join("e2e.bin"))],
    });
    std::fs::write(
        resume_dir.join(format!("{id}.resume")),
        serde_json::to_vec(&snapshot).unwrap(),
    )
    .unwrap();

    let torrent = spawn(meta, &out, Some(&resume_dir)).await;
    let stats = torrent.subscribe();
    let settled = wait_for(
        &stats,
        |s| {
            matches!(
                s.state,
                State::Completed | State::Downloading | State::Seeding
            )
        },
        20,
    )
    .await;
    assert!(!settled.resumed_from_saved_state);
    assert!(settled.resume_fallback.is_some());
    assert_eq!(settled.verified_bytes as usize, data.len());
    torrent.stop().await.unwrap();
    cleanup_dir(&out);
    cleanup_dir(&resume_dir);
}
