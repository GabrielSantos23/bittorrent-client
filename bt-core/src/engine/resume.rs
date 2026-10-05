use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::error::ResumeError;
use crate::metainfo::MetaInfo;
use crate::peer::Bitfield;

use super::storage::Storage;

pub const RESUME_FORMAT_VERSION: u32 = 1;
pub const MAX_RESUME_FILE_BYTES: u64 = 4 * 1024 * 1024;
const SAMPLE_MINIMUM: usize = 8;
const SAMPLE_CAP: usize = 64;
const SAMPLE_PERCENTAGE: usize = 100;
const SNAPSHOT_SUFFIX: &str = ".resume";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFingerprint {
    pub length: u64,
    pub modified_secs: u64,
    pub modified_nanos: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeSnapshot {
    pub version: u32,
    pub info_hash: String,
    pub piece_count: usize,
    pub bitfield: Vec<u8>,
    /// One entry per torrent file. `None` marks a skipped file that did not
    /// exist when the snapshot was written; fingerprint checks do not apply
    /// to it.
    pub files: Vec<Option<FileFingerprint>>,
}

#[derive(Debug, Clone)]
pub struct ResumeStartup {
    pub trusted: Bitfield,
    pub overlaps: Vec<usize>,
    pub sample: Vec<usize>,
}

pub fn snapshot_path(dir: &Path, info_hash_hex: &str) -> PathBuf {
    dir.join(format!("{info_hash_hex}{SNAPSHOT_SUFFIX}"))
}

pub fn remove_snapshot(dir: &Path, info_hash_hex: &str) -> bool {
    let path = snapshot_path(dir, info_hash_hex);
    let temp = temp_sibling(&path);
    let removed_snapshot = std::fs::remove_file(&path).is_ok();
    let removed_temp = std::fs::remove_file(&temp).is_ok();
    removed_snapshot || removed_temp
}

pub fn write_snapshot(path: &Path, snapshot: &ResumeSnapshot) -> Result<(), ResumeError> {
    let bytes = serde_json::to_vec(snapshot).map_err(|_| ResumeError::Corrupt)?;
    let temp = temp_sibling(path);
    {
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&temp, path)?;
    Ok(())
}

pub fn load_snapshot(path: &Path) -> Result<ResumeSnapshot, ResumeError> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(ResumeError::Missing),
        Err(err) => return Err(err.into()),
    };
    if metadata.len() > MAX_RESUME_FILE_BYTES {
        return Err(ResumeError::TooLarge(metadata.len(), MAX_RESUME_FILE_BYTES));
    }
    let bytes = std::fs::read(path)?;
    let snapshot: ResumeSnapshot =
        serde_json::from_slice(&bytes).map_err(|_| ResumeError::Corrupt)?;
    if snapshot.version != RESUME_FORMAT_VERSION {
        return Err(ResumeError::UnknownVersion(snapshot.version));
    }
    Ok(snapshot)
}

pub fn current_fingerprints(paths: &[PathBuf]) -> Vec<Option<FileFingerprint>> {
    paths
        .iter()
        .map(
            |path| match std::fs::metadata(crate::paths::prepare_file_path(path)) {
                Ok(metadata) if metadata.is_file() => {
                    metadata.modified().ok().and_then(|modified| {
                        let since_epoch = modified.duration_since(UNIX_EPOCH).ok()?;
                        Some(FileFingerprint {
                            length: metadata.len(),
                            modified_secs: since_epoch.as_secs(),
                            modified_nanos: since_epoch.subsec_nanos(),
                        })
                    })
                }
                _ => None,
            },
        )
        .collect()
}

pub fn startup_plan(
    dir: &Path,
    meta: &MetaInfo,
    storage: &Storage,
) -> Result<ResumeStartup, ResumeError> {
    let info_hash_hex = crate::hex::encode(&meta.info_hash);
    let snapshot = load_snapshot(&snapshot_path(dir, &info_hash_hex))?;
    if snapshot.info_hash != info_hash_hex {
        return Err(ResumeError::InfoHashMismatch);
    }
    let piece_count = meta.info.pieces.len();
    if snapshot.piece_count != piece_count {
        return Err(ResumeError::PieceCountMismatch {
            expected: piece_count,
            actual: snapshot.piece_count,
        });
    }
    let paths = storage.file_paths();
    if snapshot.files.len() != paths.len() {
        return Err(ResumeError::FileCountMismatch {
            expected: paths.len(),
            actual: snapshot.files.len(),
        });
    }
    let lengths = storage.file_lengths();
    for (index, fingerprint) in snapshot.files.iter().enumerate() {
        if let Some(fingerprint) = fingerprint {
            if fingerprint.length != lengths[index] {
                return Err(ResumeError::FileLengthMismatch {
                    index,
                    expected: lengths[index],
                    actual: fingerprint.length,
                });
            }
        }
    }
    let mut trusted = Bitfield::from_bytes(&snapshot.bitfield, piece_count)?;
    let current = current_fingerprints(&paths);
    let mut overlaps: Vec<usize> = Vec::new();
    for (index, fingerprint) in current.iter().enumerate() {
        let should_exist = !storage
            .priorities()
            .get(index)
            .copied()
            .unwrap_or(crate::engine::FilePriority::Normal)
            .is_skip();
        // Fingerprint checks only apply to files that are supposed to exist:
        // non-skipped files, and skipped files that were created for boundary
        // pieces (those existed when the snapshot was written).
        let changed = match (&snapshot.files[index], fingerprint) {
            (Some(saved), Some(current)) => current != saved,
            (Some(_), None) | (None, Some(_)) => true,
            (None, None) => should_exist,
        };
        if changed {
            overlaps.extend(storage.pieces_overlapping_file(index));
        }
    }
    overlaps.sort_unstable();
    overlaps.dedup();
    for index in &overlaps {
        let _ = trusted.clear(*index);
    }
    let verified: Vec<usize> = (0..piece_count)
        .filter(|index| trusted.get(*index))
        .collect();
    let sample = sample_of(&verified);
    Ok(ResumeStartup {
        trusted,
        overlaps,
        sample,
    })
}

fn sample_of(verified: &[usize]) -> Vec<usize> {
    if verified.is_empty() {
        return Vec::new();
    }
    let one_percent = verified.len().div_ceil(SAMPLE_PERCENTAGE);
    let wanted = (SAMPLE_MINIMUM.max(one_percent))
        .min(SAMPLE_CAP)
        .min(verified.len());
    let mut rng = rand::rng();
    let mut sample: Vec<usize> = verified
        .choose_multiple(&mut rng, wanted)
        .copied()
        .collect();
    sample.sort_unstable();
    sample
}

pub fn verify_pieces(storage: &Storage, hashes: &[[u8; 20]], pieces: &[usize]) -> Vec<usize> {
    let mut buffer = vec![0u8; storage.piece_length() as usize];
    let mut failed = Vec::new();
    for &index in pieces {
        let Some(expected) = hashes.get(index) else {
            failed.push(index);
            continue;
        };
        let size = storage.piece_size(index);
        let verified = storage.read_piece(index, &mut buffer[..size]).is_ok()
            && Sha1::digest(&buffer[..size]).as_slice() == expected;
        if !verified {
            failed.push(index);
        }
    }
    failed
}

pub fn verified_length(storage: &Storage, have: &Bitfield) -> u64 {
    let mut total = 0u64;
    for index in 0..have.piece_count() {
        if have.get(index) {
            total += storage.piece_size(index) as u64;
        }
    }
    total
}

fn temp_sibling(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::MetaInfo;
    use std::time::Duration;

    const PIECE_LENGTH: usize = 4;

    fn multi_file_meta(hashes: &[[u8; 20]]) -> MetaInfo {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d4:infod5:filesl");
        raw.extend_from_slice(b"d6:lengthi5e4:pathl5:a.txtee");
        raw.extend_from_slice(b"d6:lengthi3e4:pathl3:sub5:b.binee");
        raw.extend_from_slice(b"d6:lengthi4e4:pathl5:c.txtee");
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
        let dir = std::env::temp_dir().join(format!("bt-resume-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn content() -> Vec<u8> {
        (1..=12).collect()
    }

    fn hashes_of(data: &[u8]) -> Vec<[u8; 20]> {
        data.chunks(PIECE_LENGTH)
            .map(|chunk| Sha1::digest(chunk).into())
            .collect()
    }

    fn storage_with_content(meta: &MetaInfo, dir: &Path, data: &[u8]) -> Storage {
        let priorities = vec![crate::engine::FilePriority::Normal; storage_file_count(meta)];
        let storage = Storage::create(meta, dir, &priorities).unwrap();
        let mut offset = 0usize;
        for index in 0..meta.info.pieces.len() {
            let size = storage.piece_size(index);
            storage
                .write_piece(index, &data[offset..offset + size])
                .unwrap();
            offset += size;
        }
        storage
    }

    fn storage_file_count(meta: &MetaInfo) -> usize {
        match &meta.info.content {
            crate::metainfo::Content::Single { .. } => 1,
            crate::metainfo::Content::Multi { files } => files.len(),
        }
    }

    fn wait_for_mtime_change(dir: &Path, relative: &[&str]) {
        let path = dir.join(relative.join("/"));
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        let content = std::fs::read(&path).unwrap();
        for _ in 0..10 {
            std::thread::sleep(Duration::from_millis(20));
            std::fs::write(&path, &content).unwrap();
            let after = std::fs::metadata(&path).unwrap().modified().unwrap();
            if after != before {
                return;
            }
        }
        panic!("mtime never changed");
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = temp_dir("roundtrip");
        let path = snapshot_path(&dir, "abc");
        let snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: "abc".to_string(),
            piece_count: 3,
            bitfield: vec![0xE0],
            files: vec![Some(FileFingerprint {
                length: 5,
                modified_secs: 100,
                modified_nanos: 5,
            })],
        };
        write_snapshot(&path, &snapshot).unwrap();
        assert_eq!(load_snapshot(&path).unwrap(), snapshot);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn atomic_write_leaves_no_temp_file() {
        let dir = temp_dir("atomic");
        let path = snapshot_path(&dir, "abc");
        let snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: "abc".to_string(),
            piece_count: 0,
            bitfield: Vec::new(),
            files: Vec::new(),
        };
        write_snapshot(&path, &snapshot).unwrap();
        let entries: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["abc.resume".to_string()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn missing_file_is_missing_not_corrupt() {
        let dir = temp_dir("missing");
        assert!(matches!(
            load_snapshot(&snapshot_path(&dir, "abc")),
            Err(ResumeError::Missing)
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn corrupt_and_truncated_files_are_rejected() {
        let dir = temp_dir("corrupt");
        let path = snapshot_path(&dir, "abc");
        std::fs::write(&path, b"{not json at all").unwrap();
        assert!(matches!(load_snapshot(&path), Err(ResumeError::Corrupt)));
        let full = serde_json::to_vec(&ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: "abc".to_string(),
            piece_count: 3,
            bitfield: vec![0xE0],
            files: Vec::new(),
        })
        .unwrap();
        std::fs::write(&path, &full[..full.len() - 6]).unwrap();
        assert!(matches!(load_snapshot(&path), Err(ResumeError::Corrupt)));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unknown_version_is_rejected() {
        let dir = temp_dir("version");
        let path = snapshot_path(&dir, "abc");
        std::fs::write(
            &path,
            br#"{"version":99,"info_hash":"abc","piece_count":1,"bitfield":[],"files":[]}"#,
        )
        .unwrap();
        assert!(matches!(
            load_snapshot(&path),
            Err(ResumeError::UnknownVersion(99))
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wrong_info_hash_is_rejected() {
        let dir = temp_dir("wronghash");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = storage_with_content(&meta, &dir, &data);
        assert!(matches!(
            startup_plan(&dir, &meta, &storage),
            Err(ResumeError::Missing)
        ));
        let snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: "different".to_string(),
            piece_count: 3,
            bitfield: vec![0xE0],
            files: Vec::new(),
        };
        write_snapshot(
            &snapshot_path(&dir, &crate::hex::encode(&meta.info_hash)),
            &snapshot,
        )
        .unwrap();
        assert!(matches!(
            startup_plan(&dir, &meta, &storage),
            Err(ResumeError::InfoHashMismatch)
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wrong_piece_count_is_rejected() {
        let dir = temp_dir("wrongpieces");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = storage_with_content(&meta, &dir, &data);
        let snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: crate::hex::encode(&meta.info_hash),
            piece_count: 99,
            bitfield: vec![0xE0],
            files: Vec::new(),
        };
        write_snapshot(&snapshot_path(&dir, &snapshot.info_hash), &snapshot).unwrap();
        let err = startup_plan(&dir, &meta, &storage).unwrap_err();
        assert!(matches!(
            err,
            ResumeError::PieceCountMismatch {
                expected: 3,
                actual: 99
            }
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn changed_file_length_only_reverifies_overlapping_pieces() {
        let dir = temp_dir("changedlength");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = storage_with_content(&meta, &dir, &data);
        let info_hash_hex = crate::hex::encode(&meta.info_hash);
        let mut snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: info_hash_hex.clone(),
            piece_count: 3,
            bitfield: vec![0xE0],
            files: current_fingerprints(&storage.file_paths()),
        };
        write_snapshot(&snapshot_path(&dir, &info_hash_hex), &snapshot).unwrap();

        std::fs::write(dir.join("dir/c.txt"), b"910").unwrap();
        let plan = startup_plan(&dir, &meta, &storage).unwrap();
        assert_eq!(plan.overlaps, vec![2], "only piece 2 overlaps c.txt");
        assert!(plan.trusted.get(0));
        assert!(plan.trusted.get(1));
        assert!(!plan.trusted.get(2));
        assert_eq!(plan.sample, vec![0, 1]);

        snapshot.files[2].as_mut().unwrap().length = 99;
        write_snapshot(&snapshot_path(&dir, &info_hash_hex), &snapshot).unwrap();
        assert!(matches!(
            startup_plan(&dir, &meta, &storage),
            Err(ResumeError::FileLengthMismatch { index: 2, .. })
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn changed_mtime_only_reverifies_overlapping_pieces() {
        let dir = temp_dir("changedmtime");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = storage_with_content(&meta, &dir, &data);
        let info_hash_hex = crate::hex::encode(&meta.info_hash);
        let snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: info_hash_hex.clone(),
            piece_count: 3,
            bitfield: vec![0xE0],
            files: current_fingerprints(&storage.file_paths()),
        };
        write_snapshot(&snapshot_path(&dir, &info_hash_hex), &snapshot).unwrap();

        wait_for_mtime_change(&dir, &["dir", "c.txt"]);
        let plan = startup_plan(&dir, &meta, &storage).unwrap();
        assert_eq!(plan.overlaps, vec![2], "only piece 2 overlaps c.txt");
        assert!(plan.trusted.get(0));
        assert!(plan.trusted.get(1));
        assert!(!plan.trusted.get(2));
        assert_eq!(plan.sample, vec![0, 1]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_missing_file_reverifies_every_piece_it_overlaps() {
        let dir = temp_dir("missingfile");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = storage_with_content(&meta, &dir, &data);
        let info_hash_hex = crate::hex::encode(&meta.info_hash);
        let snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: info_hash_hex.clone(),
            piece_count: 3,
            bitfield: vec![0xE0],
            files: current_fingerprints(&storage.file_paths()),
        };
        write_snapshot(&snapshot_path(&dir, &info_hash_hex), &snapshot).unwrap();
        std::fs::remove_file(dir.join("dir/sub/b.bin")).unwrap();
        let plan = startup_plan(&dir, &meta, &storage).unwrap();
        assert_eq!(plan.overlaps, vec![1], "b.bin holds piece 1 only");
        assert!(plan.trusted.get(0));
        assert!(!plan.trusted.get(1));
        assert!(plan.trusted.get(2));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sample_is_bounded_and_grows_with_the_verified_share() {
        assert!(sample_of(&[]).is_empty());
        let small: Vec<usize> = (0..5).collect();
        assert_eq!(
            sample_of(&small),
            small,
            "few verified pieces are all hashed"
        );
        let medium: Vec<usize> = (0..40).collect();
        assert_eq!(sample_of(&medium).len(), 8);
        let big: Vec<usize> = (0..1000).collect();
        assert_eq!(sample_of(&big).len(), 10);
        let huge: Vec<usize> = (0..100_000).collect();
        assert_eq!(sample_of(&huge).len(), SAMPLE_CAP);
        let chosen = sample_of(&big);
        assert!(chosen.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(chosen.iter().all(|index| big.contains(index)));
    }

    #[test]
    fn verify_pieces_catches_tampered_content() {
        let dir = temp_dir("tampered");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = storage_with_content(&meta, &dir, &data);
        let failed = verify_pieces(&storage, &meta.info.pieces, &[0, 1, 2]);
        assert!(failed.is_empty());
        let mut tampered = data.clone();
        tampered[6] ^= 0xFF;
        storage.write_piece(1, &tampered[4..8]).unwrap();
        let failed = verify_pieces(&storage, &meta.info.pieces, &[0, 1, 2]);
        assert_eq!(failed, vec![1]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn verified_length_sums_piece_sizes() {
        let dir = temp_dir("verifiedlength");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = storage_with_content(&meta, &dir, &data);
        let mut have = Bitfield::new(3);
        have.set(0).unwrap();
        have.set(2).unwrap();
        assert_eq!(
            verified_length(&storage, &have),
            4 + 4,
            "piece sizes, not file lengths"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn skip_first_file_storage(meta: &MetaInfo, dir: &std::path::Path) -> Storage {
        Storage::create(
            meta,
            dir,
            &[
                crate::engine::FilePriority::Skip,
                crate::engine::FilePriority::Normal,
                crate::engine::FilePriority::Normal,
            ],
        )
        .unwrap()
    }

    fn write_snapshot_for(
        dir: &std::path::Path,
        meta: &MetaInfo,
        storage: &Storage,
        have: &[usize],
    ) {
        let info_hash_hex = crate::hex::encode(&meta.info_hash);
        let mut bitfield = Bitfield::new(meta.info.pieces.len());
        for &index in have {
            bitfield.set(index).unwrap();
        }
        let snapshot = ResumeSnapshot {
            version: RESUME_FORMAT_VERSION,
            info_hash: info_hash_hex.clone(),
            piece_count: meta.info.pieces.len(),
            bitfield: bitfield.as_raw().to_vec(),
            files: current_fingerprints(&storage.file_paths()),
        };
        write_snapshot(&snapshot_path(dir, &info_hash_hex), &snapshot).unwrap();
    }

    #[test]
    fn skipped_file_that_was_never_created_still_resumes() {
        let dir = temp_dir("skipped-resume");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = skip_first_file_storage(&meta, &dir);
        assert!(
            !dir.join("dir/a.txt").exists(),
            "the skipped file was never created"
        );
        // Pieces 1 and 2 are verified; the skipped file's piece 0 is not.
        storage.write_piece(1, &data[4..8]).unwrap();
        storage.write_piece(2, &data[8..12]).unwrap();
        write_snapshot_for(&dir, &meta, &storage, &[1, 2]);

        let plan = startup_plan(&dir, &meta, &storage).unwrap();
        assert!(
            plan.overlaps.is_empty(),
            "a missing skipped file must not invalidate the resume state"
        );
        assert!(plan.trusted.get(1));
        assert!(plan.trusted.get(2));
        assert!(!plan.trusted.get(0));
        assert_eq!(plan.sample, vec![1, 2]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn skipped_file_created_for_boundary_pieces_is_fingerprint_checked() {
        let dir = temp_dir("boundary-fingerprint");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = skip_first_file_storage(&meta, &dir);
        // Piece 1 brings the skipped file into existence (boundary piece).
        storage.write_piece(1, &data[4..8]).unwrap();
        assert!(dir.join("dir/a.txt").exists());
        storage.write_piece(2, &data[8..12]).unwrap();
        write_snapshot_for(&dir, &meta, &storage, &[1, 2]);

        let plan = startup_plan(&dir, &meta, &storage).unwrap();
        assert!(plan.overlaps.is_empty());

        // If that created file is later removed, its pieces are re-verified.
        std::fs::remove_file(dir.join("dir/a.txt")).unwrap();
        let plan = startup_plan(&dir, &meta, &storage).unwrap();
        assert_eq!(plan.overlaps, vec![0, 1], "a.txt overlaps pieces 0 and 1");
        assert!(!plan.trusted.get(0));
        assert!(!plan.trusted.get(1));
        assert!(plan.trusted.get(2));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn missing_file_that_should_exist_forces_reverification() {
        let dir = temp_dir("should-exist");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        // The snapshot was written while a.txt was skipped (files[0] is None),
        // but the current priorities say a.txt should exist.
        let storage = skip_first_file_storage(&meta, &dir);
        storage.write_piece(1, &data[4..8]).unwrap();
        storage.write_piece(2, &data[8..12]).unwrap();
        write_snapshot_for(&dir, &meta, &storage, &[1, 2]);
        let normal_storage = Storage::create(
            &meta,
            &dir,
            &[
                crate::engine::FilePriority::Normal,
                crate::engine::FilePriority::Normal,
                crate::engine::FilePriority::Normal,
            ],
        )
        .unwrap();
        std::fs::remove_file(dir.join("dir/a.txt")).unwrap();
        let plan = startup_plan(&dir, &meta, &normal_storage).unwrap();
        assert_eq!(
            plan.overlaps,
            vec![0, 1],
            "a file that should exist but is missing still forces re-verification"
        );
        assert!(!plan.trusted.get(0));
        assert!(!plan.trusted.get(1));
        assert!(plan.trusted.get(2));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn skipped_file_that_appeared_since_the_snapshot_is_reverified() {
        let dir = temp_dir("appeared");
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let storage = skip_first_file_storage(&meta, &dir);
        storage.write_piece(1, &data[4..8]).unwrap();
        storage.write_piece(2, &data[8..12]).unwrap();
        write_snapshot_for(&dir, &meta, &storage, &[1, 2]);
        std::fs::write(dir.join("dir/a.txt"), b"xyzzy").unwrap();
        let plan = startup_plan(&dir, &meta, &storage).unwrap();
        assert_eq!(plan.overlaps, vec![0, 1]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
