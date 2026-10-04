use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use sha1::{Digest, Sha1};

use crate::engine::FilePriority;
use crate::error::StorageError;
use crate::metainfo::{Content, MetaInfo};
use crate::peer::Bitfield;

pub struct FileSlot {
    pub offset: u64,
    pub length: u64,
    path: PathBuf,
    file: Mutex<Option<File>>,
    skip: AtomicBool,
    dirty: AtomicBool,
}

pub struct Storage {
    slots: Vec<FileSlot>,
    piece_length: u32,
    total_length: u64,
    priorities: Vec<FilePriority>,
}

impl Storage {
    pub fn create(
        meta: &MetaInfo,
        output_dir: &Path,
        priorities: &[FilePriority],
    ) -> Result<Storage, StorageError> {
        std::fs::create_dir_all(output_dir)?;
        let entries: Vec<(Vec<String>, u64)> = match &meta.info.content {
            Content::Single { length } => vec![(vec![meta.info.name.clone()], *length)],
            Content::Multi { files } => files
                .iter()
                .map(|file| {
                    let mut path = vec![meta.info.name.clone()];
                    path.extend(file.path.clone());
                    (path, file.length)
                })
                .collect(),
        };
        let mut slots = Vec::new();
        let mut offset = 0u64;
        for (index, (segments, length)) in entries.iter().enumerate() {
            let skip = priorities
                .get(index)
                .copied()
                .unwrap_or(FilePriority::Normal)
                .is_skip();
            let mut full = output_dir.to_path_buf();
            for segment in segments {
                full.push(segment);
            }
            if skip {
                // Skipped files that no boundary piece touches are never
                // created or preallocated; the file appears on demand the
                // first time a boundary piece is written.
                slots.push(FileSlot {
                    offset,
                    length: *length,
                    path: full,
                    file: Mutex::new(None),
                    skip: AtomicBool::new(true),
                    dirty: AtomicBool::new(false),
                });
            } else {
                if let Some(parent) = full.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let existed = full.exists();
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&full)?;
                if !existed && *length > 0 {
                    file.set_len(*length)?;
                }
                slots.push(FileSlot {
                    offset,
                    length: *length,
                    path: full,
                    file: Mutex::new(Some(file)),
                    skip: AtomicBool::new(false),
                    dirty: AtomicBool::new(false),
                });
            }
            offset += *length;
        }
        Ok(Storage {
            slots,
            piece_length: meta.info.piece_length,
            total_length: offset,
            priorities: priorities.to_vec(),
        })
    }

    pub fn file_paths(&self) -> Vec<PathBuf> {
        self.slots.iter().map(|slot| slot.path.clone()).collect()
    }

    pub fn file_lengths(&self) -> Vec<u64> {
        self.slots.iter().map(|slot| slot.length).collect()
    }

    /// Priorities as of creation time; startup resume planning reads these.
    pub fn priorities(&self) -> &[FilePriority] {
        &self.priorities
    }

    /// Updates which files are skipped at runtime, so that boundary writes
    /// create the right files on demand. Only the skip flags change; the
    /// creation-time priorities stay untouched.
    pub fn set_skipped(&self, skipped: &[bool]) {
        for (slot, skip) in self.slots.iter().zip(skipped.iter()) {
            slot.skip.store(*skip, Ordering::SeqCst);
        }
    }

    /// Eagerly creates and preallocates the given files if they do not exist
    /// yet, so fingerprint-based resume can trust them.
    pub fn ensure_files_exist(&self, indices: &[usize]) -> Result<(), StorageError> {
        for index in indices {
            if *index >= self.slots.len() {
                continue;
            }
            let slot = &self.slots[*index];
            let mut guard = lock_file(&slot.file);
            if guard.is_some() {
                continue;
            }
            *guard = Some(self.open_slot(*index)?);
        }
        Ok(())
    }

    fn open_slot(&self, slot_index: usize) -> Result<File, StorageError> {
        let slot = &self.slots[slot_index];
        if let Some(parent) = slot.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&slot.path)?;
        if slot.length > 0 {
            file.set_len(slot.length)?;
        }
        Ok(file)
    }

    pub fn pieces_overlapping_file(&self, file_index: usize) -> Vec<usize> {
        let Some(slot) = self.slots.get(file_index) else {
            return Vec::new();
        };
        if slot.length == 0 {
            return Vec::new();
        }
        let first = (slot.offset / self.piece_length as u64) as usize;
        let last = ((slot.offset + slot.length - 1) / self.piece_length as u64) as usize;
        (first..=last).collect()
    }

    /// Files a piece covers and how many of its bytes fall into each of them.
    pub fn piece_file_spans(&self, index: usize) -> Vec<(usize, usize)> {
        self.spans(index)
            .into_iter()
            .map(|(slot_index, _, length)| (slot_index, length))
            .collect()
    }

    pub fn sync_dirty(&self) -> Result<(), StorageError> {
        for slot in &self.slots {
            if !slot.dirty.swap(false, Ordering::SeqCst) {
                continue;
            }
            let result = {
                let mut guard = lock_file(&slot.file);
                match guard.as_mut() {
                    Some(file) => file.sync_data(),
                    None => Ok(()),
                }
            };
            if let Err(err) = result {
                slot.dirty.store(true, Ordering::SeqCst);
                return Err(err.into());
            }
        }
        Ok(())
    }

    pub fn piece_length(&self) -> u32 {
        self.piece_length
    }

    pub fn total_length(&self) -> u64 {
        self.total_length
    }

    pub fn piece_size(&self, index: usize) -> usize {
        let start = index as u64 * self.piece_length as u64;
        (self.total_length - start).min(self.piece_length as u64) as usize
    }

    fn spans(&self, index: usize) -> Vec<(usize, u64, usize)> {
        let start = index as u64 * self.piece_length as u64;
        let end = start + self.piece_size(index) as u64;
        self.spans_range(start, end)
    }

    fn spans_range(&self, start: u64, end: u64) -> Vec<(usize, u64, usize)> {
        let mut result = Vec::new();
        for (slot_index, slot) in self.slots.iter().enumerate() {
            let overlap_start = start.max(slot.offset);
            let overlap_end = end.min(slot.offset + slot.length);
            if overlap_start < overlap_end {
                result.push((
                    slot_index,
                    overlap_start - slot.offset,
                    (overlap_end - overlap_start) as usize,
                ));
            }
        }
        result
    }

    pub fn read_block(
        &self,
        index: usize,
        begin: usize,
        length: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let piece_start = (index as u64)
            .checked_mul(self.piece_length as u64)
            .ok_or(StorageError::PieceOutOfRange(index))?;
        if length == 0 || piece_start >= self.total_length {
            return Err(StorageError::PieceOutOfRange(index));
        }
        let piece_size = (self.total_length - piece_start).min(self.piece_length as u64) as usize;
        if begin >= piece_size || length > piece_size - begin {
            return Err(StorageError::PieceOutOfRange(index));
        }
        let mut buffer = vec![0u8; length];
        let mut cursor = 0usize;
        let start = piece_start + begin as u64;
        for (slot_index, file_offset, span_length) in self.spans_range(start, start + length as u64)
        {
            let slot = &self.slots[slot_index];
            let mut guard = lock_file(&slot.file);
            let Some(file) = guard.as_mut() else {
                // A skipped file that was never created reads as zeros.
                cursor += span_length;
                continue;
            };
            file.seek(SeekFrom::Start(file_offset))?;
            let mut filled = 0usize;
            while filled < span_length {
                match file.read(&mut buffer[cursor + filled..cursor + span_length])? {
                    0 => break,
                    read => filled += read,
                }
            }
            cursor += span_length;
        }
        Ok(buffer)
    }

    pub fn read_piece(&self, index: usize, buf: &mut [u8]) -> Result<(), StorageError> {
        if buf.len() != self.piece_size(index) {
            return Err(StorageError::PieceOutOfRange(index));
        }
        let mut cursor = 0usize;
        for (slot_index, file_offset, length) in self.spans(index) {
            let slot = &self.slots[slot_index];
            let mut guard = lock_file(&slot.file);
            let Some(file) = guard.as_mut() else {
                cursor += length;
                continue;
            };
            file.seek(SeekFrom::Start(file_offset))?;
            let mut filled = 0usize;
            while filled < length {
                match file.read(&mut buf[cursor + filled..cursor + length])? {
                    0 => {
                        buf[cursor + filled..cursor + length].fill(0);
                        break;
                    }
                    read => filled += read,
                }
            }
            cursor += length;
        }
        Ok(())
    }

    pub fn write_piece(&self, index: usize, data: &[u8]) -> Result<(), StorageError> {
        if data.len() != self.piece_size(index) {
            return Err(StorageError::PieceOutOfRange(index));
        }
        let mut cursor = 0usize;
        for (slot_index, file_offset, length) in self.spans(index) {
            let slot = &self.slots[slot_index];
            let mut guard = lock_file(&slot.file);
            let file = match guard.as_mut() {
                Some(file) => file,
                None => {
                    // A boundary piece brings a skipped file into existence
                    // so the whole piece can be stored and served later.
                    guard.insert(self.open_slot(slot_index)?)
                }
            };
            file.seek(SeekFrom::Start(file_offset))?;
            file.write_all(&data[cursor..cursor + length])?;
            slot.dirty.store(true, Ordering::SeqCst);
            cursor += length;
        }
        Ok(())
    }
}

pub fn delete_torrent_files(meta: &MetaInfo, output_dir: &Path) -> Result<(), StorageError> {
    let relatives: Vec<Vec<String>> = match &meta.info.content {
        Content::Single { length: _ } => vec![vec![meta.info.name.clone()]],
        Content::Multi { files } => files
            .iter()
            .map(|file| {
                let mut path = vec![meta.info.name.clone()];
                path.extend(file.path.clone());
                path
            })
            .collect(),
    };
    let mut dirs: Vec<PathBuf> = Vec::new();
    for segments in &relatives {
        for segment in segments {
            crate::metainfo::validate_path_component(segment, "path")
                .map_err(|_| StorageError::UnsafePath)?;
        }
        let mut full = output_dir.to_path_buf();
        for segment in segments {
            full.push(segment);
        }
        if full.is_file() {
            fs::remove_file(&full)?;
        }
        let mut parent = full.parent();
        while let Some(current) = parent {
            if current == output_dir || !current.starts_with(output_dir) {
                break;
            }
            dirs.push(current.to_path_buf());
            parent = current.parent();
        }
    }
    dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    dirs.dedup();
    for dir in dirs {
        match fs::remove_dir(&dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

fn lock_file(file: &Mutex<Option<File>>) -> MutexGuard<'_, Option<File>> {
    match file.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub fn recheck(
    storage: &Storage,
    hashes: &[[u8; 20]],
    mut on_progress: impl FnMut(usize),
) -> Bitfield {
    let mut have = Bitfield::new(hashes.len());
    let mut buffer = vec![0u8; storage.piece_length() as usize];
    for (index, expected) in hashes.iter().enumerate() {
        let size = storage.piece_size(index);
        if storage.read_piece(index, &mut buffer[..size]).is_ok() {
            let digest: [u8; 20] = Sha1::digest(&buffer[..size]).into();
            if digest == *expected {
                let _ = have.set(index);
            }
        }
        on_progress(index + 1);
    }
    have
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    const PIECE_LENGTH: u32 = 4;

    fn priorities_of(priorities: &[FilePriority]) -> Vec<FilePriority> {
        priorities.to_vec()
    }

    fn all_normal(count: usize) -> Vec<FilePriority> {
        vec![FilePriority::Normal; count]
    }

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
        let dir = std::env::temp_dir().join(format!("bt-core-{}-{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn content() -> Vec<u8> {
        (1..=12).collect()
    }

    fn hashes_of(content: &[u8]) -> Vec<[u8; 20]> {
        content
            .chunks(PIECE_LENGTH as usize)
            .map(|chunk| {
                let digest: [u8; 20] = Sha1::digest(chunk).into();
                digest
            })
            .collect()
    }

    #[test]
    fn maps_pieces_across_file_boundaries() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("mapping");
        let storage = Storage::create(&meta, &dir, &all_normal(3)).unwrap();
        assert_eq!(meta.info.pieces.len(), 3);
        storage.write_piece(0, &[1, 2, 3, 4]).unwrap();
        storage.write_piece(1, &[5, 6, 7, 8]).unwrap();
        storage.write_piece(2, &[9, 10, 11, 12]).unwrap();
        assert_eq!(fs::read(dir.join("dir/a.txt")).unwrap(), [1, 2, 3, 4, 5]);
        assert_eq!(fs::read(dir.join("dir/sub/b.bin")).unwrap(), [6, 7, 8]);
        assert_eq!(fs::read(dir.join("dir/c.txt")).unwrap(), [9, 10, 11, 12]);
        let mut buffer = [0u8; 4];
        storage.read_piece(1, &mut buffer).unwrap();
        assert_eq!(buffer, [5, 6, 7, 8]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reads_blocks_across_file_boundaries() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("readblock");
        let storage = Storage::create(&meta, &dir, &all_normal(3)).unwrap();
        storage.write_piece(0, &[1, 2, 3, 4]).unwrap();
        storage.write_piece(1, &[5, 6, 7, 8]).unwrap();
        storage.write_piece(2, &[9, 10, 11, 12]).unwrap();
        assert_eq!(storage.read_block(0, 1, 3).unwrap(), [2, 3, 4]);
        assert_eq!(storage.read_block(1, 0, 3).unwrap(), [5, 6, 7]);
        assert_eq!(storage.read_block(1, 2, 2).unwrap(), [7, 8]);
        assert_eq!(storage.read_block(2, 0, 4).unwrap(), [9, 10, 11, 12]);
        for (index, begin, length) in [(2, 0, 5), (3, 0, 1), (0, 0, 0), (0, 4, 1)] {
            assert!(matches!(
                storage.read_block(index, begin, length),
                Err(StorageError::PieceOutOfRange(_))
            ));
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recheck_marks_valid_pieces() {
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let dir = temp_dir("recheck");
        let storage = Storage::create(&meta, &dir, &all_normal(3)).unwrap();
        fs::write(dir.join("dir/a.txt"), &data[0..5]).unwrap();
        fs::write(dir.join("dir/sub/b.bin"), &data[5..8]).unwrap();
        fs::write(dir.join("dir/c.txt"), &data[8..12]).unwrap();
        let have = recheck(&storage, &meta.info.pieces, |_| {});
        assert_eq!(have.count(), 3);
        let mut corrupted = data[0..5].to_vec();
        corrupted[4] = 0xFF;
        fs::write(dir.join("dir/a.txt"), corrupted).unwrap();
        let have = recheck(&storage, &meta.info.pieces, |_| {});
        assert!(have.get(0));
        assert!(!have.get(1));
        assert!(have.get(2));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn preallocates_missing_files() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("prealloc");
        let storage = Storage::create(&meta, &dir, &all_normal(3)).unwrap();
        let metadata = fs::metadata(dir.join("dir/sub/b.bin")).unwrap();
        assert_eq!(metadata.len(), 3);
        let mut buffer = [0u8; 4];
        storage.read_piece(2, &mut buffer).unwrap();
        assert_eq!(buffer, [0, 0, 0, 0]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn skipped_files_are_never_created_or_preallocated() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("skipped-none");
        let storage = Storage::create(
            &meta,
            &dir,
            &priorities_of(&[
                FilePriority::Normal,
                FilePriority::Skip,
                FilePriority::Normal,
            ]),
        )
        .unwrap();
        assert!(dir.join("dir/a.txt").exists());
        assert!(!dir.join("dir/sub/b.bin").exists());
        assert!(dir.join("dir/c.txt").exists());
        // A never-created skipped file reads as zeros, never as an error.
        let mut buffer = [0u8; 4];
        storage.read_piece(1, &mut buffer).unwrap();
        assert_eq!(buffer, [0, 0, 0, 0]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn boundary_piece_writes_every_byte_and_creates_the_skipped_file() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("boundary-write");
        let storage = Storage::create(
            &meta,
            &dir,
            &priorities_of(&[
                FilePriority::Normal,
                FilePriority::Skip,
                FilePriority::Normal,
            ]),
        )
        .unwrap();
        assert!(!dir.join("dir/sub/b.bin").exists());
        // Piece 1 spans a.txt's tail and all of b.bin.
        storage.write_piece(1, &[5, 6, 7, 8]).unwrap();
        assert_eq!(fs::read(dir.join("dir/sub/b.bin")).unwrap(), [6, 7, 8]);
        assert_eq!(
            fs::read(dir.join("dir/a.txt")).unwrap(),
            [0, 0, 0, 0, 5],
            "the whole piece is written, including the wanted file's bytes"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn on_demand_skipped_file_gets_the_full_declared_length() {
        // Two files: "head" (2 bytes) and "tail" (6 bytes), piece length 4.
        // Piece 0 covers head + tail[0..2]; piece 1 covers tail[2..6] only.
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d4:infod5:filesl");
        raw.extend_from_slice(b"d6:lengthi2e4:pathl4:headee");
        raw.extend_from_slice(b"d6:lengthi6e4:pathl4:tailee");
        raw.extend_from_slice(b"e4:name1:t12:piece lengthi4e6:pieces");
        let data: Vec<u8> = (1..=8).collect();
        let hashes = hashes_of(&data);
        raw.extend_from_slice((hashes.len() * 20).to_string().as_bytes());
        raw.push(b':');
        for hash in &hashes {
            raw.extend_from_slice(hash);
        }
        raw.extend_from_slice(b"ee");
        let meta = MetaInfo::from_bytes(&raw).unwrap();
        let dir = temp_dir("sparse-length");
        let storage = Storage::create(
            &meta,
            &dir,
            &priorities_of(&[FilePriority::Normal, FilePriority::Skip]),
        )
        .unwrap();
        assert!(!dir.join("t/tail").exists());
        storage.write_piece(0, &data[0..4]).unwrap();
        let written = fs::read(dir.join("t/tail")).unwrap();
        assert_eq!(written.len(), 6, "created with the full declared length");
        assert_eq!(&written[0..2], &data[2..4]);
        assert_eq!(&written[2..6], &[0, 0, 0, 0], "untouched tail stays sparse");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn set_skip_flags_and_ensure_files_exist() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("ensure");
        let storage = Storage::create(
            &meta,
            &dir,
            &priorities_of(&[
                FilePriority::Normal,
                FilePriority::Skip,
                FilePriority::Normal,
            ]),
        )
        .unwrap();
        assert!(!dir.join("dir/sub/b.bin").exists());
        storage.set_skipped(&[false, false, false]);
        storage.ensure_files_exist(&[1]).unwrap();
        assert_eq!(fs::metadata(dir.join("dir/sub/b.bin")).unwrap().len(), 3);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn piece_file_spans_report_byte_shares() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("spans");
        let storage = Storage::create(&meta, &dir, &all_normal(3)).unwrap();
        // Piece 1 (bytes 4..8) covers a.txt's last byte and all of b.bin.
        assert_eq!(storage.piece_file_spans(1), vec![(0, 1), (1, 3)]);
        assert_eq!(storage.piece_file_spans(0), vec![(0, 4)]);
        assert_eq!(storage.piece_file_spans(2), vec![(2, 4)]);
        fs::remove_dir_all(dir).unwrap();
    }
}
