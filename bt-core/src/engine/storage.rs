use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use sha1::{Digest, Sha1};

use crate::error::StorageError;
use crate::metainfo::{Content, MetaInfo};
use crate::peer::Bitfield;

pub struct FileSlot {
    pub offset: u64,
    pub length: u64,
    file: Mutex<File>,
}

pub struct Storage {
    slots: Vec<FileSlot>,
    piece_length: u32,
    total_length: u64,
}

impl Storage {
    pub fn create(meta: &MetaInfo, output_dir: &Path) -> Result<Storage, StorageError> {
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
        for (segments, length) in entries {
            let mut full = output_dir.to_path_buf();
            for segment in &segments {
                full.push(segment);
            }
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
            if !existed && length > 0 {
                file.set_len(length)?;
            }
            slots.push(FileSlot {
                offset,
                length,
                file: Mutex::new(file),
            });
            offset += length;
        }
        Ok(Storage {
            slots,
            piece_length: meta.info.piece_length,
            total_length: offset,
        })
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

    pub fn read_piece(&self, index: usize, buf: &mut [u8]) -> Result<(), StorageError> {
        if buf.len() != self.piece_size(index) {
            return Err(StorageError::PieceOutOfRange(index));
        }
        let mut cursor = 0usize;
        for (slot_index, file_offset, length) in self.spans(index) {
            let slot = &self.slots[slot_index];
            let mut guard = lock_file(&slot.file);
            guard.seek(SeekFrom::Start(file_offset))?;
            let mut filled = 0usize;
            while filled < length {
                match guard.read(&mut buf[cursor + filled..cursor + length])? {
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
            guard.seek(SeekFrom::Start(file_offset))?;
            guard.write_all(&data[cursor..cursor + length])?;
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

fn lock_file(file: &Mutex<File>) -> MutexGuard<'_, File> {
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
        let storage = Storage::create(&meta, &dir).unwrap();
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
    fn recheck_marks_valid_pieces() {
        let data = content();
        let meta = multi_file_meta(&hashes_of(&data));
        let dir = temp_dir("recheck");
        let storage = Storage::create(&meta, &dir).unwrap();
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
        let storage = Storage::create(&meta, &dir).unwrap();
        let metadata = fs::metadata(dir.join("dir/sub/b.bin")).unwrap();
        assert_eq!(metadata.len(), 3);
        let mut buffer = [0u8; 4];
        storage.read_piece(2, &mut buffer).unwrap();
        assert_eq!(buffer, [0, 0, 0, 0]);
        fs::remove_dir_all(dir).unwrap();
    }
}
