use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use sha1::{Digest, Sha1};

use crate::engine::FilePriority;
use crate::error::{classify_dir_io, classify_file_io, StorageError};
use crate::metainfo::{Content, MetaInfo};
use crate::paths;
use crate::peer::Bitfield;

pub struct FileSlot {
    pub offset: u64,
    pub length: u64,
    path: PathBuf,
    file: Mutex<Option<File>>,
    skip: AtomicBool,
    dirty: AtomicBool,
}

/// The filesystem boundary behind [`Storage`]. Production uses [`RealFs`];
/// tests install a [`FaultyFs`] to inject disk errors.
pub trait FileBackend: Send + Sync + 'static {
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()>;
    fn exists(&self, path: &Path) -> bool;
    fn open_rw(&self, path: &Path) -> std::io::Result<File>;
    fn set_len(&self, file: &File, len: u64) -> std::io::Result<()>;
    /// One read at `offset`; short reads are allowed, the caller loops.
    fn read_at(&self, file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize>;
    fn write_all_at(&self, file: &File, offset: u64, data: &[u8]) -> std::io::Result<()>;
    fn sync_data(&self, file: &File) -> std::io::Result<()>;
    fn available_space(&self, path: &Path) -> std::io::Result<u64>;
}

#[derive(Debug, Default)]
pub struct RealFs;

impl FileBackend for RealFs {
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn open_rw(&self, path: &Path) -> std::io::Result<File> {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
    }

    fn set_len(&self, file: &File, len: u64) -> std::io::Result<()> {
        file.set_len(len)
    }

    fn read_at(&self, file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }

    fn write_all_at(&self, file: &File, offset: u64, data: &[u8]) -> std::io::Result<()> {
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }

    fn sync_data(&self, file: &File) -> std::io::Result<()> {
        file.sync_data()
    }

    fn available_space(&self, path: &Path) -> std::io::Result<u64> {
        fs4::available_space(path)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultOp {
    CreateDirAll,
    Open,
    SetLen,
    Read,
    Write,
    SyncData,
}

/// A filesystem double that delegates to the real filesystem but fails the
/// configured operations with the configured io error kinds, and can report
/// an injected amount of available space.
#[derive(Debug, Default)]
pub struct FaultyFs {
    faults: Mutex<Vec<(FaultOp, u32, std::io::ErrorKind)>>,
    available_space: Mutex<Option<u64>>,
    real: RealFs,
}

impl FaultyFs {
    pub fn new() -> FaultyFs {
        FaultyFs::default()
    }

    /// Fails the next `times` operations of `op` with `kind`.
    pub fn fail(&self, op: FaultOp, times: u32, kind: std::io::ErrorKind) {
        self.faults
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((op, times, kind));
    }

    pub fn clear_faults(&self) {
        self.faults
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }

    pub fn set_available_space(&self, bytes: Option<u64>) {
        *self
            .available_space
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = bytes;
    }

    fn take_fault(&self, op: FaultOp) -> Option<std::io::ErrorKind> {
        let mut faults = self.faults.lock().unwrap_or_else(|p| p.into_inner());
        for entry in faults.iter_mut() {
            if entry.0 == op && entry.1 > 0 {
                entry.1 -= 1;
                return Some(entry.2);
            }
        }
        None
    }
}

impl FileBackend for FaultyFs {
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        if let Some(kind) = self.take_fault(FaultOp::CreateDirAll) {
            return Err(std::io::Error::from(kind));
        }
        self.real.create_dir_all(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.real.exists(path)
    }

    fn open_rw(&self, path: &Path) -> std::io::Result<File> {
        if let Some(kind) = self.take_fault(FaultOp::Open) {
            return Err(std::io::Error::from(kind));
        }
        self.real.open_rw(path)
    }

    fn set_len(&self, file: &File, len: u64) -> std::io::Result<()> {
        if let Some(kind) = self.take_fault(FaultOp::SetLen) {
            return Err(std::io::Error::from(kind));
        }
        self.real.set_len(file, len)
    }

    fn read_at(&self, file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(kind) = self.take_fault(FaultOp::Read) {
            return Err(std::io::Error::from(kind));
        }
        self.real.read_at(file, offset, buf)
    }

    fn write_all_at(&self, file: &File, offset: u64, data: &[u8]) -> std::io::Result<()> {
        if let Some(kind) = self.take_fault(FaultOp::Write) {
            return Err(std::io::Error::from(kind));
        }
        self.real.write_all_at(file, offset, data)
    }

    fn sync_data(&self, file: &File) -> std::io::Result<()> {
        if let Some(kind) = self.take_fault(FaultOp::SyncData) {
            return Err(std::io::Error::from(kind));
        }
        self.real.sync_data(file)
    }

    fn available_space(&self, path: &Path) -> std::io::Result<u64> {
        let injected = *self
            .available_space
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        match injected {
            Some(bytes) => Ok(bytes),
            None => self.real.available_space(path),
        }
    }
}

pub struct Storage {
    slots: Vec<FileSlot>,
    piece_length: u32,
    total_length: u64,
    priorities: Vec<FilePriority>,
    fs: Arc<dyn FileBackend>,
    closed: AtomicBool,
}

impl Storage {
    pub fn create(
        meta: &MetaInfo,
        output_dir: &Path,
        priorities: &[FilePriority],
    ) -> Result<Storage, StorageError> {
        Storage::create_with_fs(meta, output_dir, priorities, Arc::new(RealFs))
    }

    pub fn create_with_fs(
        meta: &MetaInfo,
        output_dir: &Path,
        priorities: &[FilePriority],
        fs: Arc<dyn FileBackend>,
    ) -> Result<Storage, StorageError> {
        if fs.exists(output_dir) && !output_dir.is_dir() {
            return Err(StorageError::OutputDirMissing {
                path: output_dir.to_path_buf(),
            });
        }
        fs.create_dir_all(&paths::prepare_file_path(output_dir))
            .map_err(|err| classify_dir_io(output_dir, err))?;
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
                    fs.create_dir_all(&paths::prepare_file_path(parent))
                        .map_err(|err| classify_dir_io(parent, err))?;
                }
                let existed = fs.exists(&full);
                let file = fs
                    .open_rw(&paths::prepare_file_path(&full))
                    .map_err(|err| classify_file_io(&full, err))?;
                if !existed && *length > 0 {
                    fs.set_len(&file, *length)
                        .map_err(|err| classify_file_io(&full, err))?;
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
            fs,
            closed: AtomicBool::new(false),
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

    /// Recreates missing parent directories of files that will be written.
    /// Never deletes anything; used when a download retries after a fault.
    pub fn recreate_directories(&self) -> Result<(), StorageError> {
        for slot in &self.slots {
            if slot.skip.load(Ordering::SeqCst) {
                continue;
            }
            if let Some(parent) = slot.path.parent() {
                self.fs
                    .create_dir_all(&paths::prepare_file_path(parent))
                    .map_err(|err| classify_dir_io(parent, err))?;
            }
        }
        Ok(())
    }

    fn open_slot(&self, slot_index: usize) -> Result<File, StorageError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(StorageError::Closed);
        }
        let slot = &self.slots[slot_index];
        if let Some(parent) = slot.path.parent() {
            self.fs
                .create_dir_all(&paths::prepare_file_path(parent))
                .map_err(|err| classify_dir_io(parent, err))?;
        }
        let file = self
            .fs
            .open_rw(&paths::prepare_file_path(&slot.path))
            .map_err(|err| classify_file_io(&slot.path, err))?;
        if slot.length > 0 {
            self.fs
                .set_len(&file, slot.length)
                .map_err(|err| classify_file_io(&slot.path, err))?;
        }
        Ok(file)
    }

    /// Closes every open file handle and refuses later reopens. Called when
    /// the owning torrent is being removed, so its content can be deleted
    /// with no handle left writing to it. Waits out any in-flight write on a
    /// slot by taking its file under the slot's lock.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        for slot in &self.slots {
            lock_file(&slot.file).take();
        }
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
                    Some(file) => self.fs.sync_data(file),
                    None => Ok(()),
                }
            };
            if let Err(err) = result {
                slot.dirty.store(true, Ordering::SeqCst);
                return Err(classify_file_io(&slot.path, err));
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
            let mut filled = 0usize;
            while filled < span_length {
                let read = self
                    .fs
                    .read_at(
                        file,
                        file_offset + filled as u64,
                        &mut buffer[cursor + filled..cursor + span_length],
                    )
                    .map_err(|err| classify_file_io(&slot.path, err))?;
                if read == 0 {
                    break;
                }
                filled += read;
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
            let mut filled = 0usize;
            while filled < length {
                let read = self
                    .fs
                    .read_at(
                        file,
                        file_offset + filled as u64,
                        &mut buf[cursor + filled..cursor + length],
                    )
                    .map_err(|err| classify_file_io(&slot.path, err))?;
                if read == 0 {
                    buf[cursor + filled..cursor + length].fill(0);
                    break;
                }
                filled += read;
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
            self.fs
                .write_all_at(file, file_offset, &data[cursor..cursor + length])
                .map_err(|err| classify_file_io(&slot.path, err))?;
            slot.dirty.store(true, Ordering::SeqCst);
            cursor += length;
        }
        Ok(())
    }

    /// Bytes still to be written for the non-skipped files: their declared
    /// lengths minus what already exists on disk.
    pub fn bytes_still_needed(
        meta: &MetaInfo,
        output_dir: &Path,
        priorities: &[FilePriority],
    ) -> u64 {
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
        let mut needed = 0u64;
        for (index, (segments, length)) in entries.iter().enumerate() {
            if priorities
                .get(index)
                .copied()
                .unwrap_or(FilePriority::Normal)
                .is_skip()
            {
                continue;
            }
            let mut full = output_dir.to_path_buf();
            for segment in segments {
                full.push(segment);
            }
            let existing = std::fs::metadata(&full).map(|meta| meta.len()).unwrap_or(0);
            needed += length.saturating_sub(existing);
        }
        needed
    }

    /// Free space check for the wanted bytes of a torrent, honoring file
    /// priorities: skipped files do not count.
    pub fn check_free_space(
        meta: &MetaInfo,
        output_dir: &Path,
        priorities: &[FilePriority],
        fs: &dyn FileBackend,
    ) -> Result<(), StorageError> {
        let needed = Storage::bytes_still_needed(meta, output_dir, priorities);
        let available = fs
            .available_space(&paths::prepare_file_path(output_dir))
            .map_err(|err| classify_dir_io(output_dir, err))?;
        if available < needed {
            return Err(StorageError::NotEnoughSpace {
                path: output_dir.to_path_buf(),
                needed,
                available,
            });
        }
        Ok(())
    }
}

const DELETE_ATTEMPTS: usize = 8;
const DELETE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

fn remove_file_exact(path: &Path) -> std::io::Result<()> {
    std::fs::remove_file(path)
}

fn remove_dir_exact(path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir(path)
}

/// Removes a file or directory, retrying briefly when something still holds
/// the name (a straggler handle, an antivirus scan). `tolerated` kinds are
/// success from the caller's point of view (already gone, folder shared).
fn remove_with_retry(
    path: &Path,
    remove: fn(&Path) -> std::io::Result<()>,
    tolerated: &[std::io::ErrorKind],
) -> std::io::Result<()> {
    for attempt in 0..DELETE_ATTEMPTS {
        match remove(path) {
            Ok(()) => return Ok(()),
            Err(err) if tolerated.contains(&err.kind()) => return Ok(()),
            Err(_) if attempt + 1 < DELETE_ATTEMPTS => {
                std::thread::sleep(DELETE_RETRY_DELAY);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub fn delete_torrent_files(meta: &MetaInfo, output_dir: &Path) -> Result<(), StorageError> {
    let prepared_output = paths::prepare_file_path(output_dir);
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
        let full = paths::prepare_file_path(&full);
        remove_with_retry(&full, remove_file_exact, &[std::io::ErrorKind::NotFound])
            .map_err(|err| classify_file_io(&full, err))?;
        let mut parent = full.parent();
        while let Some(current) = parent {
            if current == prepared_output || !current.starts_with(&prepared_output) {
                break;
            }
            dirs.push(current.to_path_buf());
            parent = current.parent();
        }
    }
    dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    dirs.dedup();
    for dir in dirs {
        remove_with_retry(
            &dir,
            remove_dir_exact,
            &[
                std::io::ErrorKind::NotFound,
                std::io::ErrorKind::DirectoryNotEmpty,
            ],
        )
        .map_err(|err| classify_dir_io(&dir, err))?;
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
    mut on_progress: impl FnMut(usize) -> bool,
) -> Bitfield {
    let mut have = Bitfield::new(hashes.len());
    let mut buffer = vec![0u8; storage.piece_length() as usize];
    for (index, expected) in hashes.iter().enumerate() {
        // Returning false from the callback cancels the recheck mid-flight
        // (the torrent is being discarded; there is no point hashing on).
        if !on_progress(index) {
            break;
        }
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
    fn delete_torrent_files_removes_files_and_empty_folders() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("delete-files");
        let storage = Storage::create(&meta, &dir, &all_normal(3)).unwrap();
        storage.write_piece(0, &[1, 2, 3, 4]).unwrap();
        storage.write_piece(1, &[5, 6, 7, 8]).unwrap();
        storage.write_piece(2, &[9, 10, 11, 12]).unwrap();
        assert!(dir.join("dir/a.txt").is_file());
        assert!(dir.join("dir/sub/b.bin").is_file());

        // An unrelated file in the same output dir must survive.
        let unrelated = dir.join("keep.txt");
        fs::write(&unrelated, b"keep me").unwrap();

        storage.close();
        delete_torrent_files(&meta, &dir).unwrap();

        assert!(!dir.join("dir").exists());
        assert!(unrelated.is_file());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn closed_storage_refuses_to_reopen_skipped_files() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("closed");
        let storage = Storage::create(
            &meta,
            &dir,
            &[
                FilePriority::Normal,
                FilePriority::Skip,
                FilePriority::Normal,
            ],
        )
        .unwrap();
        storage.close();
        // Piece 1 spans a.txt and the skipped sub/b.bin; bringing the skipped
        // file back would have to reopen it, which must be refused now.
        assert!(matches!(
            storage.write_piece(1, &[5, 6, 7, 8]),
            Err(StorageError::Closed)
        ));
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
        let have = recheck(&storage, &meta.info.pieces, |_| true);
        assert_eq!(have.count(), 3);
        let mut corrupted = data[0..5].to_vec();
        corrupted[4] = 0xFF;
        fs::write(dir.join("dir/a.txt"), corrupted).unwrap();
        let have = recheck(&storage, &meta.info.pieces, |_| true);
        assert!(have.get(0));
        assert!(!have.get(1));
        assert!(have.get(2));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recheck_can_be_cancelled_mid_flight() {
        let meta = multi_file_meta(&hashes_of(&content()));
        let dir = temp_dir("recheck-cancel");
        let storage = Storage::create(&meta, &dir, &all_normal(3)).unwrap();
        fs::write(dir.join("dir/a.txt"), &content()[0..5]).unwrap();
        fs::write(dir.join("dir/sub/b.bin"), &content()[5..8]).unwrap();
        fs::write(dir.join("dir/c.txt"), &content()[8..12]).unwrap();
        let mut calls = 0;
        let have = recheck(&storage, &meta.info.pieces, |done| {
            calls += 1;
            done < 1
        });
        // Cancelled right after the first piece: only that piece was hashed.
        assert_eq!(have.count(), 1);
        assert_eq!(calls, 3);
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
