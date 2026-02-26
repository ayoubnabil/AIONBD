use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crc32fast::Hasher;

use crate::Collection;

use super::fsync::{ensure_parent_dir, sync_parent_dir, truncate_file_fully_synced};
use super::{apply_wal_record, PersistenceError, WalAppendInfo, WalRecord};

const WAL_BINARY_MAGIC: &[u8; 8] = b"AIONWAL1";
const WAL_FRAME_HEADER_LEN: usize = 8;
const WAL_MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WalEncoding {
    JsonLines,
    BinaryV1,
}

struct CachedWalFile {
    file: File,
    encoding: WalEncoding,
}

pub(super) fn append_wal(
    path: &Path,
    record: &WalRecord,
    sync_on_write: bool,
) -> Result<WalAppendInfo, PersistenceError> {
    append_wal_batch(path, std::slice::from_ref(record), sync_on_write)
}

pub(super) fn append_wal_batch(
    path: &Path,
    records: &[WalRecord],
    sync_on_write: bool,
) -> Result<WalAppendInfo, PersistenceError> {
    if records.is_empty() {
        let wal_size_bytes = if path.exists() {
            path.metadata()?.len()
        } else {
            0
        };
        return Ok(WalAppendInfo {
            wal_size_bytes,
            wal_tail_open: false,
        });
    }
    ensure_parent_dir(path)?;

    let existed = path.exists();
    let mut cache = wal_file_cache()
        .lock()
        .map_err(|_| PersistenceError::InvalidData("wal file cache lock poisoned".to_string()))?;

    if !existed {
        let _ = cache.remove(path);
    }

    if !cache.contains_key(path) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        let encoding = detect_wal_encoding(&mut file)?;
        if matches!(encoding, WalEncoding::BinaryV1) && file.metadata()?.len() == 0 {
            file.write_all(WAL_BINARY_MAGIC)?;
        }
        cache.insert(path.to_path_buf(), CachedWalFile { file, encoding });
    }

    let cached = cache
        .get_mut(path)
        .ok_or_else(|| PersistenceError::InvalidData("wal file cache entry missing".to_string()))?;
    match cached.encoding {
        WalEncoding::JsonLines => append_json_records(&mut cached.file, records)?,
        WalEncoding::BinaryV1 => append_binary_records(&mut cached.file, records)?,
    }
    if sync_on_write {
        cached.file.flush()?;
        cached.file.sync_data()?;
    }
    if !existed {
        sync_parent_dir(path)?;
    }
    let wal_size_bytes = cached.file.seek(SeekFrom::End(0))?;
    Ok(WalAppendInfo {
        wal_size_bytes,
        wal_tail_open: false,
    })
}

pub(super) fn truncate_wal(path: &Path) -> Result<(), PersistenceError> {
    ensure_parent_dir(path)?;
    if let Ok(mut cache) = wal_file_cache().lock() {
        let _ = cache.remove(path);
    }
    truncate_file_fully_synced(path)?;
    Ok(())
}

fn wal_file_cache() -> &'static Mutex<BTreeMap<PathBuf, CachedWalFile>> {
    static CACHE: OnceLock<Mutex<BTreeMap<PathBuf, CachedWalFile>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(super) fn replay_wal(
    path: &Path,
    collections: &mut BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    if !path.exists() {
        return Ok(());
    }

    let mut file = File::open(path)?;
    let encoding = detect_wal_encoding(&mut file)?;
    file.seek(SeekFrom::Start(0))?;
    match encoding {
        WalEncoding::JsonLines => replay_json_lines(BufReader::new(file), collections),
        WalEncoding::BinaryV1 => replay_binary_frames(BufReader::new(file), collections),
    }
}

fn replay_json_lines(
    mut reader: BufReader<File>,
    collections: &mut BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    let mut line = String::new();
    let mut line_number = 0usize;

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        line_number += 1;

        let has_trailing_newline = line.ends_with('\n');
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let record = match serde_json::from_str(trimmed) {
            Ok(record) => record,
            Err(error) => {
                let tolerate_tail = !has_trailing_newline
                    && matches!(error.classify(), serde_json::error::Category::Eof)
                    && reader
                        .fill_buf()
                        .map(|remaining| remaining.is_empty())
                        .unwrap_or(false);
                if tolerate_tail {
                    break;
                }
                return Err(PersistenceError::InvalidData(format!(
                    "invalid wal line {line_number}: {error}"
                )));
            }
        };

        apply_wal_record(collections, &record).map_err(|error| {
            PersistenceError::InvalidData(format!(
                "failed to apply wal line {line_number}: {error}"
            ))
        })?;
    }

    Ok(())
}

fn replay_binary_frames(
    mut reader: BufReader<File>,
    collections: &mut BTreeMap<String, Collection>,
) -> Result<(), PersistenceError> {
    let mut magic = [0u8; WAL_BINARY_MAGIC.len()];
    if !read_exact_or_eof(&mut reader, &mut magic)? {
        return Ok(());
    }
    if &magic != WAL_BINARY_MAGIC {
        return Err(PersistenceError::InvalidData(
            "invalid wal binary header".to_string(),
        ));
    }

    let total_len = reader.get_ref().metadata()?.len();
    let mut frame_index = 0usize;
    let mut header = [0u8; WAL_FRAME_HEADER_LEN];
    loop {
        if !read_exact_or_eof(&mut reader, &mut header)? {
            break;
        }
        frame_index = frame_index.saturating_add(1);
        let payload_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

        if payload_len == 0 || payload_len > WAL_MAX_FRAME_LEN {
            let bytes_remaining = total_len.saturating_sub(reader.stream_position()?);
            let likely_legacy_json_tail = payload_len > WAL_MAX_FRAME_LEN
                && u64::from(payload_len) > bytes_remaining
                && looks_like_json_tail_header(&header);
            if likely_legacy_json_tail {
                break;
            }
            return Err(PersistenceError::InvalidData(format!(
                "invalid wal frame length at frame {frame_index}: {payload_len}"
            )));
        }

        let mut payload = vec![0u8; payload_len as usize];
        if !read_exact_or_eof(&mut reader, &mut payload)? {
            break;
        }

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            return Err(PersistenceError::InvalidData(format!(
                "wal frame checksum mismatch at frame {frame_index}"
            )));
        }

        let record: WalRecord = serde_json::from_slice(&payload).map_err(|error| {
            PersistenceError::InvalidData(format!(
                "invalid wal binary frame {frame_index}: {error}"
            ))
        })?;
        apply_wal_record(collections, &record).map_err(|error| {
            PersistenceError::InvalidData(format!(
                "failed to apply wal binary frame {frame_index}: {error}"
            ))
        })?;
    }

    Ok(())
}

fn append_json_records(file: &mut File, records: &[WalRecord]) -> Result<(), PersistenceError> {
    for record in records {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        file.write_all(&line)?;
    }
    Ok(())
}

fn append_binary_records(file: &mut File, records: &[WalRecord]) -> Result<(), PersistenceError> {
    for record in records {
        let payload = serde_json::to_vec(record)?;
        if payload.len() > WAL_MAX_FRAME_LEN as usize {
            return Err(PersistenceError::InvalidData(format!(
                "wal payload too large: {} bytes",
                payload.len()
            )));
        }

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&checksum.to_le_bytes())?;
        file.write_all(&payload)?;
    }
    Ok(())
}

fn detect_wal_encoding(file: &mut File) -> Result<WalEncoding, PersistenceError> {
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(WalEncoding::BinaryV1);
    }

    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0u8; WAL_BINARY_MAGIC.len()];
    let read = file.read(&mut magic)?;
    file.seek(SeekFrom::End(0))?;
    if read == WAL_BINARY_MAGIC.len() && &magic == WAL_BINARY_MAGIC {
        Ok(WalEncoding::BinaryV1)
    } else {
        Ok(WalEncoding::JsonLines)
    }
}

fn read_exact_or_eof(
    reader: &mut BufReader<File>,
    buffer: &mut [u8],
) -> Result<bool, std::io::Error> {
    let mut offset = 0usize;
    while offset < buffer.len() {
        let read = reader.read(&mut buffer[offset..])?;
        if read == 0 {
            return Ok(false);
        }
        offset += read;
    }
    Ok(true)
}

fn looks_like_json_tail_header(header: &[u8; WAL_FRAME_HEADER_LEN]) -> bool {
    let first = header[0];
    if matches!(first, b'{' | b'[' | b'"') {
        return true;
    }
    first.is_ascii_whitespace() && matches!(header[1], b'{' | b'[' | b'"')
}
