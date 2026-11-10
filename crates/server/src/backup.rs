//! Offline logical backup and restore over the storage backend.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use recolith_backend::{
    BackendError, KeyRange, ScanOptions, Snapshot, StorageBackend, Transaction, TxnOptions,
};
use recolith_storage::crc32c;
use thiserror::Error;

const BACKUP_MAGIC: &[u8; 8] = b"RCLTBK01";
const FOOTER_MAGIC: &[u8; 8] = b"RCLTEND1";
const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;
const BACKUP_SCAN_ENTRIES: usize = 1_024;
const RESTORE_BATCH_ENTRIES: u64 = 1_000;
const RESTORE_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// Backup or restore statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackupStats {
    /// Entries processed.
    pub entries: u64,
    /// Key and value bytes processed.
    pub bytes: u64,
}

/// Backup or restore failure.
#[derive(Debug, Error)]
pub enum BackupError {
    /// File I/O failed.
    #[error("backup I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Backend access failed.
    #[error("backup backend error: {0}")]
    Backend(#[from] BackendError),
    /// Backup magic is invalid.
    #[error("invalid backup magic")]
    InvalidMagic,
    /// Backup footer is invalid or missing.
    #[error("invalid backup footer")]
    InvalidFooter,
    /// Entry length exceeds the limit.
    #[error("backup entry exceeds {MAX_ENTRY_BYTES} bytes")]
    EntryTooLarge,
    /// Stored checksum does not match.
    #[error("backup checksum mismatch for entry {entry}")]
    Checksum {
        /// Entry index.
        entry: u64,
    },
    /// Footer count does not match processed entries.
    #[error("backup entry count mismatch: header says {expected}, processed {actual}")]
    CountMismatch {
        /// Footer count.
        expected: u64,
        /// Processed count.
        actual: u64,
    },
    /// Trailing bytes follow the footer.
    #[error("backup contains trailing data")]
    TrailingData,
    /// Output path already exists.
    #[error("backup output already exists: {0}")]
    OutputExists(PathBuf),
    /// Restore target is not empty.
    #[error("restore target is not empty")]
    TargetNotEmpty,
}

/// Writes all backend entries to an offline backup file.
///
/// The backend must not be serving live traffic. Existing output paths are
/// rejected.
///
/// # Errors
///
/// Returns [`BackupError`] for backend, I/O, or output-path failures.
pub fn backup<B: StorageBackend>(
    backend: &B,
    output: impl AsRef<Path>,
) -> Result<BackupStats, BackupError> {
    let output = output.as_ref();
    if output.exists() {
        return Err(BackupError::OutputExists(output.to_path_buf()));
    }
    let mut temporary = output.as_os_str().to_os_string();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);

    let result = write_backup(backend, &temporary);
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
        return result;
    }
    fs::rename(&temporary, output)?;

    result
}

/// Reads all entries from an offline backup file.
///
/// # Errors
///
/// Returns [`BackupError`] for malformed files, checksum failures, or I/O errors.
pub fn verify(input: impl AsRef<Path>) -> Result<BackupStats, BackupError> {
    let mut reader = BackupReader::open(input.as_ref())?;
    while reader.next_entry()?.is_some() {}

    Ok(reader.stats)
}

/// Restores a backup into an empty backend.
///
/// # Errors
///
/// Returns [`BackupError`] for malformed files, a non-empty target, or backend
/// failures.
pub fn restore<B: StorageBackend>(
    backend: &B,
    input: impl AsRef<Path>,
) -> Result<BackupStats, BackupError> {
    let existing = backend.snapshot()?.scan(
        &KeyRange::prefix(b""),
        ScanOptions {
            limit: Some(1),
            reverse: false,
        },
    )?;
    if !existing.is_empty() {
        return Err(BackupError::TargetNotEmpty);
    }

    let _ = verify(input.as_ref())?;
    let mut reader = BackupReader::open(input.as_ref())?;
    let mut transaction = backend.begin(TxnOptions::default())?;
    let mut batch_entries = 0_u64;
    let mut batch_bytes = 0_usize;
    while let Some((key, value)) = reader.next_entry()? {
        batch_bytes = batch_bytes.saturating_add(key.len() + value.len());
        transaction.put(&key, value);
        batch_entries += 1;
        if batch_entries >= RESTORE_BATCH_ENTRIES || batch_bytes >= RESTORE_BATCH_BYTES {
            transaction.commit()?;
            transaction = backend.begin(TxnOptions::default())?;
            batch_entries = 0;
            batch_bytes = 0;
        }
    }
    if batch_entries > 0 {
        transaction.commit()?;
    } else {
        transaction.rollback();
    }

    Ok(reader.stats)
}

fn write_backup<B: StorageBackend>(backend: &B, path: &Path) -> Result<BackupStats, BackupError> {
    let snapshot = backend.snapshot()?;
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(BACKUP_MAGIC)?;
    let mut stats = BackupStats::default();
    let mut start = Bound::Unbounded;
    loop {
        let entries = snapshot.scan(
            &KeyRange::new(start.clone(), Bound::Unbounded),
            ScanOptions {
                limit: Some(BACKUP_SCAN_ENTRIES),
                reverse: false,
            },
        )?;
        if entries.is_empty() {
            break;
        }
        let last = entries.last().expect("non-empty scan").0.clone();
        for (key, value) in entries {
            if key.len() > MAX_ENTRY_BYTES || value.len() > MAX_ENTRY_BYTES {
                return Err(BackupError::EntryTooLarge);
            }
            writer.write_all(&(key.len() as u32).to_le_bytes())?;
            writer.write_all(&(value.len() as u32).to_le_bytes())?;
            writer.write_all(&crc32c(&key).to_le_bytes())?;
            writer.write_all(&crc32c(&value).to_le_bytes())?;
            writer.write_all(&key)?;
            writer.write_all(&value)?;
            stats.entries += 1;
            stats.bytes += (key.len() + value.len()) as u64;
        }
        start = Bound::Excluded(last);
    }
    writer.write_all(&u32::MAX.to_le_bytes())?;
    writer.write_all(&stats.entries.to_le_bytes())?;
    writer.write_all(FOOTER_MAGIC)?;
    let file = writer.into_inner().map_err(|error| error.into_error())?;
    file.sync_all()?;

    Ok(stats)
}

type BackupEntry = (Vec<u8>, Vec<u8>);

struct BackupReader {
    reader: BufReader<File>,
    stats: BackupStats,
    finished: bool,
}

impl BackupReader {
    fn open(path: &Path) -> Result<Self, BackupError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut magic = [0_u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != BACKUP_MAGIC {
            return Err(BackupError::InvalidMagic);
        }

        Ok(Self {
            reader,
            stats: BackupStats::default(),
            finished: false,
        })
    }

    fn next_entry(&mut self) -> Result<Option<BackupEntry>, BackupError> {
        if self.finished {
            return Ok(None);
        }

        let mut key_length = [0_u8; 4];
        self.reader.read_exact(&mut key_length)?;
        let key_len = u32::from_le_bytes(key_length);
        if key_len == u32::MAX {
            return self.finish();
        }
        let mut header = [0_u8; 12];
        self.reader.read_exact(&mut header)?;
        let value_len = u32::from_le_bytes(header[0..4].try_into().expect("four bytes"));
        let key_crc = u32::from_le_bytes(header[4..8].try_into().expect("four bytes"));
        let value_crc = u32::from_le_bytes(header[8..12].try_into().expect("four bytes"));
        let key_len = key_len as usize;
        let value_len = value_len as usize;
        if key_len > MAX_ENTRY_BYTES || value_len > MAX_ENTRY_BYTES {
            return Err(BackupError::EntryTooLarge);
        }

        let mut key = vec![0_u8; key_len];
        let mut value = vec![0_u8; value_len];
        self.reader.read_exact(&mut key)?;
        self.reader.read_exact(&mut value)?;
        if crc32c(&key) != key_crc || crc32c(&value) != value_crc {
            return Err(BackupError::Checksum {
                entry: self.stats.entries,
            });
        }
        self.stats.entries += 1;
        self.stats.bytes += (key_len + value_len) as u64;

        Ok(Some((key, value)))
    }

    fn finish(&mut self) -> Result<Option<BackupEntry>, BackupError> {
        let mut footer = [0_u8; 16];
        self.reader.read_exact(&mut footer)?;
        let expected = u64::from_le_bytes(footer[0..8].try_into().expect("eight bytes"));
        if &footer[8..16] != FOOTER_MAGIC {
            return Err(BackupError::InvalidFooter);
        }
        if expected != self.stats.entries {
            return Err(BackupError::CountMismatch {
                expected,
                actual: self.stats.entries,
            });
        }
        let mut trailing = [0_u8; 1];
        match self.reader.read(&mut trailing) {
            Ok(0) => {}
            Ok(_) => return Err(BackupError::TrailingData),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {}
            Err(error) => return Err(BackupError::Io(error)),
        }
        self.finished = true;

        Ok(None)
    }
}
