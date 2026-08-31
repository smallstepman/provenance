use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use nemo_relay_plugin::Event;
use sha2::{Digest, Sha256};

use crate::error::SpoolError;

/// Immutable locator for one serialized ATOF event in the local spool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceLocator {
    /// Spool path as presented to the provenance graph.
    pub path: String,
    /// Byte offset of the JSON object, excluding its trailing newline.
    pub byte_offset: u64,
    /// Serialized JSON byte length, excluding its trailing newline.
    pub byte_length: u64,
    /// SHA-256 digest of the serialized JSON object.
    pub digest: String,
}

/// Append-only JSONL sink for complete, sanitized ATOF events.
///
/// The provenance graph stores only this locator and digest. The event payload
/// remains in the local evidence file, where it can be replayed independently
/// of telemetry providers.
pub struct LocalEventSpool {
    path: PathBuf,
    file: File,
}

impl std::fmt::Debug for LocalEventSpool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalEventSpool")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl LocalEventSpool {
    /// Open or create an append-only JSONL spool.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SpoolError> {
        let path = path.as_ref().to_owned();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self { path, file })
    }

    /// Return the on-disk path used by this spool.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Serialize and durably append one complete ATOF event.
    pub fn append(&mut self, event: &Event) -> Result<EvidenceLocator, SpoolError> {
        let bytes = serde_json::to_vec(event)?;
        let byte_offset = self.file.metadata()?.len();
        self.file.write_all(&bytes)?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        self.file.sync_data()?;

        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Ok(EvidenceLocator {
            path: self.path.to_string_lossy().into_owned(),
            byte_offset,
            byte_length: bytes.len() as u64,
            digest,
        })
    }
}

/// Return the byte length of an existing spool without opening it for writes.
pub fn spool_size(path: impl AsRef<Path>) -> io::Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}
