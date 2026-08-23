use serde::{Deserialize, Serialize};
use std::{
    ffi::OsStr,
    fmt,
    path::{Component, Path, PathBuf},
};
use thiserror::Error;
use uuid::Uuid;

use crate::TransferId;

pub const RESUME_SCHEMA_VERSION: u32 = 2;
pub const DEFAULT_RESUME_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ManifestError {
    #[error("manifest must contain at least one file")]
    Empty,
    #[error("file path is empty")]
    EmptyPath,
    #[error("file path must be relative")]
    AbsolutePath,
    #[error("file path contains an invalid component")]
    InvalidComponent,
    #[error("file path contains a NUL byte")]
    NulByte,
    #[error("file size total overflow")]
    SizeOverflow,
    #[error("manifest contains duplicate or conflicting file paths")]
    DuplicatePath,
    #[error("chunk size must be greater than zero")]
    InvalidChunkSize,
}

pub fn sanitize_relative_path(path: &Path) -> Result<PathBuf, ManifestError> {
    let raw = path.to_string_lossy();
    if raw.is_empty() {
        return Err(ManifestError::EmptyPath);
    }
    if raw.contains('\0') {
        return Err(ManifestError::NulByte);
    }

    let portable = raw.replace('\\', "/");
    let has_drive_prefix = portable.len() >= 2
        && portable.as_bytes()[0].is_ascii_alphabetic()
        && portable.as_bytes()[1] == b':';
    if path.is_absolute() || portable.starts_with('/') || has_drive_prefix {
        return Err(ManifestError::AbsolutePath);
    }

    let mut sanitized = PathBuf::new();
    for component in portable.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(ManifestError::InvalidComponent);
        }
        if component == OsStr::new("") {
            return Err(ManifestError::InvalidComponent);
        }
        sanitized.push(component);
    }

    if sanitized.as_os_str().is_empty() {
        return Err(ManifestError::EmptyPath);
    }
    if sanitized.components().any(|component| {
        matches!(
            component,
            Component::CurDir | Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(ManifestError::InvalidComponent);
    }
    Ok(sanitized)
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumeRequest {
    Send { source_paths: Vec<PathBuf> },
    Receive { output_directory: PathBuf },
}

impl ResumeRequest {
    fn validate(&self) -> Result<(), ResumeStateError> {
        match self {
            Self::Send { source_paths } if source_paths.is_empty() => {
                Err(ResumeStateError::InvalidRequest)
            }
            Self::Send { source_paths }
                if source_paths.iter().any(|path| path.as_os_str().is_empty()) =>
            {
                Err(ResumeStateError::InvalidRequest)
            }
            Self::Receive { output_directory } if output_directory.as_os_str().is_empty() => {
                Err(ResumeStateError::InvalidRequest)
            }
            Self::Send { .. } | Self::Receive { .. } => Ok(()),
        }
    }
}

impl fmt::Debug for ResumeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Send { source_paths } => formatter
                .debug_struct("ResumeRequest::Send")
                .field("source_count", &source_paths.len())
                .finish(),
            Self::Receive { .. } => formatter
                .debug_struct("ResumeRequest::Receive")
                .field("output_directory_configured", &true)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeCapabilities {
    pub pause: bool,
    pub resume: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResumeStateError {
    #[error("unsupported resume schema version {found}; expected {expected}")]
    UnsupportedSchema { found: u32, expected: u32 },
    #[error("resume backend name must not be empty")]
    EmptyBackend,
    #[error("resume backend version must not be empty")]
    EmptyBackendVersion,
    #[error("resume request is invalid")]
    InvalidRequest,
    #[error("resume manifest is invalid")]
    InvalidManifest(#[source] ManifestError),
    #[error("resume chunk size must be greater than zero")]
    InvalidChunkSize,
    #[error("resume temporary path must be relative and safe")]
    InvalidTemporaryPath,
    #[error("resume completed chunks are invalid")]
    InvalidCompletedChunks,
    #[error("resume file is not present in manifest")]
    FileNotInManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub file_id: Uuid,
    pub relative_path: PathBuf,
    pub size: u64,
    pub modified_at: Option<u64>,
    pub digest: Option<String>,
}

impl FileEntry {
    pub fn new(relative_path: impl Into<PathBuf>, size: u64) -> Result<Self, ManifestError> {
        let relative_path = sanitize_relative_path(&relative_path.into())?;
        Ok(Self {
            file_id: Uuid::new_v4(),
            relative_path,
            size,
            modified_at: None,
            digest: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferManifest {
    pub transfer_id: TransferId,
    pub files: Vec<FileEntry>,
    pub total_size: u64,
}

impl TransferManifest {
    pub fn new(transfer_id: TransferId, files: Vec<FileEntry>) -> Result<Self, ManifestError> {
        if files.is_empty() {
            return Err(ManifestError::Empty);
        }
        let total_size = files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.size)
                .ok_or(ManifestError::SizeOverflow)
        })?;
        Ok(Self {
            transfer_id,
            files,
            total_size,
        })
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.files.is_empty() {
            return Err(ManifestError::Empty);
        }
        let mut paths = self
            .files
            .iter()
            .map(|file| sanitize_relative_path(&file.relative_path))
            .collect::<Result<Vec<_>, ManifestError>>()?;
        paths.sort();
        if paths
            .windows(2)
            .any(|window| window[0] == window[1] || window[1].starts_with(&window[0]))
        {
            return Err(ManifestError::DuplicatePath);
        }
        let computed_total = self.files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.size)
                .ok_or(ManifestError::SizeOverflow)
        })?;
        if computed_total != self.total_size {
            return Err(ManifestError::SizeOverflow);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    pub file_id: Uuid,
    pub index: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkState {
    Pending,
    InFlight,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkScheduler {
    chunks: Vec<Chunk>,
    states: Vec<ChunkState>,
    next_index: usize,
}

impl ChunkScheduler {
    pub fn new(file_id: Uuid, file_size: u64, chunk_size: u64) -> Result<Self, ManifestError> {
        if chunk_size == 0 {
            return Err(ManifestError::InvalidChunkSize);
        }
        let chunk_count = if file_size == 0 {
            0
        } else {
            (file_size - 1) / chunk_size + 1
        };
        let chunks = (0..chunk_count)
            .map(|index| {
                let offset = index * chunk_size;
                let length = (file_size - offset).min(chunk_size);
                Chunk {
                    file_id,
                    index,
                    offset,
                    length,
                }
            })
            .collect::<Vec<_>>();
        let states = vec![ChunkState::Pending; chunks.len()];
        Ok(Self {
            chunks,
            states,
            next_index: 0,
        })
    }
}

impl Iterator for ChunkScheduler {
    type Item = Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_index < self.chunks.len() {
            let index = self.next_index;
            self.next_index += 1;
            if self.states[index] == ChunkState::Pending {
                self.states[index] = ChunkState::InFlight;
                return Some(self.chunks[index]);
            }
        }
        None
    }
}

impl ChunkScheduler {
    pub fn mark_completed(&mut self, index: u64) -> bool {
        self.update_state(index, ChunkState::Completed)
    }

    pub fn mark_failed(&mut self, index: u64) -> bool {
        self.update_state(index, ChunkState::Failed)
    }

    pub fn retry_failed(&mut self) {
        for state in &mut self.states {
            if *state == ChunkState::Failed {
                *state = ChunkState::Pending;
            }
        }
        self.next_index = 0;
    }

    pub fn chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    pub fn state(&self, index: u64) -> Option<ChunkState> {
        self.states.get(index as usize).copied()
    }

    pub fn completed_count(&self) -> usize {
        self.states
            .iter()
            .filter(|state| **state == ChunkState::Completed)
            .count()
    }

    fn update_state(&mut self, index: u64, state: ChunkState) -> bool {
        let Some(current) = self.states.get_mut(index as usize) else {
            return false;
        };
        *current = state;
        true
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeState {
    pub schema_version: u32,
    pub transfer_id: TransferId,
    pub backend: String,
    pub backend_version: Option<String>,
    pub capabilities: ResumeCapabilities,
    pub request: ResumeRequest,
    pub manifest: Option<TransferManifest>,
    pub file_id: Uuid,
    pub chunk_size: u64,
    pub file_size: u64,
    pub completed_chunks: Vec<u64>,
    pub file_digest: Option<String>,
    pub temp_file_path: Option<PathBuf>,
}

impl fmt::Debug for ResumeState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResumeState")
            .field("schema_version", &self.schema_version)
            .field("transfer_id", &self.transfer_id)
            .field("backend", &self.backend)
            .field("backend_version", &self.backend_version)
            .field("capabilities", &self.capabilities)
            .field("request", &self.request)
            .field("manifest_configured", &self.manifest.is_some())
            .field("file_id", &self.file_id)
            .field("chunk_size", &self.chunk_size)
            .field("file_size", &self.file_size)
            .field("completed_chunk_count", &self.completed_chunks.len())
            .field("file_digest_configured", &self.file_digest.is_some())
            .field("temp_file_configured", &self.temp_file_path.is_some())
            .finish()
    }
}

impl ResumeState {
    pub fn validate(&self) -> Result<(), ResumeStateError> {
        if self.schema_version != RESUME_SCHEMA_VERSION {
            return Err(ResumeStateError::UnsupportedSchema {
                found: self.schema_version,
                expected: RESUME_SCHEMA_VERSION,
            });
        }
        if self.backend.trim().is_empty() {
            return Err(ResumeStateError::EmptyBackend);
        }
        if self.backend_version.as_deref().is_some_and(str::is_empty) {
            return Err(ResumeStateError::EmptyBackendVersion);
        }
        self.request.validate()?;
        if matches!(self.request, ResumeRequest::Send { .. }) && self.manifest.is_none() {
            return Err(ResumeStateError::InvalidManifest(ManifestError::Empty));
        }
        if let Some(manifest) = &self.manifest {
            manifest
                .validate()
                .map_err(ResumeStateError::InvalidManifest)?;
            let Some(file) = manifest
                .files
                .iter()
                .find(|file| file.file_id == self.file_id)
            else {
                return Err(ResumeStateError::FileNotInManifest);
            };
            if manifest.transfer_id != self.transfer_id
                || file.size != self.file_size
                || file.digest != self.file_digest
            {
                return Err(ResumeStateError::FileNotInManifest);
            }
        }
        if self.chunk_size == 0 {
            return Err(ResumeStateError::InvalidChunkSize);
        }
        if let Some(temp_file_path) = &self.temp_file_path {
            if temp_file_path.is_absolute() || sanitize_relative_path(temp_file_path).is_err() {
                return Err(ResumeStateError::InvalidTemporaryPath);
            }
        }
        let chunk_count = if self.file_size == 0 {
            0
        } else {
            (self.file_size - 1) / self.chunk_size + 1
        };
        if self
            .completed_chunks
            .windows(2)
            .any(|window| window[0] >= window[1])
            || self
                .completed_chunks
                .iter()
                .any(|index| *index >= chunk_count)
        {
            return Err(ResumeStateError::InvalidCompletedChunks);
        }
        Ok(())
    }

    pub fn is_completed(&self, chunk_index: u64) -> bool {
        self.completed_chunks.binary_search(&chunk_index).is_ok()
    }

    pub fn mark_completed(&mut self, chunk_index: u64) {
        match self.completed_chunks.binary_search(&chunk_index) {
            Ok(_) => {}
            Err(position) => self.completed_chunks.insert(position, chunk_index),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_traversal_and_absolute_paths() {
        assert_eq!(
            sanitize_relative_path(Path::new("../secret")),
            Err(ManifestError::InvalidComponent)
        );
        assert_eq!(
            sanitize_relative_path(Path::new("/tmp/secret")),
            Err(ManifestError::AbsolutePath)
        );
        assert_eq!(
            sanitize_relative_path(Path::new("C:\\secret.txt")),
            Err(ManifestError::AbsolutePath)
        );
        assert_eq!(
            sanitize_relative_path(Path::new("..\\secret.txt")),
            Err(ManifestError::InvalidComponent)
        );
    }

    #[test]
    fn accepts_nested_relative_path() {
        assert_eq!(
            sanitize_relative_path(Path::new("folder/file.txt")).unwrap(),
            PathBuf::from("folder/file.txt")
        );
    }

    #[test]
    fn computes_manifest_total() {
        let transfer_id = TransferId::new();
        let files = vec![
            FileEntry::new("one.bin", 2).unwrap(),
            FileEntry::new("two.bin", 3).unwrap(),
        ];
        let manifest = TransferManifest::new(transfer_id, files).unwrap();
        assert_eq!(manifest.total_size, 5);
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn rejects_duplicate_and_file_directory_conflicts() {
        let duplicate = TransferManifest {
            transfer_id: TransferId::new(),
            files: vec![
                FileEntry::new("folder/file.txt", 1).unwrap(),
                FileEntry::new("folder/file.txt", 1).unwrap(),
            ],
            total_size: 2,
        };
        assert_eq!(duplicate.validate(), Err(ManifestError::DuplicatePath));

        let conflict = TransferManifest {
            transfer_id: TransferId::new(),
            files: vec![
                FileEntry::new("folder", 1).unwrap(),
                FileEntry::new("folder/file.txt", 1).unwrap(),
            ],
            total_size: 2,
        };
        assert_eq!(conflict.validate(), Err(ManifestError::DuplicatePath));
    }

    #[test]
    fn rejects_duplicate_paths_after_normalization() {
        let duplicate = TransferManifest {
            transfer_id: TransferId::new(),
            files: vec![
                FileEntry {
                    file_id: Uuid::new_v4(),
                    relative_path: PathBuf::from("folder/file.txt"),
                    size: 1,
                    modified_at: None,
                    digest: None,
                },
                FileEntry {
                    file_id: Uuid::new_v4(),
                    relative_path: PathBuf::from(r"folder\file.txt"),
                    size: 1,
                    modified_at: None,
                    digest: None,
                },
            ],
            total_size: 2,
        };

        assert_eq!(duplicate.validate(), Err(ManifestError::DuplicatePath));
    }

    #[test]
    fn schedules_final_partial_chunk() {
        let file_id = Uuid::new_v4();
        let mut scheduler = ChunkScheduler::new(file_id, 10, 4).unwrap();
        assert_eq!(scheduler.next().unwrap().length, 4);
        assert_eq!(scheduler.next().unwrap().length, 4);
        assert_eq!(scheduler.next().unwrap().length, 2);
        assert!(scheduler.next().is_none());
    }

    #[test]
    fn resume_state_keeps_completed_chunks_sorted_and_unique() {
        let mut state = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            transfer_id: TransferId::new(),
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: ResumeRequest::Send {
                source_paths: vec![PathBuf::from("source.bin")],
            },
            manifest: None,
            file_id: Uuid::new_v4(),
            chunk_size: 4,
            file_size: 10,
            completed_chunks: Vec::new(),
            file_digest: None,
            temp_file_path: None,
        };
        state.mark_completed(2);
        state.mark_completed(0);
        state.mark_completed(2);
        assert_eq!(state.completed_chunks, vec![0, 2]);
        assert!(state.is_completed(2));
    }

    #[test]
    fn rejects_incompatible_resume_metadata_without_secret_fields() {
        let transfer_id = TransferId::new();
        let state = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION + 1,
            transfer_id,
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: ResumeRequest::Receive {
                output_directory: PathBuf::from("/tmp/receive"),
            },
            manifest: None,
            file_id: Uuid::nil(),
            chunk_size: DEFAULT_RESUME_CHUNK_SIZE,
            file_size: 0,
            completed_chunks: Vec::new(),
            file_digest: None,
            temp_file_path: None,
        };

        assert!(matches!(
            state.validate(),
            Err(ResumeStateError::UnsupportedSchema { .. })
        ));
        assert!(!format!("{state:?}").contains("/tmp/receive"));
    }
}
