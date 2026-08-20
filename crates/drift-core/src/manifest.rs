use serde::{Deserialize, Serialize};
use std::{
    ffi::OsStr,
    path::{Component, Path, PathBuf},
};
use thiserror::Error;
use uuid::Uuid;

use crate::TransferId;

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
            .map(|file| {
                sanitize_relative_path(&file.relative_path)?;
                Ok(file.relative_path.clone())
            })
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

    pub fn next(&mut self) -> Option<Chunk> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeState {
    pub transfer_id: TransferId,
    pub file_id: Uuid,
    pub chunk_size: u64,
    pub file_size: u64,
    pub completed_chunks: Vec<u64>,
    pub file_digest: Option<String>,
    pub temp_file_path: PathBuf,
}

impl ResumeState {
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
            transfer_id: TransferId::new(),
            file_id: Uuid::new_v4(),
            chunk_size: 4,
            file_size: 10,
            completed_chunks: Vec::new(),
            file_digest: None,
            temp_file_path: PathBuf::from("partial.bin"),
        };
        state.mark_completed(2);
        state.mark_completed(0);
        state.mark_completed(2);
        assert_eq!(state.completed_chunks, vec![0, 2]);
        assert!(state.is_completed(2));
    }
}
