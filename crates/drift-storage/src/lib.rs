use drift_core::{ResumeState, TransferId};
use std::{
    io,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::fs;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage I/O failed")]
    Io(#[source] io::Error),
    #[error("storage serialization failed")]
    Serialization(#[source] serde_json::Error),
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum DestinationError {
    #[error("destination path must not be empty")]
    Empty,
    #[error("destination path is unavailable")]
    Unavailable,
    #[error("destination path is not a directory")]
    NotDirectory,
    #[error("destination path is not writable")]
    NotWritable,
}

pub async fn validate_receive_directory(path: impl AsRef<Path>) -> Result<(), DestinationError> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(DestinationError::Empty);
    }

    let write_probe_directory = match fs::metadata(path).await {
        Ok(metadata) if metadata.is_dir() => path.to_path_buf(),
        Ok(_) => return Err(DestinationError::NotDirectory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            match fs::metadata(parent).await {
                Ok(metadata) if metadata.is_dir() => parent.to_path_buf(),
                _ => return Err(DestinationError::Unavailable),
            }
        }
        Err(_) => return Err(DestinationError::Unavailable),
    };

    let probe = write_probe_directory.join(format!(".drift-write-check-{}", TransferId::new()));
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .await
    {
        Ok(_) => fs::remove_file(probe)
            .await
            .map_err(|_| DestinationError::NotWritable),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::ReadOnlyFilesystem
            ) => Err(DestinationError::NotWritable),
        Err(_) => Err(DestinationError::Unavailable),
    }
}

pub struct JsonStore {
    root: PathBuf,
}

impl JsonStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn save_resume(&self, state: &ResumeState) -> Result<PathBuf, StorageError> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(StorageError::Io)?;
        let path = self.resume_path(state.transfer_id);
        let temporary_path = path.with_extension("resume.json.tmp");
        let data = serde_json::to_vec_pretty(state).map_err(StorageError::Serialization)?;
        fs::write(&temporary_path, data)
            .await
            .map_err(StorageError::Io)?;
        match fs::rename(&temporary_path, &path).await {
            Ok(()) => Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                fs::remove_file(&path).await.map_err(StorageError::Io)?;
                fs::rename(&temporary_path, &path)
                    .await
                    .map_err(StorageError::Io)?;
                Ok(path)
            }
            Err(error) => Err(StorageError::Io(error)),
        }
    }

    pub async fn load_resume(
        &self,
        transfer_id: TransferId,
    ) -> Result<Option<ResumeState>, StorageError> {
        let path = self.resume_path(transfer_id);
        let data = match fs::read(path).await {
            Ok(data) => data,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(StorageError::Io(error)),
        };
        serde_json::from_slice(&data)
            .map(Some)
            .map_err(StorageError::Serialization)
    }

    pub async fn remove_resume(&self, transfer_id: TransferId) -> Result<(), StorageError> {
        match fs::remove_file(self.resume_path(transfer_id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::Io(error)),
        }
    }

    fn resume_path(&self, transfer_id: TransferId) -> PathBuf {
        self.root.join(format!("{transfer_id}.resume.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn round_trips_resume_state_and_removes_it() {
        let root = std::env::temp_dir().join(format!(
            "drift-storage-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = JsonStore::new(&root);
        let state = ResumeState {
            transfer_id: TransferId::new(),
            file_id: Uuid::new_v4(),
            chunk_size: 4,
            file_size: 10,
            completed_chunks: vec![0, 2],
            file_digest: Some("digest".into()),
            temp_file_path: PathBuf::from("partial.bin"),
        };
        let transfer_id = state.transfer_id;

        store.save_resume(&state).await.unwrap();
        assert_eq!(store.load_resume(transfer_id).await.unwrap(), Some(state));
        store.remove_resume(transfer_id).await.unwrap();
        assert_eq!(store.load_resume(transfer_id).await.unwrap(), None);
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn validates_writable_receive_directory() {
        let root = std::env::temp_dir().join(format!("drift-storage-destination-{}", TransferId::new()));
        fs::create_dir_all(&root).await.unwrap();

        assert_eq!(validate_receive_directory(&root).await, Ok(()));

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn rejects_empty_file_and_unavailable_receive_destinations() {
        let root = std::env::temp_dir().join(format!("drift-storage-destination-{}", TransferId::new()));
        fs::create_dir_all(&root).await.unwrap();
        let file = root.join("file");
        fs::write(&file, b"not a directory").await.unwrap();

        assert_eq!(
            validate_receive_directory(PathBuf::new()).await,
            Err(DestinationError::Empty)
        );
        assert_eq!(
            validate_receive_directory(&file).await,
            Err(DestinationError::NotDirectory)
        );
        assert_eq!(
            validate_receive_directory(root.join("missing").join("nested")).await,
            Err(DestinationError::Unavailable)
        );

        let _ = fs::remove_dir_all(root).await;
    }
}
