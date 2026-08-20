use drift_core::{
    sanitize_relative_path, FileEntry, ManifestError, ResumeState, TransferId, TransferManifest,
};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::UNIX_EPOCH,
};
use thiserror::Error;
use tokio::fs;

const MAX_SCAN_DIRECTORY_ENTRIES: usize = 1024;

#[derive(Clone, Default)]
pub struct ScanCancellation {
    cancelled: Arc<AtomicBool>,
}

impl ScanCancellation {
    /// Creates a cancellation handle shared with one scan task.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests that the associated scan stop at its next filesystem boundary.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum SourceScanError {
    #[error("source selection must contain at least one path")]
    EmptySelection,
    #[error("source path is unavailable")]
    Unavailable,
    #[error("source path is unreadable")]
    Unreadable,
    #[error("symbolic links are not supported as send sources")]
    SymlinkNotAllowed,
    #[error("source contains an unsupported file type")]
    UnsupportedFileType,
    #[error("source directory contains no regular files")]
    EmptyDirectory,
    #[error("source path has no valid name")]
    InvalidRoot,
    #[error("source contains duplicate or conflicting output paths")]
    DuplicatePath,
    #[error("source path cannot be represented as a relative manifest path")]
    InvalidRelativePath,
    #[error("source size total overflowed")]
    SizeOverflow,
    #[error("source scan was cancelled")]
    Cancelled,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SourceRoot {
    path: PathBuf,
    file_count: usize,
    total_bytes: u64,
}

impl SourceRoot {
    /// Returns the selected source root.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the number of regular files below this root.
    pub fn file_count(&self) -> usize {
        self.file_count
    }

    /// Returns the sum of regular-file sizes below this root.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl std::fmt::Debug for SourceRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceRoot")
            .field("path_configured", &true)
            .field("file_count", &self.file_count)
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SourceScan {
    source_paths: Vec<PathBuf>,
    roots: Vec<SourceRoot>,
    manifest: TransferManifest,
}

impl SourceScan {
    /// Returns the original picker or drop paths.
    pub fn source_paths(&self) -> &[PathBuf] {
        &self.source_paths
    }

    /// Returns per-root counts and byte totals.
    pub fn roots(&self) -> &[SourceRoot] {
        &self.roots
    }

    /// Returns the immutable sender manifest.
    pub fn manifest(&self) -> &TransferManifest {
        &self.manifest
    }

    /// Returns the number of regular files in the manifest.
    pub fn file_count(&self) -> usize {
        self.manifest.files.len()
    }

    /// Returns the manifest's exact byte total.
    pub fn total_bytes(&self) -> u64 {
        self.manifest.total_size
    }
}

impl std::fmt::Debug for SourceScan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceScan")
            .field("source_count", &self.source_paths.len())
            .field("file_count", &self.file_count())
            .field("total_bytes", &self.total_bytes())
            .finish()
    }
}

struct ScannedFile {
    relative_path: PathBuf,
    size: u64,
    modified_at: Option<u64>,
}

struct PendingDirectory {
    absolute_path: PathBuf,
    relative_path: PathBuf,
}

/// Scans sender paths using metadata only and builds deterministic relative paths.
pub async fn scan_send_paths(
    paths: Vec<PathBuf>,
    cancellation: ScanCancellation,
) -> Result<SourceScan, SourceScanError> {
    if paths.is_empty() {
        return Err(SourceScanError::EmptySelection);
    }

    let source_paths = paths.clone();
    let mut files = Vec::new();
    let mut roots = Vec::with_capacity(paths.len());
    let mut root_paths = std::collections::HashSet::new();

    for path in paths {
        check_cancelled(&cancellation)?;
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(map_source_io_error)?;
        if metadata.file_type().is_symlink() {
            return Err(SourceScanError::SymlinkNotAllowed);
        }

        let root_name = path.file_name().ok_or(SourceScanError::InvalidRoot)?;
        let relative_root = sanitize_relative_path(Path::new(root_name))
            .map_err(|_| SourceScanError::InvalidRelativePath)?;
        if !root_paths.insert(relative_root.clone()) {
            return Err(SourceScanError::DuplicatePath);
        }

        let first_file_index = files.len();
        if metadata.file_type().is_file() {
            ensure_readable_file(&metadata)?;
            files.push(scanned_file(relative_root, &metadata));
        } else if metadata.file_type().is_dir() {
            scan_directory(path.clone(), relative_root, &cancellation, &mut files).await?;
            if files.len() == first_file_index {
                return Err(SourceScanError::EmptyDirectory);
            }
        } else {
            return Err(SourceScanError::UnsupportedFileType);
        }

        let root_files = &files[first_file_index..];
        let file_count = root_files.len();
        let total_bytes = root_files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.size)
                .ok_or(SourceScanError::SizeOverflow)
        })?;
        roots.push(SourceRoot {
            path,
            file_count,
            total_bytes,
        });
    }

    check_cancelled(&cancellation)?;
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    validate_relative_paths(&files)?;

    let entries = files
        .into_iter()
        .map(|file| {
            let mut entry =
                FileEntry::new(file.relative_path, file.size).map_err(map_manifest_error)?;
            entry.modified_at = file.modified_at;
            Ok(entry)
        })
        .collect::<Result<Vec<_>, SourceScanError>>()?;
    let manifest = TransferManifest::new(TransferId::new(), entries).map_err(map_manifest_error)?;

    Ok(SourceScan {
        source_paths,
        roots,
        manifest,
    })
}

async fn scan_directory(
    root_path: PathBuf,
    root_relative_path: PathBuf,
    cancellation: &ScanCancellation,
    files: &mut Vec<ScannedFile>,
) -> Result<(), SourceScanError> {
    let mut pending = vec![PendingDirectory {
        absolute_path: root_path,
        relative_path: root_relative_path,
    }];

    while let Some(directory) = pending.pop() {
        check_cancelled(cancellation)?;
        let metadata = fs::symlink_metadata(&directory.absolute_path)
            .await
            .map_err(map_source_io_error)?;
        if metadata.file_type().is_symlink() {
            return Err(SourceScanError::SymlinkNotAllowed);
        }
        if !metadata.file_type().is_dir() {
            return Err(SourceScanError::UnsupportedFileType);
        }

        let mut entries = Vec::new();
        let mut read_dir = fs::read_dir(&directory.absolute_path)
            .await
            .map_err(map_source_io_error)?;
        while let Some(entry) = read_dir.next_entry().await.map_err(map_source_io_error)? {
            check_cancelled(cancellation)?;
            entries.push(entry.path());
            if entries.len() > MAX_SCAN_DIRECTORY_ENTRIES {
                return Err(SourceScanError::SizeOverflow);
            }
        }
        entries.sort();

        for entry_path in entries.into_iter().rev() {
            check_cancelled(cancellation)?;
            let entry_name = entry_path
                .file_name()
                .ok_or(SourceScanError::InvalidRelativePath)?;
            let relative_path = directory.relative_path.join(entry_name);
            let relative_path = sanitize_relative_path(&relative_path)
                .map_err(|_| SourceScanError::InvalidRelativePath)?;
            let metadata = fs::symlink_metadata(&entry_path)
                .await
                .map_err(map_source_io_error)?;
            if metadata.file_type().is_symlink() {
                return Err(SourceScanError::SymlinkNotAllowed);
            }
            if metadata.file_type().is_dir() {
                pending.push(PendingDirectory {
                    absolute_path: entry_path,
                    relative_path,
                });
            } else if metadata.file_type().is_file() {
                ensure_readable_file(&metadata)?;
                files.push(scanned_file(relative_path, &metadata));
            } else {
                return Err(SourceScanError::UnsupportedFileType);
            }
        }
    }

    Ok(())
}

fn scanned_file(relative_path: PathBuf, metadata: &std::fs::Metadata) -> ScannedFile {
    let modified_at = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs());
    ScannedFile {
        relative_path,
        size: metadata.len(),
        modified_at,
    }
}

fn validate_relative_paths(files: &[ScannedFile]) -> Result<(), SourceScanError> {
    let mut previous = None;
    for file in files {
        if let Some(previous_path) = previous {
            if previous_path == file.relative_path || file.relative_path.starts_with(previous_path)
            {
                return Err(SourceScanError::DuplicatePath);
            }
        }
        previous = Some(file.relative_path.as_path());
    }
    Ok(())
}

fn ensure_readable_file(metadata: &std::fs::Metadata) -> Result<(), SourceScanError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o444 == 0 {
            return Err(SourceScanError::Unreadable);
        }
    }
    Ok(())
}

fn check_cancelled(cancellation: &ScanCancellation) -> Result<(), SourceScanError> {
    if cancellation.is_cancelled() {
        Err(SourceScanError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_source_io_error(error: io::Error) -> SourceScanError {
    match error.kind() {
        io::ErrorKind::PermissionDenied => SourceScanError::Unreadable,
        io::ErrorKind::NotFound => SourceScanError::Unavailable,
        _ => SourceScanError::Unavailable,
    }
}

fn map_manifest_error(error: ManifestError) -> SourceScanError {
    match error {
        ManifestError::SizeOverflow => SourceScanError::SizeOverflow,
        _ => SourceScanError::InvalidRelativePath,
    }
}

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
        fs as std_fs,
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

    fn scan_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("drift-storage-{prefix}-{}", TransferId::new()))
    }

    #[tokio::test]
    async fn scans_files_and_nested_directories_with_exact_sorted_totals() {
        let root = scan_root("scan");
        let directory = root.join("bundle");
        fs::create_dir_all(directory.join("nested")).await.unwrap();
        fs::write(directory.join("z.txt"), b"1234").await.unwrap();
        fs::write(directory.join("nested").join("a.txt"), b"12")
            .await
            .unwrap();

        let scan = scan_send_paths(vec![directory.clone()], ScanCancellation::new())
            .await
            .unwrap();
        let paths = scan
            .manifest()
            .files
            .iter()
            .map(|file| file.relative_path.clone())
            .collect::<Vec<_>>();

        assert_eq!(scan.file_count(), 2);
        assert_eq!(scan.total_bytes(), 6);
        assert_eq!(scan.roots()[0].file_count(), 2);
        assert_eq!(scan.roots()[0].total_bytes(), 6);
        assert_eq!(
            paths,
            vec![
                PathBuf::from("bundle/nested/a.txt"),
                PathBuf::from("bundle/z.txt")
            ]
        );
        assert!(scan.manifest().validate().is_ok());

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn scans_multiple_files_and_directories_without_merging_roots() {
        let root = scan_root("multi");
        let first = root.join("first");
        let second = root.join("second");
        fs::create_dir_all(&first).await.unwrap();
        fs::create_dir_all(&second).await.unwrap();
        let first_file = first.join("one.txt");
        let second_file = second.join("two.txt");
        fs::write(&first_file, b"one").await.unwrap();
        fs::write(&second_file, b"two-two").await.unwrap();

        let scan = scan_send_paths(
            vec![first_file.clone(), second.clone()],
            ScanCancellation::new(),
        )
        .await
        .unwrap();

        assert_eq!(scan.file_count(), 2);
        assert_eq!(scan.total_bytes(), 10);
        assert_eq!(scan.roots().len(), 2);
        assert_eq!(
            scan.manifest().files[0].relative_path,
            PathBuf::from("one.txt")
        );
        assert_eq!(
            scan.manifest().files[1].relative_path,
            PathBuf::from("second/two.txt")
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn rejects_empty_selection_and_empty_directories() {
        let root = scan_root("empty");
        fs::create_dir_all(&root).await.unwrap();

        assert_eq!(
            scan_send_paths(Vec::new(), ScanCancellation::new()).await,
            Err(SourceScanError::EmptySelection)
        );
        assert_eq!(
            scan_send_paths(vec![root.clone()], ScanCancellation::new()).await,
            Err(SourceScanError::EmptyDirectory)
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn rejects_duplicate_logical_paths() {
        let root = scan_root("duplicate");
        let first = root.join("first");
        let second = root.join("second");
        fs::create_dir_all(&first).await.unwrap();
        fs::create_dir_all(&second).await.unwrap();
        let first_file = first.join("same.txt");
        let second_file = second.join("same.txt");
        fs::write(&first_file, b"one").await.unwrap();
        fs::write(&second_file, b"two").await.unwrap();

        assert_eq!(
            scan_send_paths(vec![first_file, second_file], ScanCancellation::new()).await,
            Err(SourceScanError::DuplicatePath)
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn cancellation_stops_a_scan_before_filesystem_work() {
        let root = scan_root("cancel");
        fs::create_dir_all(&root).await.unwrap();
        fs::write(root.join("file.txt"), b"data").await.unwrap();
        let cancellation = ScanCancellation::new();
        cancellation.cancel();

        assert_eq!(
            scan_send_paths(vec![root.clone()], cancellation).await,
            Err(SourceScanError::Cancelled)
        );

        let _ = fs::remove_dir_all(root).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinks_unreadable_files_and_special_files() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let root = scan_root("unsafe");
        fs::create_dir_all(root.join("tree")).await.unwrap();
        let target = root.join("target.txt");
        fs::write(&target, b"target").await.unwrap();
        let broken = root.join("broken");
        symlink(root.join("missing"), &broken).unwrap();
        assert_eq!(
            scan_send_paths(vec![broken], ScanCancellation::new()).await,
            Err(SourceScanError::SymlinkNotAllowed)
        );

        let nested_link = root.join("tree").join("escape");
        symlink(&target, &nested_link).unwrap();
        assert_eq!(
            scan_send_paths(vec![root.join("tree")], ScanCancellation::new()).await,
            Err(SourceScanError::SymlinkNotAllowed)
        );

        let unreadable = root.join("unreadable.txt");
        fs::write(&unreadable, b"private").await.unwrap();
        let mut permissions = std_fs::metadata(&unreadable).unwrap().permissions();
        permissions.set_mode(0o000);
        std_fs::set_permissions(&unreadable, permissions).unwrap();
        assert_eq!(
            scan_send_paths(vec![unreadable.clone()], ScanCancellation::new()).await,
            Err(SourceScanError::Unreadable)
        );
        let mut permissions = std_fs::metadata(&unreadable).unwrap().permissions();
        permissions.set_mode(0o600);
        std_fs::set_permissions(&unreadable, permissions).unwrap();

        let socket_path =
            PathBuf::from("/tmp").join(format!("drift-storage-{}", TransferId::new()));
        let _socket = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        assert_eq!(
            scan_send_paths(vec![socket_path.clone()], ScanCancellation::new()).await,
            Err(SourceScanError::UnsupportedFileType)
        );
        drop(_socket);
        let _ = std_fs::remove_file(&socket_path);

        let _ = fs::remove_dir_all(root).await;
    }
}
