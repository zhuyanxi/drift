use drift_core::{
    sanitize_relative_path, FileEntry, ManifestError, ResumeState, ResumeStateError, TransferId,
    TransferManifest,
};
use std::{
    ffi::{OsStr, OsString},
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
const RECEIVE_STAGING_PREFIX: &str = ".drift-staging-";

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
    #[error("source directory contains too many entries")]
    TooManyEntries,
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
                return Err(SourceScanError::TooManyEntries);
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
    #[error("resume metadata is invalid")]
    InvalidResume(#[source] ResumeStateError),
}

#[derive(Debug, Error)]
pub enum ReceiveStagingError {
    #[error("receive destination is invalid")]
    Destination(#[source] DestinationError),
    #[error("receive destination changed during transfer")]
    DestinationChanged,
    #[error("receive output is empty")]
    EmptyOutput,
    #[error("receive output path is invalid")]
    InvalidOutputPath,
    #[error("receive output contains a symbolic link")]
    SymlinkOutput,
    #[error("receive output conflicts with an existing destination entry")]
    Conflict,
    #[error("receive staging I/O failed")]
    Io(#[source] io::Error),
    #[error("receive staging rollback failed")]
    Rollback(#[source] io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveFinalizeReport {
    cleanup_failed: bool,
}

impl ReceiveFinalizeReport {
    pub fn cleanup_failed(self) -> bool {
        self.cleanup_failed
    }
}

#[derive(Clone)]
pub struct ReceiveStaging {
    destination: PathBuf,
    path: PathBuf,
    relative_path: PathBuf,
    existing_destination_entries: Vec<OsString>,
}

impl std::fmt::Debug for ReceiveStaging {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceiveStaging")
            .field("destination_configured", &true)
            .field("staging_configured", &true)
            .finish()
    }
}

impl ReceiveStaging {
    pub async fn create(destination: impl Into<PathBuf>) -> Result<Self, ReceiveStagingError> {
        let destination = destination.into();
        validate_receive_directory(&destination)
            .await
            .map_err(ReceiveStagingError::Destination)?;
        fs::create_dir_all(&destination)
            .await
            .map_err(ReceiveStagingError::Io)?;
        validate_destination_components(&destination)
            .await
            .map_err(ReceiveStagingError::Destination)?;

        let existing_destination_entries = read_directory_names(&destination)
            .await
            .map_err(ReceiveStagingError::Io)?;
        let relative_path = PathBuf::from(format!(
            "{RECEIVE_STAGING_PREFIX}{}",
            TransferId::new()
        ));
        let path = destination.join(&relative_path);
        fs::create_dir(&path)
            .await
            .map_err(ReceiveStagingError::Io)?;
        set_private_directory_permissions(&path)
            .await
            .map_err(ReceiveStagingError::Io)?;

        Ok(Self {
            destination,
            path,
            relative_path,
            existing_destination_entries,
        })
    }

    pub fn destination(&self) -> &Path {
        &self.destination
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn relative_path(&self) -> &Path {
        &self.relative_path
    }

    /// Publishes verified staged output without replacing existing destination entries.
    ///
    /// Each top-level entry is published independently. If a later entry fails, earlier
    /// entries are rolled back on a best-effort basis because filesystems provide no
    /// group transaction for multiple paths.
    pub async fn finalize(&self) -> Result<ReceiveFinalizeReport, ReceiveStagingError> {
        validate_receive_directory(&self.destination)
            .await
            .map_err(ReceiveStagingError::Destination)?;
        validate_staging_root(&self.path).await?;
        reject_unexpected_destination_entries(
            &self.destination,
            &self.relative_path,
            &self.existing_destination_entries,
        )
        .await?;

        let entries = read_directory_entries(&self.path)
            .await
            .map_err(ReceiveStagingError::Io)?;
        if entries.is_empty() {
            return Err(ReceiveStagingError::EmptyOutput);
        }
        for entry in &entries {
            validate_staged_tree(&entry.path, Path::new(entry.name.as_os_str())).await?;
        }

        let mut published = Vec::with_capacity(entries.len());
        for entry in entries {
            let final_path = self.destination.join(&entry.name);
            if let Err(error) =
                rename_without_replacing(entry.path.clone(), final_path.clone()).await
            {
                rollback_published(&published).await?;
                return Err(if error.kind() == io::ErrorKind::AlreadyExists {
                    ReceiveStagingError::Conflict
                } else {
                    ReceiveStagingError::Io(error)
                });
            }
            published.push((final_path, entry.path));
        }

        let cleanup_failed = fs::remove_dir(&self.path).await.is_err();
        Ok(ReceiveFinalizeReport { cleanup_failed })
    }

    pub async fn cleanup(&self) -> Result<(), ReceiveStagingError> {
        match fs::symlink_metadata(&self.path).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                Err(ReceiveStagingError::SymlinkOutput)
            }
            Ok(_) => fs::remove_dir_all(&self.path)
                .await
                .map_err(ReceiveStagingError::Io),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(ReceiveStagingError::Io(error)),
        }
    }
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
    #[error("destination path contains a symbolic link")]
    SymlinkNotAllowed,
}

/// Validates that a receive destination exists or can be created and is writable.
///
/// Existing ancestor symlinks resolving to directories are allowed for standard paths
/// such as macOS `/var`; the selected destination itself must not be a symlink.
pub async fn validate_receive_directory(path: impl AsRef<Path>) -> Result<(), DestinationError> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(DestinationError::Empty);
    }
    validate_destination_components(path).await?;

    let write_probe_directory = match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() => path.to_path_buf(),
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(DestinationError::SymlinkNotAllowed)
        }
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

async fn validate_destination_components(path: &Path) -> Result<(), DestinationError> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(DestinationError::Unavailable);
    }
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            continue;
        }
        match fs::symlink_metadata(ancestor).await {
            Ok(metadata) if metadata.file_type().is_symlink() && ancestor == path => {
                return Err(DestinationError::SymlinkNotAllowed)
            }
            Ok(metadata) if metadata.file_type().is_symlink() => {
                match fs::metadata(ancestor).await {
                    Ok(metadata) if metadata.is_dir() => {}
                    Ok(_) => return Err(DestinationError::Unavailable),
                    Err(_) => return Err(DestinationError::Unavailable),
                }
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(if ancestor == path {
                    DestinationError::NotDirectory
                } else {
                    DestinationError::Unavailable
                })
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(DestinationError::Unavailable),
        }
    }
    Ok(())
}

struct DirectoryEntry {
    name: OsString,
    path: PathBuf,
}

async fn read_directory_entries(path: &Path) -> io::Result<Vec<DirectoryEntry>> {
    let mut entries = Vec::new();
    let mut directory = fs::read_dir(path).await?;
    while let Some(entry) = directory.next_entry().await? {
        entries.push(DirectoryEntry {
            name: entry.file_name(),
            path: entry.path(),
        });
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

async fn read_directory_names(path: &Path) -> io::Result<Vec<OsString>> {
    Ok(read_directory_entries(path)
        .await?
        .into_iter()
        .map(|entry| entry.name)
        .collect())
}

async fn set_private_directory_permissions(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path).await?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).await?;
    }
    let _ = path;
    Ok(())
}

async fn validate_staging_root(path: &Path) -> Result<(), ReceiveStagingError> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(ReceiveStagingError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ReceiveStagingError::SymlinkOutput);
    }
    Ok(())
}

async fn reject_unexpected_destination_entries(
    destination: &Path,
    staging_name: &Path,
    existing_entries: &[OsString],
) -> Result<(), ReceiveStagingError> {
    let staging_name = staging_name
        .file_name()
        .ok_or(ReceiveStagingError::InvalidOutputPath)?;
    let current_entries = read_directory_names(destination)
        .await
        .map_err(ReceiveStagingError::Io)?;
    if current_entries.into_iter().any(|entry| {
        entry != staging_name
            && !is_receive_staging_name(&entry)
            && !existing_entries.iter().any(|existing| existing == &entry)
    }) {
        return Err(ReceiveStagingError::DestinationChanged);
    }
    Ok(())
}

async fn validate_staged_tree(path: &Path, relative_path: &Path) -> Result<(), ReceiveStagingError> {
    let mut pending = vec![(path.to_path_buf(), relative_path.to_path_buf())];
    while let Some((path, relative_path)) = pending.pop() {
        sanitize_relative_path(&relative_path)
            .map_err(|_| ReceiveStagingError::InvalidOutputPath)?;
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(ReceiveStagingError::Io)?;
        if metadata.file_type().is_symlink() {
            return Err(ReceiveStagingError::SymlinkOutput);
        }
        if metadata.is_dir() {
            let entries = read_directory_entries(&path)
                .await
                .map_err(ReceiveStagingError::Io)?;
            for entry in entries {
                pending.push((entry.path, relative_path.join(entry.name)));
            }
        } else if !metadata.is_file() {
            return Err(ReceiveStagingError::InvalidOutputPath);
        }
    }
    Ok(())
}

async fn rollback_published(
    published: &[(PathBuf, PathBuf)],
) -> Result<(), ReceiveStagingError> {
    for (published_path, staged_path) in published.iter().rev() {
        if let Err(error) =
            rename_without_replacing(published_path.clone(), staged_path.clone()).await
        {
            return Err(ReceiveStagingError::Rollback(error));
        }
    }
    Ok(())
}

async fn rename_without_replacing(source: PathBuf, destination: PathBuf) -> io::Result<()> {
    tokio::task::spawn_blocking(move || rename_without_replacing_sync(&source, &destination))
        .await
        .map_err(|_| io::Error::other("exclusive rename task failed"))?
}

fn rename_without_replacing_sync(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let source = path_to_c_string(source)?;
        let destination = path_to_c_string(destination)?;
        let result = unsafe {
            libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL)
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "linux")]
    {
        let source = path_to_c_string(source)?;
        let destination = path_to_c_string(destination)?;
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (source, destination);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "exclusive rename unsupported on this platform",
        ))
    }
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn path_to_c_string(path: &Path) -> io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;

    std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))
}

fn is_receive_staging_path(path: &Path) -> bool {
    path.components().count() == 1
        && path
            .file_name()
            .is_some_and(is_receive_staging_name)
}

fn is_receive_staging_name(name: &OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with(RECEIVE_STAGING_PREFIX))
}

async fn remove_owned_partial(path: PathBuf) -> Result<(), StorageError> {
    match fs::symlink_metadata(&path).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path).await.map_err(StorageError::Io)
        }
        Ok(_) => fs::remove_file(path).await.map_err(StorageError::Io),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::Io(error)),
    }
}

#[derive(Clone)]
pub struct JsonStore {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeDiscovery {
    recoverable: Vec<ResumeState>,
    invalid_count: usize,
}

impl ResumeDiscovery {
    pub fn recoverable(&self) -> &[ResumeState] {
        &self.recoverable
    }

    pub fn invalid_count(&self) -> usize {
        self.invalid_count
    }
}

impl JsonStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub async fn save_resume(&self, state: &ResumeState) -> Result<PathBuf, StorageError> {
        state.validate().map_err(StorageError::InvalidResume)?;
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
        let state: ResumeState =
            serde_json::from_slice(&data).map_err(StorageError::Serialization)?;
        state
            .validate()
            .map_err(StorageError::InvalidResume)
            .map(|()| Some(state))
    }

    pub async fn remove_resume(&self, transfer_id: TransferId) -> Result<(), StorageError> {
        let path = self.resume_path(transfer_id);
        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(StorageError::Io(error)),
        }
        match fs::remove_file(path.with_extension("resume.json.tmp")).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::Io(error)),
        }
    }

    pub async fn discover_resumes(&self) -> Result<ResumeDiscovery, StorageError> {
        let mut directory = match fs::read_dir(&self.root).await {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(ResumeDiscovery {
                    recoverable: Vec::new(),
                    invalid_count: 0,
                })
            }
            Err(error) => return Err(StorageError::Io(error)),
        };
        let mut recoverable = Vec::new();
        let mut invalid_count = 0;
        while let Some(entry) = directory.next_entry().await.map_err(StorageError::Io)? {
            let path = entry.path();
            if !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".resume.json"))
            {
                continue;
            }
            let data = match fs::read(&path).await {
                Ok(data) => data,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(StorageError::Io(error)),
            };
            match serde_json::from_slice::<ResumeState>(&data) {
                Ok(state) if state.validate().is_ok() => recoverable.push(state),
                Ok(_) | Err(_) => invalid_count += 1,
            }
        }
        recoverable.sort_by_key(|state| state.transfer_id.to_string());
        Ok(ResumeDiscovery {
            recoverable,
            invalid_count,
        })
    }

    pub async fn discard_resume(&self, transfer_id: TransferId) -> Result<(), StorageError> {
        let state = match self.load_resume(transfer_id).await {
            Ok(state) => state,
            Err(StorageError::InvalidResume(_) | StorageError::Serialization(_)) => None,
            Err(error) => return Err(error),
        };
        if let Some(state) = state {
            if let Some(temp_file_path) = state.temp_file_path {
                let partial_path = match &state.request {
                    drift_core::ResumeRequest::Receive { output_directory }
                        if is_receive_staging_path(&temp_file_path)
                            && validate_destination_components(output_directory)
                                .await
                                .is_ok() => output_directory.join(&temp_file_path),
                    _ => self.root.join(&temp_file_path),
                };
                remove_owned_partial(partial_path).await?;
            }
        }
        self.remove_resume(transfer_id).await
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
            schema_version: drift_core::RESUME_SCHEMA_VERSION,
            transfer_id: TransferId::new(),
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: drift_core::ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: drift_core::ResumeRequest::Receive {
                output_directory: PathBuf::from("/tmp/receive"),
            },
            manifest: None,
            file_id: Uuid::new_v4(),
            chunk_size: 4,
            file_size: 10,
            completed_chunks: vec![0, 2],
            file_digest: Some("digest".into()),
            temp_file_path: Some(PathBuf::from("partial.bin")),
        };
        let transfer_id = state.transfer_id;

        store.save_resume(&state).await.unwrap();
        assert_eq!(store.load_resume(transfer_id).await.unwrap(), Some(state));
        store.remove_resume(transfer_id).await.unwrap();
        assert_eq!(store.load_resume(transfer_id).await.unwrap(), None);
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn discovers_valid_resume_and_counts_corrupt_metadata() {
        let root = std::env::temp_dir().join(format!(
            "drift-storage-discovery-{}",
            TransferId::new()
        ));
        let store = JsonStore::new(&root);
        let transfer_id = TransferId::new();
        let file = FileEntry::new("source.bin", 10).unwrap();
        let manifest = TransferManifest::new(transfer_id, vec![file.clone()]).unwrap();
        let state = ResumeState {
            schema_version: drift_core::RESUME_SCHEMA_VERSION,
            transfer_id,
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: drift_core::ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: drift_core::ResumeRequest::Send {
                source_paths: vec![PathBuf::from("source.bin")],
            },
            manifest: Some(manifest),
            file_id: file.file_id,
            chunk_size: 4,
            file_size: 10,
            completed_chunks: vec![0, 2],
            file_digest: None,
            temp_file_path: Some(PathBuf::from("partial.bin")),
        };
        store.save_resume(&state).await.unwrap();
        fs::write(
            root.join(format!("{}.resume.json", TransferId::new())),
            b"not-json",
        )
        .await
        .unwrap();

        let discovery = store.discover_resumes().await.unwrap();
        assert_eq!(discovery.recoverable(), &[state]);
        assert_eq!(discovery.invalid_count(), 1);
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn discarding_resume_removes_explicitly_owned_partial_file_and_metadata() {
        let root = std::env::temp_dir().join(format!(
            "drift-storage-discard-{}",
            TransferId::new()
        ));
        let store = JsonStore::new(&root);
        let state = ResumeState {
            schema_version: drift_core::RESUME_SCHEMA_VERSION,
            transfer_id: TransferId::new(),
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: drift_core::ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: drift_core::ResumeRequest::Receive {
                output_directory: PathBuf::from("/tmp/receive"),
            },
            manifest: None,
            file_id: Uuid::new_v4(),
            chunk_size: 4,
            file_size: 10,
            completed_chunks: vec![0, 2],
            file_digest: None,
            temp_file_path: Some(PathBuf::from("partial.bin")),
        };
        store.save_resume(&state).await.unwrap();
        fs::write(root.join("partial.bin"), b"partial")
            .await
            .unwrap();

        store.discard_resume(state.transfer_id).await.unwrap();
        assert!(!root.join("partial.bin").exists());
        assert_eq!(store.load_resume(state.transfer_id).await.unwrap(), None);
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn discarding_croc_resume_preserves_unowned_partial_path() {
        let root = std::env::temp_dir().join(format!(
            "drift-storage-unowned-partial-{}",
            TransferId::new()
        ));
        let store = JsonStore::new(&root);
        let state = ResumeState {
            schema_version: drift_core::RESUME_SCHEMA_VERSION,
            transfer_id: TransferId::new(),
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: drift_core::ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: drift_core::ResumeRequest::Receive {
                output_directory: PathBuf::from("/tmp/receive"),
            },
            manifest: None,
            file_id: Uuid::new_v4(),
            chunk_size: 4,
            file_size: 10,
            completed_chunks: vec![0, 2],
            file_digest: None,
            temp_file_path: None,
        };
        let partial_directory = root.join("partials");
        let partial_path = partial_directory.join(format!("{}.partial", state.transfer_id));
        store.save_resume(&state).await.unwrap();
        fs::create_dir_all(&partial_directory).await.unwrap();
        fs::write(&partial_path, b"croc-owned output").await.unwrap();

        store.discard_resume(state.transfer_id).await.unwrap();

        assert!(partial_path.exists());
        assert_eq!(store.load_resume(state.transfer_id).await.unwrap(), None);
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

    #[tokio::test]
    async fn receive_staging_atomically_publishes_nested_output() {
        let root = scan_root("receive-finalize");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        let staging = ReceiveStaging::create(&destination).await.unwrap();
        let nested = staging.path().join("bundle").join("nested");
        fs::create_dir_all(&nested).await.unwrap();
        fs::write(nested.join("file.txt"), b"verified output")
            .await
            .unwrap();

        let report = staging.finalize().await.unwrap();

        assert!(!report.cleanup_failed());
        assert_eq!(
            fs::read(destination.join("bundle").join("nested").join("file.txt"))
                .await
                .unwrap(),
            b"verified output"
        );
        assert!(!staging.path().exists());
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn receive_staging_allows_concurrent_private_staging() {
        let root = scan_root("receive-concurrent");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        let first = ReceiveStaging::create(&destination).await.unwrap();
        let second = ReceiveStaging::create(&destination).await.unwrap();
        fs::write(first.path().join("first.txt"), b"first")
            .await
            .unwrap();

        first.finalize().await.unwrap();

        assert_eq!(fs::read(destination.join("first.txt")).await.unwrap(), b"first");
        second.cleanup().await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn receive_staging_publishes_multiple_top_level_entries() {
        let root = scan_root("receive-multi-entry");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        let staging = ReceiveStaging::create(&destination).await.unwrap();
        fs::write(staging.path().join("first.txt"), b"first")
            .await
            .unwrap();
        fs::write(staging.path().join("second.txt"), b"second")
            .await
            .unwrap();

        staging.finalize().await.unwrap();

        assert_eq!(fs::read(destination.join("first.txt")).await.unwrap(), b"first");
        assert_eq!(fs::read(destination.join("second.txt")).await.unwrap(), b"second");
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn receive_staging_refuses_existing_output_without_overwrite() {
        let root = scan_root("receive-conflict");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        fs::create_dir_all(&destination).await.unwrap();
        fs::write(destination.join("file.txt"), b"user output")
            .await
            .unwrap();
        let staging = ReceiveStaging::create(&destination).await.unwrap();
        fs::write(staging.path().join("file.txt"), b"incoming output")
            .await
            .unwrap();

        assert!(matches!(
            staging.finalize().await,
            Err(ReceiveStagingError::Conflict)
        ));
        assert_eq!(
            fs::read(destination.join("file.txt")).await.unwrap(),
            b"user output"
        );
        staging.cleanup().await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn receive_staging_rejects_new_destination_output_outside_staging() {
        let root = scan_root("receive-containment");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        let staging = ReceiveStaging::create(&destination).await.unwrap();
        fs::write(destination.join("escape.txt"), b"unexpected")
            .await
            .unwrap();
        fs::write(staging.path().join("file.txt"), b"incoming")
            .await
            .unwrap();

        assert!(matches!(
            staging.finalize().await,
            Err(ReceiveStagingError::DestinationChanged)
        ));
        assert!(destination.join("escape.txt").exists());
        staging.cleanup().await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn receive_staging_rolls_back_when_late_conflict_is_found() {
        let root = scan_root("receive-late-conflict");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        fs::create_dir_all(&destination).await.unwrap();
        fs::write(destination.join("second.txt"), b"user output")
            .await
            .unwrap();
        let staging = ReceiveStaging::create(&destination).await.unwrap();
        fs::write(staging.path().join("first.txt"), b"incoming first")
            .await
            .unwrap();
        fs::write(staging.path().join("second.txt"), b"incoming second")
            .await
            .unwrap();

        assert!(matches!(
            staging.finalize().await,
            Err(ReceiveStagingError::Conflict)
        ));
        assert!(!destination.join("first.txt").exists());
        assert_eq!(
            fs::read(destination.join("second.txt")).await.unwrap(),
            b"user output"
        );
        assert!(staging.path().join("first.txt").exists());
        staging.cleanup().await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn receive_staging_does_not_replace_destination_symlink() {
        use std::os::unix::fs::symlink;

        let root = scan_root("receive-symlink-conflict");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        let outside = root.join("outside.txt");
        fs::write(&outside, b"user output").await.unwrap();
        fs::create_dir_all(&destination).await.unwrap();
        symlink(&outside, destination.join("file.txt")).unwrap();
        let staging = ReceiveStaging::create(&destination).await.unwrap();
        fs::write(staging.path().join("file.txt"), b"incoming output")
            .await
            .unwrap();

        assert!(matches!(
            staging.finalize().await,
            Err(ReceiveStagingError::Conflict)
        ));
        assert!(fs::symlink_metadata(destination.join("file.txt"))
            .await
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read(&outside).await.unwrap(), b"user output");
        staging.cleanup().await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn receive_staging_rejects_symlink_output() {
        use std::os::unix::fs::symlink;

        let root = scan_root("receive-symlink");
        fs::create_dir_all(&root).await.unwrap();
        let destination = root.join("received");
        let outside = root.join("outside.txt");
        fs::write(&outside, b"outside").await.unwrap();
        let staging = ReceiveStaging::create(&destination).await.unwrap();
        symlink(&outside, staging.path().join("escape.txt")).unwrap();

        assert!(matches!(
            staging.finalize().await,
            Err(ReceiveStagingError::SymlinkOutput)
        ));
        assert_eq!(fs::read(&outside).await.unwrap(), b"outside");
        staging.cleanup().await.unwrap();
        let _ = fs::remove_dir_all(root).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn receive_destination_rejects_final_symlink() {
        use std::os::unix::fs::symlink;

        let root = scan_root("receive-destination-symlink");
        let actual = root.join("actual");
        let selected = root.join("selected");
        fs::create_dir_all(&actual).await.unwrap();
        symlink(&actual, &selected).unwrap();

        assert_eq!(
            validate_receive_directory(&selected).await,
            Err(DestinationError::SymlinkNotAllowed)
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
    async fn rejects_directories_over_entry_bound_with_actionable_error() {
        let root = scan_root("too-many-entries");
        fs::create_dir_all(&root).await.unwrap();
        for index in 0..=MAX_SCAN_DIRECTORY_ENTRIES {
            fs::write(root.join(format!("file-{index}.txt")), b"data")
                .await
                .unwrap();
        }

        assert_eq!(
            scan_send_paths(vec![root.clone()], ScanCancellation::new()).await,
            Err(SourceScanError::TooManyEntries)
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

        let socket_path = std::env::temp_dir().join(format!("d-{}", TransferId::new()));
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
