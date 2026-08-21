use crate::progress::{accept_progress, eta_seconds};
use drift_core::{Progress, TransferCapability, TransferId, TransferManifest};
use std::{fmt, future::Future, path::PathBuf, pin::Pin};

use crate::RecoveryCandidate;

pub type SendFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub type SendEventFuture<'a> = Pin<Box<dyn Future<Output = Option<SendEvent>> + Send + 'a>>;

pub trait SendEventStream: Send {
    fn next(&mut self) -> SendEventFuture<'_>;
}

pub trait SendController: Send + Sync {
    fn recoveries(&self) -> SendFuture<Result<Vec<RecoveryCandidate>, SendCommandError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn choose(&self) -> SendFuture<Result<SendSelection, SendCommandError>> {
        Box::pin(async { Err(SendCommandError::selection_unavailable()) })
    }

    fn scan(&self, _paths: Vec<PathBuf>) -> SendFuture<Result<SendSelection, SendCommandError>> {
        Box::pin(async { Err(SendCommandError::scan_failed()) })
    }

    fn cancel_scan(&self) {}

    fn preflight(&self, paths: Vec<PathBuf>) -> SendFuture<Result<(), SendCommandError>>;

    fn start_send(
        &self,
        paths: Vec<PathBuf>,
        manifest: Option<TransferManifest>,
    ) -> SendFuture<Result<TransferId, SendCommandError>>;

    fn cancel(&self, transfer_id: TransferId) -> SendFuture<Result<(), SendCommandError>>;

    fn retry(&self, transfer_id: TransferId) -> SendFuture<Result<TransferId, SendCommandError>>;

    fn recover(&self, _transfer_id: TransferId) -> SendFuture<Result<TransferId, SendCommandError>> {
        Box::pin(async { Err(SendCommandError::start_failed()) })
    }

    fn discard_recovery(
        &self,
        _transfer_id: TransferId,
    ) -> SendFuture<Result<(), SendCommandError>> {
        Box::pin(async { Err(SendCommandError::start_failed()) })
    }

    fn subscribe(&self) -> Box<dyn SendEventStream>;
}

#[derive(Clone, PartialEq, Eq)]
pub struct SelectedItem {
    path: PathBuf,
    bytes: u64,
}

impl SelectedItem {
    pub fn new(path: impl Into<PathBuf>, bytes: u64) -> Result<Self, SelectionError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(SelectionError::EmptyPath);
        }
        Ok(Self { path, bytes })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl fmt::Debug for SelectedItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectedItem")
            .field("path_configured", &true)
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionError {
    EmptySelection,
    EmptyPath,
    SizeOverflow,
    InvalidManifest,
}

impl fmt::Display for SelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptySelection => "selection must contain at least one item",
            Self::EmptyPath => "selected path must not be empty",
            Self::SizeOverflow => "selected item size is too large",
            Self::InvalidManifest => "selected manifest is invalid",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for SelectionError {}

#[derive(Clone, PartialEq, Eq)]
pub struct SendSelection {
    items: Vec<SelectedItem>,
    total_bytes: u64,
    manifest: Option<TransferManifest>,
}

impl SendSelection {
    pub fn new(items: Vec<SelectedItem>) -> Result<Self, SelectionError> {
        if items.is_empty() {
            return Err(SelectionError::EmptySelection);
        }
        let total_bytes = items.iter().try_fold(0u64, |total, item| {
            total
                .checked_add(item.bytes)
                .ok_or(SelectionError::SizeOverflow)
        })?;
        Ok(Self {
            items,
            total_bytes,
            manifest: None,
        })
    }

    pub fn with_manifest(
        items: Vec<SelectedItem>,
        manifest: TransferManifest,
    ) -> Result<Self, SelectionError> {
        let mut selection = Self::new(items)?;
        manifest
            .validate()
            .map_err(|_| SelectionError::InvalidManifest)?;
        if selection.total_bytes != manifest.total_size {
            return Err(SelectionError::InvalidManifest);
        }
        selection.manifest = Some(manifest);
        Ok(selection)
    }

    pub fn items(&self) -> &[SelectedItem] {
        &self.items
    }

    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn file_count(&self) -> usize {
        self.manifest
            .as_ref()
            .map_or(self.items.len(), |manifest| manifest.files.len())
    }

    pub fn manifest(&self) -> Option<&TransferManifest> {
        self.manifest.as_ref()
    }

    fn into_paths(self) -> Vec<PathBuf> {
        self.items.into_iter().map(|item| item.path).collect()
    }

    fn remove(&mut self, index: usize) -> bool {
        if index >= self.items.len() {
            return false;
        }
        let removed = self.items.remove(index);
        if let Some(manifest) = self.manifest.as_mut() {
            let Some(root_name) = removed.path.file_name() else {
                self.manifest = None;
                self.recompute_total();
                return true;
            };
            manifest.files.retain(|file| {
                file.relative_path != root_name && !file.relative_path.starts_with(root_name)
            });
            manifest.total_size = manifest
                .files
                .iter()
                .map(|file| file.size)
                .fold(0_u64, u64::saturating_add);
        }
        self.recompute_total();
        true
    }

    fn recompute_total(&mut self) {
        self.total_bytes = self
            .items
            .iter()
            .map(|item| item.bytes)
            .fold(0u64, u64::saturating_add);
    }
}

impl fmt::Debug for SendSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendSelection")
            .field("item_count", &self.item_count())
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendProgress {
    pub transferred: u64,
    pub total: u64,
    pub speed_bps: u64,
}

impl SendProgress {
    pub fn eta_seconds(self) -> Option<u64> {
        eta_seconds(self.transferred, self.total, self.speed_bps)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendPhase {
    Empty,
    Choosing,
    Scanning,
    Preflighting,
    Ready,
    Starting,
    Connecting,
    Connected,
    Authenticating,
    Negotiating,
    Transferring,
    CodeReady,
    Verifying,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl SendPhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Empty => "No files selected",
            Self::Choosing => "Opening file picker",
            Self::Scanning => "Checking selected files",
            Self::Preflighting => "Checking Croc",
            Self::Ready => "Ready to send",
            Self::Starting => "Starting transfer",
            Self::Connecting => "Connecting",
            Self::Connected => "Connected",
            Self::Authenticating => "Authenticating",
            Self::Negotiating => "Negotiating",
            Self::Transferring => "Transferring",
            Self::CodeReady => "Code ready",
            Self::Verifying => "Verifying",
            Self::Cancelling => "Cancelling",
            Self::Completed => "Transfer complete",
            Self::Failed => "Transfer failed",
            Self::Cancelled => "Transfer cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFeedback {
    Succeeded,
    Failed,
}

#[derive(Clone, PartialEq, Eq)]
pub enum SendAction {
    Choose,
    RemoveSelection { index: usize },
    ClearSelection,
    Start,
    CopyCode,
    Cancel,
    Recover { transfer_id: TransferId },
    DiscardRecovery { transfer_id: TransferId },
}

impl fmt::Debug for SendAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Choose => formatter.write_str("Choose"),
            Self::RemoveSelection { index } => formatter
                .debug_struct("RemoveSelection")
                .field("index", index)
                .finish(),
            Self::ClearSelection => formatter.write_str("ClearSelection"),
            Self::Start => formatter.write_str("Start"),
            Self::CopyCode => formatter.write_str("CopyCode"),
            Self::Cancel => formatter.write_str("Cancel"),
            Self::Recover { transfer_id } => formatter
                .debug_struct("Recover")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::DiscardRecovery { transfer_id } => formatter
                .debug_struct("DiscardRecovery")
                .field("transfer_id", transfer_id)
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SendIntent {
    Choose,
    CancelScan,
    Preflight {
        generation: u64,
        paths: Vec<PathBuf>,
    },
    Start {
        paths: Vec<PathBuf>,
        manifest: Option<TransferManifest>,
    },
    CopyCode {
        code: String,
    },
    Cancel {
        transfer_id: TransferId,
    },
    Retry {
        transfer_id: TransferId,
    },
    Recover {
        transfer_id: TransferId,
    },
    DiscardRecovery {
        transfer_id: TransferId,
    },
}

impl fmt::Debug for SendIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Choose => formatter.write_str("Choose"),
            Self::CancelScan => formatter.write_str("CancelScan"),
            Self::Preflight { generation, paths } => formatter
                .debug_struct("Preflight")
                .field("generation", generation)
                .field("path_count", &paths.len())
                .finish(),
            Self::Start { paths, .. } => formatter
                .debug_struct("Start")
                .field("path_count", &paths.len())
                .finish(),
            Self::CopyCode { .. } => formatter
                .debug_struct("CopyCode")
                .field("code", &"[REDACTED]")
                .finish(),
            Self::Cancel { transfer_id } => formatter
                .debug_struct("Cancel")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Retry { transfer_id } => formatter
                .debug_struct("Retry")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Recover { transfer_id } => formatter
                .debug_struct("Recover")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::DiscardRecovery { transfer_id } => formatter
                .debug_struct("DiscardRecovery")
                .field("transfer_id", transfer_id)
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SendEvent {
    Created {
        transfer_id: TransferId,
    },
    Connecting {
        transfer_id: TransferId,
    },
    Connected {
        transfer_id: TransferId,
    },
    Authenticating {
        transfer_id: TransferId,
    },
    Negotiating {
        transfer_id: TransferId,
    },
    Started {
        transfer_id: TransferId,
    },
    CodeAvailable {
        transfer_id: TransferId,
        code: String,
    },
    Progress {
        transfer_id: TransferId,
        progress: SendProgress,
    },
    CapabilityUnavailable {
        transfer_id: TransferId,
        capability: TransferCapability,
    },
    Verifying {
        transfer_id: TransferId,
    },
    Completed {
        transfer_id: TransferId,
    },
    Failed {
        transfer_id: TransferId,
        message: String,
        retryable: bool,
    },
    Cancelled {
        transfer_id: TransferId,
    },
}

impl fmt::Debug for SendEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CodeAvailable { transfer_id, .. } => formatter
                .debug_struct("CodeAvailable")
                .field("transfer_id", transfer_id)
                .field("code", &"[REDACTED]")
                .finish(),
            Self::Failed {
                transfer_id,
                message,
                ..
            } => formatter
                .debug_struct("Failed")
                .field("transfer_id", transfer_id)
                .field("message", message)
                .finish(),
            Self::Created { transfer_id } => formatter
                .debug_struct("Created")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Connecting { transfer_id } => formatter
                .debug_struct("Connecting")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Connected { transfer_id } => formatter
                .debug_struct("Connected")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Authenticating { transfer_id } => formatter
                .debug_struct("Authenticating")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Negotiating { transfer_id } => formatter
                .debug_struct("Negotiating")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Started { transfer_id } => formatter
                .debug_struct("Started")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Progress {
                transfer_id,
                progress,
            } => formatter
                .debug_struct("Progress")
                .field("transfer_id", transfer_id)
                .field("progress", progress)
                .finish(),
            Self::CapabilityUnavailable {
                transfer_id,
                capability,
            } => formatter
                .debug_struct("CapabilityUnavailable")
                .field("transfer_id", transfer_id)
                .field("capability", capability)
                .finish(),
            Self::Verifying { transfer_id } => formatter
                .debug_struct("Verifying")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Completed { transfer_id } => formatter
                .debug_struct("Completed")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::Cancelled { transfer_id } => formatter
                .debug_struct("Cancelled")
                .field("transfer_id", transfer_id)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendCommandErrorKind {
    SelectionUnavailable,
    ScanFailed,
    SourceUnavailable,
    SourceUnreadable,
    SymlinkNotAllowed,
    UnsupportedFileType,
    EmptyDirectory,
    TooManyEntries,
    DuplicatePath,
    InvalidRelativePath,
    ScanCancelled,
    PreflightFailed,
    StartFailed,
    CancelFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendCommandError {
    kind: SendCommandErrorKind,
}

impl SendCommandError {
    pub fn new(kind: SendCommandErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(self) -> SendCommandErrorKind {
        self.kind
    }

    pub fn message(self) -> &'static str {
        match self.kind {
            SendCommandErrorKind::SelectionUnavailable => "File selection is unavailable.",
            SendCommandErrorKind::ScanFailed => "Selected files could not be checked.",
            SendCommandErrorKind::SourceUnavailable => "A selected source is unavailable.",
            SendCommandErrorKind::SourceUnreadable => "A selected source cannot be read.",
            SendCommandErrorKind::SymlinkNotAllowed => {
                "Symbolic links are not supported as send sources."
            }
            SendCommandErrorKind::UnsupportedFileType => {
                "A selected source is not a regular file or directory."
            }
            SendCommandErrorKind::EmptyDirectory => {
                "A selected directory contains no regular files."
            }
            SendCommandErrorKind::TooManyEntries => {
                "A selected directory contains too many entries."
            }
            SendCommandErrorKind::DuplicatePath => {
                "Selected sources contain duplicate output paths."
            }
            SendCommandErrorKind::InvalidRelativePath => {
                "A selected source has an invalid relative path."
            }
            SendCommandErrorKind::ScanCancelled => "Source checking was cancelled.",
            SendCommandErrorKind::PreflightFailed => "Croc is not ready.",
            SendCommandErrorKind::StartFailed => "The transfer could not start.",
            SendCommandErrorKind::CancelFailed => "The transfer could not be cancelled.",
        }
    }

    pub fn selection_unavailable() -> Self {
        Self::new(SendCommandErrorKind::SelectionUnavailable)
    }

    pub fn scan_failed() -> Self {
        Self::new(SendCommandErrorKind::ScanFailed)
    }

    pub fn preflight_failed() -> Self {
        Self::new(SendCommandErrorKind::PreflightFailed)
    }

    pub fn start_failed() -> Self {
        Self::new(SendCommandErrorKind::StartFailed)
    }

    pub fn cancel_failed() -> Self {
        Self::new(SendCommandErrorKind::CancelFailed)
    }
}

impl fmt::Display for SendCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for SendCommandError {}

#[derive(Clone, PartialEq, Eq)]
pub struct SendViewState {
    selection: Option<SendSelection>,
    phase: SendPhase,
    active_transfer_id: Option<TransferId>,
    code: Option<String>,
    progress: Option<SendProgress>,
    progress_available: bool,
    copy_feedback: Option<CopyFeedback>,
    error: Option<String>,
    selection_generation: u64,
    retry_transfer_id: Option<TransferId>,
    retryable: bool,
}

impl Default for SendViewState {
    fn default() -> Self {
        Self::new()
    }
}

impl SendViewState {
    pub fn new() -> Self {
        Self {
            selection: None,
            phase: SendPhase::Empty,
            active_transfer_id: None,
            code: None,
            progress: None,
            progress_available: true,
            copy_feedback: None,
            error: None,
            selection_generation: 0,
            retry_transfer_id: None,
            retryable: false,
        }
    }

    pub fn phase(&self) -> SendPhase {
        self.phase
    }

    pub fn selection(&self) -> Option<&SendSelection> {
        self.selection.as_ref()
    }

    pub fn active_transfer_id(&self) -> Option<TransferId> {
        self.active_transfer_id
    }

    pub fn transfer_code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    pub fn progress(&self) -> Option<SendProgress> {
        self.progress
    }

    pub fn progress_speed_bps(&self) -> Option<u64> {
        if !self.progress_available || !self.progress_is_active() {
            return None;
        }
        self.progress
            .filter(|progress| progress.speed_bps > 0)
            .map(|progress| progress.speed_bps)
    }

    pub fn progress_eta_seconds(&self) -> Option<u64> {
        if !self.progress_available || !self.progress_is_active() {
            return None;
        }
        self.progress.and_then(SendProgress::eta_seconds)
    }

    pub fn progress_available(&self) -> bool {
        self.progress_available
    }

    pub fn copy_feedback(&self) -> Option<CopyFeedback> {
        self.copy_feedback
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn retry_enabled(&self) -> bool {
        self.phase == SendPhase::Failed && self.retryable && self.retry_transfer_id.is_some()
    }

    pub fn recovery_enabled(&self) -> bool {
        self.active_transfer_id.is_none()
            && matches!(self.phase, SendPhase::Empty | SendPhase::Ready | SendPhase::Failed)
    }

    pub fn start_enabled(&self) -> bool {
        (matches!(self.phase, SendPhase::Ready)
            && self
                .selection
                .as_ref()
                .is_some_and(|selection| selection.manifest().is_some()))
            || (self.phase == SendPhase::Failed
                && (self.retry_transfer_id.is_none() || self.retryable)
                && self
                    .selection
                    .as_ref()
                    .is_some_and(|selection| selection.manifest().is_some()))
    }

    pub fn clear_enabled(&self) -> bool {
        matches!(
            self.phase,
            SendPhase::Scanning | SendPhase::Preflighting | SendPhase::Ready | SendPhase::Failed
        ) && (self.selection.is_some() || self.phase == SendPhase::Scanning)
    }

    pub fn choose_enabled(&self) -> bool {
        matches!(
            self.phase,
            SendPhase::Empty
                | SendPhase::Ready
                | SendPhase::Completed
                | SendPhase::Failed
                | SendPhase::Cancelled
        )
    }

    pub fn cancel_enabled(&self) -> bool {
        self.active_transfer_id.is_some()
            && matches!(
                self.phase,
                SendPhase::Starting
                    | SendPhase::Connecting
                    | SendPhase::Connected
                    | SendPhase::Authenticating
                    | SendPhase::Negotiating
                    | SendPhase::Transferring
                    | SendPhase::CodeReady
                    | SendPhase::Verifying
            )
    }

    pub fn set_selection(&mut self, selection: SendSelection) -> SendIntent {
        self.selection = Some(selection);
        self.selection_generation = self.selection_generation.saturating_add(1);
        self.active_transfer_id = None;
        self.code = None;
        self.progress = None;
        self.progress_available = true;
        self.copy_feedback = None;
        self.error = None;
        self.retry_transfer_id = None;
        self.retryable = false;
        self.phase = SendPhase::Preflighting;
        self.preflight_intent()
    }

    pub fn begin_scan(&mut self) -> Option<u64> {
        if !self.choose_enabled()
            && !matches!(
                self.phase,
                SendPhase::Choosing
                    | SendPhase::Scanning
                    | SendPhase::Preflighting
                    | SendPhase::Ready
            )
        {
            return None;
        }
        self.selection_generation = self.selection_generation.saturating_add(1);
        self.selection = None;
        self.active_transfer_id = None;
        self.code = None;
        self.progress = None;
        self.progress_available = true;
        self.copy_feedback = None;
        self.error = None;
        self.retry_transfer_id = None;
        self.retryable = false;
        self.phase = SendPhase::Scanning;
        Some(self.selection_generation)
    }

    pub fn apply_scan_result(
        &mut self,
        generation: u64,
        selection: SendSelection,
    ) -> Option<SendIntent> {
        if generation != self.selection_generation || self.phase != SendPhase::Scanning {
            return None;
        }
        self.selection = Some(selection);
        self.phase = SendPhase::Preflighting;
        Some(self.preflight_intent())
    }

    pub fn mark_scan_failed(&mut self, generation: u64, error: SendCommandError) {
        if generation == self.selection_generation && self.phase == SendPhase::Scanning {
            self.phase = SendPhase::Failed;
            self.error = Some(error.to_string());
        }
    }

    pub fn handle_action(&mut self, action: SendAction) -> Option<SendIntent> {
        match action {
            SendAction::Choose if self.choose_enabled() => {
                self.selection = None;
                self.active_transfer_id = None;
                self.code = None;
                self.progress = None;
                self.copy_feedback = None;
                self.error = None;
                self.retry_transfer_id = None;
                self.retryable = false;
                self.phase = SendPhase::Choosing;
                Some(SendIntent::Choose)
            }
            SendAction::ClearSelection if self.clear_enabled() => {
                self.selection_generation = self.selection_generation.saturating_add(1);
                self.selection = None;
                self.active_transfer_id = None;
                self.code = None;
                self.progress = None;
                self.copy_feedback = None;
                self.error = None;
                self.retry_transfer_id = None;
                self.retryable = false;
                self.phase = SendPhase::Empty;
                Some(SendIntent::CancelScan)
            }
            SendAction::RemoveSelection { index }
                if matches!(
                    self.phase,
                    SendPhase::Preflighting | SendPhase::Ready | SendPhase::Failed
                ) =>
            {
                let Some(selection) = self.selection.as_mut() else {
                    return None;
                };
                if !selection.remove(index) {
                    return None;
                }
                self.selection_generation = self.selection_generation.saturating_add(1);
                self.copy_feedback = None;
                self.error = None;
                self.retry_transfer_id = None;
                self.retryable = false;
                if selection.items.is_empty() {
                    self.selection = None;
                    self.phase = SendPhase::Empty;
                    None
                } else {
                    self.phase = SendPhase::Preflighting;
                    Some(self.preflight_intent())
                }
            }
            SendAction::Start if self.start_enabled() => {
                if self.phase == SendPhase::Failed {
                    if self.retry_enabled() {
                        let transfer_id = self.retry_transfer_id?;
                        self.phase = SendPhase::Starting;
                        self.progress = None;
                        self.progress_available = true;
                        self.error = None;
                        self.retry_transfer_id = None;
                        self.retryable = false;
                        return Some(SendIntent::Retry { transfer_id });
                    }
                    self.phase = SendPhase::Preflighting;
                    self.progress = None;
                    self.progress_available = true;
                    self.error = None;
                    return Some(self.preflight_intent());
                }
                let paths = self
                    .selection
                    .as_ref()
                    .map(|selection| selection.clone().into_paths())?;
                self.phase = SendPhase::Starting;
                self.progress = None;
                self.progress_available = true;
                self.error = None;
                self.retry_transfer_id = None;
                self.retryable = false;
                let manifest = self
                    .selection
                    .as_ref()
                    .and_then(|selection| selection.manifest().cloned());
                Some(SendIntent::Start { paths, manifest })
            }
            SendAction::Recover { transfer_id } if self.recovery_enabled() => {
                self.phase = SendPhase::Starting;
                self.error = None;
                self.progress = None;
                Some(SendIntent::Recover { transfer_id })
            }
            SendAction::DiscardRecovery { transfer_id } if self.active_transfer_id.is_none() => {
                Some(SendIntent::DiscardRecovery { transfer_id })
            }
            SendAction::CopyCode
                if matches!(self.phase, SendPhase::CodeReady | SendPhase::Transferring) =>
            {
                self.code
                    .as_ref()
                    .cloned()
                    .map(|code| SendIntent::CopyCode { code })
            }
            SendAction::Cancel if self.cancel_enabled() => {
                let transfer_id = self.active_transfer_id?;
                self.phase = SendPhase::Cancelling;
                Some(SendIntent::Cancel { transfer_id })
            }
            _ => None,
        }
    }

    pub fn mark_preflight_succeeded(&mut self, generation: u64) {
        if generation == self.selection_generation
            && self.phase == SendPhase::Preflighting
            && self.selection.is_some()
        {
            self.phase = SendPhase::Ready;
            self.error = None;
        }
    }

    pub fn mark_preflight_failed(&mut self, generation: u64) {
        if generation == self.selection_generation && self.phase == SendPhase::Preflighting {
            self.phase = SendPhase::Failed;
            self.error = Some(SendCommandError::preflight_failed().message().to_owned());
        }
    }

    pub fn mark_choose_failed(&mut self) {
        if self.phase == SendPhase::Choosing {
            self.phase = SendPhase::Failed;
            self.error = Some(
                SendCommandError::selection_unavailable()
                    .message()
                    .to_owned(),
            );
        }
    }

    pub fn cancel_choose(&mut self) {
        if self.phase == SendPhase::Choosing {
            self.phase = SendPhase::Empty;
            self.error = None;
        }
    }

    pub fn mark_start_succeeded(&mut self, transfer_id: TransferId) {
        if self.phase == SendPhase::Starting && self.active_transfer_id.is_none() {
            self.active_transfer_id = Some(transfer_id);
        }
    }

    pub fn mark_start_failed(&mut self) {
        if self.phase == SendPhase::Starting {
            self.phase = SendPhase::Failed;
            self.active_transfer_id = None;
            self.code = None;
            self.retry_transfer_id = None;
            self.retryable = false;
            self.error = Some(SendCommandError::start_failed().message().to_owned());
        }
    }

    pub fn mark_cancel_failed(&mut self) {
        if self.phase == SendPhase::Cancelling {
            self.phase = if self.code.is_some() {
                SendPhase::CodeReady
            } else {
                SendPhase::Transferring
            };
            self.error = Some(SendCommandError::cancel_failed().message().to_owned());
        }
    }

    pub fn mark_copy_result(&mut self, result: Result<(), ()>) {
        self.copy_feedback = Some(if result.is_ok() {
            CopyFeedback::Succeeded
        } else {
            CopyFeedback::Failed
        });
    }

    pub fn apply_event(&mut self, event: SendEvent) {
        match event {
            SendEvent::Created { transfer_id } => {
                if self.phase == SendPhase::Starting && self.active_transfer_id.is_none() {
                    self.active_transfer_id = Some(transfer_id);
                    self.progress = None;
                    self.progress_available = true;
                    self.retry_transfer_id = None;
                    self.retryable = false;
                }
            }
            SendEvent::Connecting { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Connecting;
                }
            }
            SendEvent::Connected { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Connected;
                }
            }
            SendEvent::Authenticating { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Authenticating;
                }
            }
            SendEvent::Negotiating { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Negotiating;
                }
            }
            SendEvent::Started { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Transferring;
                }
            }
            SendEvent::CodeAvailable { transfer_id, code } => {
                if self.accepts_transfer(transfer_id) {
                    self.code = Some(code);
                    if self.phase != SendPhase::Transferring {
                        self.phase = SendPhase::CodeReady;
                    }
                    self.copy_feedback = None;
                }
            }
            SendEvent::Progress {
                transfer_id,
                progress,
            } => {
                if self.accepts_transfer(transfer_id)
                    && matches!(self.phase, SendPhase::Transferring | SendPhase::CodeReady)
                {
                    let previous = self.progress.map(|current| {
                        Progress {
                            transferred_bytes: current.transferred,
                            total_bytes: current.total,
                            speed_bps: current.speed_bps,
                        }
                    });
                    let Some(progress) = accept_progress(
                        previous,
                        progress.transferred,
                        progress.total,
                        progress.speed_bps,
                    ) else {
                        return;
                    };
                    let progress = SendProgress {
                        transferred: progress.transferred_bytes,
                        total: progress.total_bytes,
                        speed_bps: progress.speed_bps,
                    };
                    self.progress = Some(progress);
                    self.phase = SendPhase::Transferring;
                }
            }
            SendEvent::CapabilityUnavailable {
                transfer_id,
                capability: TransferCapability::Progress,
            } => {
                if self.accepts_transfer(transfer_id) {
                    self.progress_available = false;
                }
            }
            SendEvent::CapabilityUnavailable {
                capability: TransferCapability::Pause | TransferCapability::Resume,
                ..
            } => {}
            SendEvent::Verifying { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Verifying;
                }
            }
            SendEvent::Completed { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Completed;
                    self.active_transfer_id = None;
                    self.code = None;
                    self.retry_transfer_id = None;
                    self.retryable = false;
                }
            }
            SendEvent::Failed {
                transfer_id,
                message,
                retryable,
            } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Failed;
                    self.active_transfer_id = None;
                    self.code = None;
                    self.retry_transfer_id = Some(transfer_id);
                    self.retryable = retryable;
                    self.error = Some(message);
                }
            }
            SendEvent::Cancelled { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = SendPhase::Cancelled;
                    self.active_transfer_id = None;
                    self.code = None;
                    self.retry_transfer_id = None;
                    self.retryable = false;
                }
            }
        }
    }

    fn preflight_intent(&self) -> SendIntent {
        SendIntent::Preflight {
            generation: self.selection_generation,
            paths: self
                .selection
                .as_ref()
                .map(|selection| selection.clone().into_paths())
                .unwrap_or_default(),
        }
    }

    fn accepts_transfer(&self, transfer_id: TransferId) -> bool {
        self.active_transfer_id == Some(transfer_id)
    }

    fn progress_is_active(&self) -> bool {
        matches!(self.phase, SendPhase::Transferring | SendPhase::CodeReady)
    }
}

impl fmt::Debug for SendViewState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendViewState")
            .field("selection", &self.selection)
            .field("phase", &self.phase)
            .field("active_transfer_id", &self.active_transfer_id)
            .field("code", &self.code.as_ref().map(|_| "[REDACTED]"))
            .field("progress", &self.progress)
            .field("progress_available", &self.progress_available)
            .field("copy_feedback", &self.copy_feedback)
            .field("error", &self.error)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use drift_core::FileEntry;

    fn selection() -> SendSelection {
        let manifest = TransferManifest::new(
            TransferId::new(),
            vec![
                FileEntry::new("first.txt", 12).unwrap(),
                FileEntry::new("second.txt", 30).unwrap(),
            ],
        )
        .unwrap();
        SendSelection::with_manifest(
            vec![
                SelectedItem::new("first.txt", 12).unwrap(),
                SelectedItem::new("second.txt", 30).unwrap(),
            ],
            manifest,
        )
        .unwrap()
    }

    fn scanned_selection() -> SendSelection {
        let manifest = TransferManifest::new(
            TransferId::new(),
            vec![
                FileEntry::new("folder/first.txt", 12).unwrap(),
                FileEntry::new("folder/second.txt", 30).unwrap(),
            ],
        )
        .unwrap();
        SendSelection::with_manifest(
            vec![SelectedItem::new("/tmp/folder", 42).unwrap()],
            manifest,
        )
        .unwrap()
    }

    #[test]
    fn empty_send_state_requires_selection_before_start() {
        let mut state = SendViewState::new();

        assert_eq!(state.phase(), SendPhase::Empty);
        assert!(!state.start_enabled());
        assert_eq!(state.handle_action(SendAction::Start), None);
        assert_eq!(
            state.handle_action(SendAction::Choose),
            Some(SendIntent::Choose)
        );
        assert_eq!(state.phase(), SendPhase::Choosing);
    }

    #[test]
    fn cancelling_file_picker_returns_to_empty_state() {
        let mut state = SendViewState::new();
        assert_eq!(
            state.handle_action(SendAction::Choose),
            Some(SendIntent::Choose)
        );

        state.cancel_choose();

        assert_eq!(state.phase(), SendPhase::Empty);
        assert!(state.choose_enabled());
        assert_eq!(state.error(), None);
    }

    #[test]
    fn selected_items_require_preflight_before_start_and_reject_duplicates() {
        let mut state = SendViewState::new();
        let intent = state.set_selection(selection());
        let generation = match intent {
            SendIntent::Preflight { generation, paths } => {
                assert_eq!(paths.len(), 2);
                generation
            }
            other => panic!("unexpected intent: {other:?}"),
        };
        assert!(!state.start_enabled());

        state.mark_preflight_succeeded(generation);
        let intent = state.handle_action(SendAction::Start).unwrap();
        assert!(matches!(intent, SendIntent::Start { .. }));
        assert!(!state.start_enabled());
        assert_eq!(state.handle_action(SendAction::Start), None);
    }

    #[test]
    fn unscanned_selection_cannot_start_transfer() {
        let mut state = SendViewState::new();
        let selection = SendSelection::new(vec![SelectedItem::new("file.txt", 4).unwrap()])
            .unwrap();
        let generation = match state.set_selection(selection) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };

        state.mark_preflight_succeeded(generation);

        assert!(!state.start_enabled());
        assert_eq!(state.handle_action(SendAction::Start), None);
    }

    #[test]
    fn send_events_show_code_then_clear_it_at_completion() {
        let mut state = SendViewState::new();
        let generation = match state.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);
        state.handle_action(SendAction::Start);
        let transfer_id = TransferId::new();
        state.apply_event(SendEvent::Created { transfer_id });
        state.apply_event(SendEvent::Connecting { transfer_id });
        state.apply_event(SendEvent::Connected { transfer_id });
        state.apply_event(SendEvent::Authenticating { transfer_id });
        state.apply_event(SendEvent::Negotiating { transfer_id });
        state.apply_event(SendEvent::Started { transfer_id });
        state.apply_event(SendEvent::CodeAvailable {
            transfer_id,
            code: "secret-code".into(),
        });

        assert_eq!(state.phase(), SendPhase::Transferring);
        assert_eq!(state.transfer_code(), Some("secret-code"));
        assert_eq!(
            format!("{state:?}").contains("secret-code"),
            false,
            "state debug must not expose transfer code"
        );
        assert_eq!(
            format!(
                "{:?}",
                SendIntent::CopyCode {
                    code: "secret-code".into()
                }
            )
            .contains("secret-code"),
            false
        );

        state.apply_event(SendEvent::Completed { transfer_id });
        assert_eq!(state.phase(), SendPhase::Completed);
        assert_eq!(state.transfer_code(), None);
        assert_eq!(state.active_transfer_id(), None);
    }

    #[test]
    fn sender_progress_is_monotonic_and_eta_is_hidden_during_verification() {
        let mut state = SendViewState::new();
        let generation = match state.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);
        state.handle_action(SendAction::Start);
        let transfer_id = TransferId::new();
        state.apply_event(SendEvent::Created { transfer_id });
        state.apply_event(SendEvent::Started { transfer_id });
        state.apply_event(SendEvent::Progress {
            transfer_id,
            progress: SendProgress {
                transferred: 25,
                total: 100,
                speed_bps: 25,
            },
        });

        assert_eq!(state.progress_speed_bps(), Some(25));
        assert_eq!(state.progress_eta_seconds(), Some(3));

        state.apply_event(SendEvent::Progress {
            transfer_id,
            progress: SendProgress {
                transferred: 10,
                total: 100,
                speed_bps: 10,
            },
        });
        assert_eq!(state.progress().map(|progress| progress.transferred), Some(25));

        state.apply_event(SendEvent::Verifying { transfer_id });
        assert_eq!(state.progress_eta_seconds(), None);
        assert_eq!(state.progress_speed_bps(), None);
        state.apply_event(SendEvent::Progress {
            transfer_id,
            progress: SendProgress {
                transferred: 50,
                total: 100,
                speed_bps: 25,
            },
        });
        assert_eq!(state.phase(), SendPhase::Verifying);
        assert_eq!(state.progress().map(|progress| progress.transferred), Some(25));
    }

    #[test]
    fn failed_and_cancelled_events_leave_no_active_code() {
        let mut failed = SendViewState::new();
        let generation = match failed.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        failed.mark_preflight_succeeded(generation);
        failed.handle_action(SendAction::Start);
        let transfer_id = TransferId::new();
        failed.apply_event(SendEvent::Created { transfer_id });
        failed.apply_event(SendEvent::CodeAvailable {
            transfer_id,
            code: "secret-code".into(),
        });
        failed.apply_event(SendEvent::Failed {
            transfer_id,
            message: "The transfer could not start.".into(),
            retryable: false,
        });
        assert_eq!(failed.phase(), SendPhase::Failed);
        assert_eq!(failed.active_transfer_id(), None);
        assert_eq!(failed.transfer_code(), None);
        assert_eq!(failed.error(), Some("The transfer could not start."));

        let mut cancelled = SendViewState::new();
        let generation = match cancelled.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        cancelled.mark_preflight_succeeded(generation);
        cancelled.handle_action(SendAction::Start);
        let transfer_id = TransferId::new();
        cancelled.apply_event(SendEvent::Created { transfer_id });
        cancelled.apply_event(SendEvent::Started { transfer_id });
        assert!(matches!(
            cancelled.handle_action(SendAction::Cancel),
            Some(SendIntent::Cancel { .. })
        ));
        cancelled.apply_event(SendEvent::Cancelled { transfer_id });
        assert_eq!(cancelled.phase(), SendPhase::Cancelled);
        assert_eq!(cancelled.active_transfer_id(), None);
    }

    #[test]
    fn retryable_failure_starts_distinct_attempt_without_rebuilding_selection() {
        let mut state = SendViewState::new();
        let generation = match state.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);
        state.handle_action(SendAction::Start);
        let old_transfer_id = TransferId::new();
        state.apply_event(SendEvent::Created {
            transfer_id: old_transfer_id,
        });
        state.apply_event(SendEvent::Failed {
            transfer_id: old_transfer_id,
            message: "The transfer failed.".into(),
            retryable: true,
        });

        assert!(state.retry_enabled());
        assert!(matches!(
            state.handle_action(SendAction::Start),
            Some(SendIntent::Retry { transfer_id }) if transfer_id == old_transfer_id
        ));
        let new_transfer_id = TransferId::new();
        state.apply_event(SendEvent::Created {
            transfer_id: new_transfer_id,
        });
        assert_eq!(state.active_transfer_id(), Some(new_transfer_id));
        assert!(!state.retry_enabled());
        assert_eq!(state.selection().unwrap().item_count(), 2);
    }

    #[test]
    fn non_retryable_failure_disables_automatic_retry_but_keeps_selection() {
        let mut state = SendViewState::new();
        let generation = match state.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);
        state.handle_action(SendAction::Start);
        let transfer_id = TransferId::new();
        state.apply_event(SendEvent::Created { transfer_id });
        state.apply_event(SendEvent::Failed {
            transfer_id,
            message: "The request is invalid.".into(),
            retryable: false,
        });

        assert!(!state.retry_enabled());
        assert!(!state.start_enabled());
        assert!(state.choose_enabled());
        assert_eq!(state.selection().unwrap().item_count(), 2);
    }

    #[test]
    fn recovery_starts_without_rebuilding_sender_selection() {
        let mut state = SendViewState::new();
        let old_transfer_id = TransferId::new();
        assert!(state.recovery_enabled());
        assert!(matches!(
            state.handle_action(SendAction::Recover {
                transfer_id: old_transfer_id,
            }),
            Some(SendIntent::Recover { transfer_id }) if transfer_id == old_transfer_id
        ));
        let new_transfer_id = TransferId::new();
        state.apply_event(SendEvent::Created {
            transfer_id: new_transfer_id,
        });
        assert_eq!(state.active_transfer_id(), Some(new_transfer_id));
        assert_eq!(
            state.handle_action(SendAction::DiscardRecovery {
                transfer_id: old_transfer_id,
            }),
            None
        );
        state.apply_event(SendEvent::Cancelled {
            transfer_id: new_transfer_id,
        });
        assert_eq!(
            state.handle_action(SendAction::DiscardRecovery {
                transfer_id: old_transfer_id,
            }),
            Some(SendIntent::DiscardRecovery {
                transfer_id: old_transfer_id,
            })
        );
    }

    #[test]
    fn removing_selection_recomputes_manifest_summary_and_rechecks_preflight() {
        let mut state = SendViewState::new();
        let generation = match state.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);

        let intent = state
            .handle_action(SendAction::RemoveSelection { index: 0 })
            .unwrap();
        assert!(matches!(intent, SendIntent::Preflight { .. }));
        assert_eq!(state.selection().unwrap().item_count(), 1);
        assert_eq!(state.selection().unwrap().total_bytes(), 30);
        assert_eq!(state.phase(), SendPhase::Preflighting);
        assert!(!state.start_enabled());
    }

    #[test]
    fn scan_result_uses_manifest_totals_and_ignores_stale_results() {
        let mut state = SendViewState::new();
        let generation = state.begin_scan().unwrap();
        assert_eq!(state.phase(), SendPhase::Scanning);
        assert!(state.selection().is_none());
        assert!(state
            .apply_scan_result(generation.saturating_sub(1), scanned_selection())
            .is_none());
        assert_eq!(state.phase(), SendPhase::Scanning);

        let intent = state
            .apply_scan_result(generation, scanned_selection())
            .unwrap();
        assert!(matches!(intent, SendIntent::Preflight { .. }));
        assert_eq!(state.selection().unwrap().file_count(), 2);
        assert_eq!(state.selection().unwrap().total_bytes(), 42);
    }

    #[test]
    fn clear_selection_cancels_scan_and_locks_during_transfer() {
        let mut state = SendViewState::new();
        state.begin_scan();
        assert_eq!(
            state.handle_action(SendAction::ClearSelection),
            Some(SendIntent::CancelScan)
        );
        assert_eq!(state.phase(), SendPhase::Empty);

        let generation = match state.set_selection(selection()) {
            SendIntent::Preflight { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);
        state.handle_action(SendAction::Start);
        assert!(!state.clear_enabled());
        assert_eq!(state.handle_action(SendAction::ClearSelection), None);
    }
}
