use async_trait::async_trait;
use drift_core::TransferFailureKind;
use std::{fmt, future::Future, io, path::PathBuf, pin::Pin, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("invalid backend request: {0}")]
    InvalidRequest(BackendRequestError),
    #[error("backend is unavailable: {reason}")]
    Unavailable { reason: BackendUnavailableReason },
    #[error("backend I/O failed")]
    Io(#[source] io::Error),
    #[error("backend task failed")]
    Task(#[source] tokio::task::JoinError),
    #[error("backend operation timed out after {timeout:?}")]
    Timeout { timeout: Duration },
    #[error("backend operation cancelled")]
    Cancelled,
    #[error("backend version {found} is unsupported; expected {supported}")]
    IncompatibleVersion {
        found: String,
        supported: &'static str,
    },
    #[error("backend protocol failure: {reason}")]
    Protocol { reason: BackendProtocolError },
    #[error("backend operation failed: {reason}")]
    OperationFailed { reason: BackendOperationError },
}

impl BackendError {
    pub fn failure_kind(&self) -> TransferFailureKind {
        match self {
            Self::InvalidRequest(_) => TransferFailureKind::InvalidRequest,
            Self::Unavailable { reason } => match reason {
                BackendUnavailableReason::DependencyMissing => TransferFailureKind::Filesystem,
                BackendUnavailableReason::NotImplemented
                | BackendUnavailableReason::NotConfigured
                | BackendUnavailableReason::UnsupportedPlatform => TransferFailureKind::Unknown,
            },
            Self::Io(_) | Self::Task(_) => TransferFailureKind::ProcessInterruption,
            Self::Timeout { .. } => TransferFailureKind::Network,
            Self::Cancelled => TransferFailureKind::Unknown,
            Self::IncompatibleVersion { .. }
            | Self::Protocol { .. }
            | Self::OperationFailed { .. } => TransferFailureKind::ProcessFailure,
        }
    }

    pub fn safe_message(&self) -> String {
        match self {
            Self::InvalidRequest(reason) => reason.safe_message().into(),
            Self::Unavailable { reason } => reason.safe_message().into(),
            Self::Io(_) => "backend I/O failed".into(),
            Self::Task(_) => "backend task failed".into(),
            Self::Timeout { .. } => "backend operation timed out".into(),
            Self::Cancelled => "backend operation cancelled".into(),
            Self::IncompatibleVersion { .. } => "backend version is unsupported".into(),
            Self::Protocol { reason } => reason.safe_message().into(),
            Self::OperationFailed { reason } => reason.safe_message().into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendRequestError {
    EmptyPaths,
    EmptyCode,
    EmptyOutputDirectory,
}

impl BackendRequestError {
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::EmptyPaths => "send request must contain at least one path",
            Self::EmptyCode => "receive request code must not be empty",
            Self::EmptyOutputDirectory => "receive request output directory must not be empty",
        }
    }
}

impl fmt::Display for BackendRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendProtocolError {
    VersionCheckFailed,
    UnrecognizedVersion,
    UnsupportedVersion,
    MalformedMessage,
    MissingRequiredSignal,
    ResourceLimit,
    InvalidState,
}

impl BackendProtocolError {
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::VersionCheckFailed => "backend version check failed",
            Self::UnrecognizedVersion => "backend version was not recognized",
            Self::UnsupportedVersion => "backend protocol version is unsupported",
            Self::MalformedMessage => "backend protocol response was not recognized",
            Self::MissingRequiredSignal => "backend did not provide a required signal",
            Self::ResourceLimit => "backend protocol resource limit was exceeded",
            Self::InvalidState => "backend protocol state was invalid",
        }
    }
}

impl fmt::Display for BackendProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendOperationError {
    StartupFailed,
    ExecutionFailed,
    ResourceLimit,
    Internal,
}

impl BackendOperationError {
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::StartupFailed => "backend could not start",
            Self::ExecutionFailed => "backend operation failed",
            Self::ResourceLimit => "backend output exceeded its limit",
            Self::Internal => "backend operation failed internally",
        }
    }
}

impl fmt::Display for BackendOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendUnavailableReason {
    DependencyMissing,
    NotImplemented,
    NotConfigured,
    UnsupportedPlatform,
}

impl BackendUnavailableReason {
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::DependencyMissing => "backend dependency is unavailable",
            Self::NotImplemented => "backend is not implemented",
            Self::NotConfigured => "backend is not configured",
            Self::UnsupportedPlatform => "backend is unavailable on this platform",
        }
    }
}

impl fmt::Display for BackendUnavailableReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DependencyMissing => "dependency missing",
            Self::NotImplemented => "not implemented",
            Self::NotConfigured => "not configured",
            Self::UnsupportedPlatform => "unsupported platform",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendAvailability {
    Ready,
    Unavailable { reason: BackendUnavailableReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendInfo {
    pub name: &'static str,
    pub version: Option<&'static str>,
    pub capabilities: BackendCapabilities,
    pub availability: BackendAvailability,
}

impl BackendInfo {
    pub const fn new(
        name: &'static str,
        version: Option<&'static str>,
        capabilities: BackendCapabilities,
        availability: BackendAvailability,
    ) -> Self {
        Self {
            name,
            version,
            capabilities,
            availability,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendCapability {
    Progress,
    Pause,
    Resume,
    Direct,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BackendCapabilities {
    progress: bool,
    pause: bool,
    resume: bool,
    direct: bool,
    relay: bool,
}

impl BackendCapabilities {
    pub const fn new(progress: bool, pause: bool, resume: bool) -> Self {
        Self {
            progress,
            pause,
            resume,
            direct: false,
            relay: false,
        }
    }

    pub const fn with_connection_modes(mut self, direct: bool, relay: bool) -> Self {
        self.direct = direct;
        self.relay = relay;
        self
    }

    pub const fn supports(self, capability: BackendCapability) -> bool {
        match capability {
            BackendCapability::Progress => self.progress,
            BackendCapability::Pause => self.pause,
            BackendCapability::Resume => self.resume,
            BackendCapability::Direct => self.direct,
            BackendCapability::Relay => self.relay,
        }
    }
}

impl fmt::Display for BackendCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Progress => formatter.write_str("progress reporting"),
            Self::Pause => formatter.write_str("pause"),
            Self::Resume => formatter.write_str("resume"),
            Self::Direct => formatter.write_str("direct connections"),
            Self::Relay => formatter.write_str("relay connections"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendControlResult {
    Confirmed,
    Unsupported,
    Terminal,
}

pub type BackendCancellation = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[derive(Clone, PartialEq, Eq)]
pub enum BackendEvent {
    CodeGenerated {
        code: String,
    },
    MetadataReady,
    Progress {
        transferred: u64,
        total: u64,
        speed_bps: u64,
    },
    CapabilityUnavailable {
        capability: BackendCapability,
    },
}

impl fmt::Debug for BackendEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CodeGenerated { .. } => formatter
                .debug_struct("BackendEvent::CodeGenerated")
                .field("code", &"[REDACTED]")
                .finish(),
            Self::MetadataReady => formatter.write_str("BackendEvent::MetadataReady"),
            Self::Progress {
                transferred,
                total,
                speed_bps,
            } => formatter
                .debug_struct("BackendEvent::Progress")
                .field("transferred", transferred)
                .field("total", total)
                .field("speed_bps", speed_bps)
                .finish(),
            Self::CapabilityUnavailable { capability } => formatter
                .debug_struct("BackendEvent::CapabilityUnavailable")
                .field("capability", capability)
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SendRequest {
    pub paths: Vec<PathBuf>,
    pub relay: Option<String>,
}

impl SendRequest {
    pub fn new(paths: Vec<PathBuf>) -> Result<Self, BackendError> {
        if paths.is_empty() {
            return Err(BackendError::InvalidRequest(
                BackendRequestError::EmptyPaths,
            ));
        }
        Ok(Self { paths, relay: None })
    }

    pub fn with_relay(mut self, relay: impl Into<String>) -> Self {
        self.relay = Some(relay.into());
        self
    }
}

impl fmt::Debug for SendRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendRequest")
            .field("path_count", &self.paths.len())
            .field("relay", &self.relay)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ReceiveRequest {
    pub code: String,
    pub output_directory: PathBuf,
    pub relay: Option<String>,
}

impl ReceiveRequest {
    pub fn new(
        code: impl Into<String>,
        output_directory: impl Into<PathBuf>,
    ) -> Result<Self, BackendError> {
        let code = code.into();
        if code.trim().is_empty() {
            return Err(BackendError::InvalidRequest(BackendRequestError::EmptyCode));
        }
        let output_directory = output_directory.into();
        if output_directory.as_os_str().is_empty() {
            return Err(BackendError::InvalidRequest(
                BackendRequestError::EmptyOutputDirectory,
            ));
        }
        Ok(Self {
            code,
            output_directory,
            relay: None,
        })
    }

    pub fn with_relay(mut self, relay: impl Into<String>) -> Self {
        self.relay = Some(relay.into());
        self
    }
}

impl fmt::Debug for ReceiveRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiveRequest")
            .field("code", &"[REDACTED]")
            .field("output_directory_configured", &true)
            .field("relay", &self.relay)
            .finish()
    }
}

#[async_trait]
pub trait TransferHandle: Send {
    fn take_updates(&mut self) -> Option<tokio::sync::mpsc::Receiver<BackendEvent>>;

    async fn wait(self: Box<Self>) -> Result<(), BackendError>;

    async fn wait_with_cancel_signal(
        self: Box<Self>,
        cancellation: BackendCancellation,
    ) -> Result<(), BackendError>;

    async fn cancel(&mut self) -> Result<BackendControlResult, BackendError>;

    async fn pause(&mut self) -> Result<BackendControlResult, BackendError> {
        Ok(BackendControlResult::Unsupported)
    }

    async fn resume(&mut self) -> Result<BackendControlResult, BackendError> {
        Ok(BackendControlResult::Unsupported)
    }
}

#[async_trait]
pub trait TransferBackend: Send + Sync {
    fn name(&self) -> &'static str {
        "unknown"
    }

    fn version(&self) -> Option<&'static str> {
        None
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::default()
    }

    fn availability(&self) -> BackendAvailability {
        BackendAvailability::Ready
    }

    fn info(&self) -> BackendInfo {
        BackendInfo::new(
            self.name(),
            self.version(),
            self.capabilities(),
            self.availability(),
        )
    }

    async fn check_ready(&self) -> Result<(), BackendError> {
        match self.availability() {
            BackendAvailability::Ready => Ok(()),
            BackendAvailability::Unavailable { reason } => {
                Err(BackendError::Unavailable { reason })
            }
        }
    }

    async fn send(&self, request: SendRequest) -> Result<Box<dyn TransferHandle>, BackendError>;

    async fn receive(
        &self,
        request: ReceiveRequest,
    ) -> Result<Box<dyn TransferHandle>, BackendError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receive_request_debug_redacts_code_and_destination() {
        let request = ReceiveRequest::new("secret-code", "/private/receive-folder").unwrap();
        let debug = format!("{request:?}");

        assert!(!debug.contains("secret-code"));
        assert!(!debug.contains("/private/receive-folder"));
        assert!(debug.contains("output_directory_configured"));
    }

    #[test]
    fn receive_request_rejects_empty_destination() {
        assert!(ReceiveRequest::new("secret-code", PathBuf::new()).is_err());
    }

    #[test]
    fn classifies_backend_failures_for_retry_policy() {
        assert_eq!(
            BackendError::Timeout {
                timeout: Duration::from_secs(1),
            }
            .failure_kind(),
            TransferFailureKind::Network
        );
        assert_eq!(
            BackendError::Io(io::Error::other("interrupted")).failure_kind(),
            TransferFailureKind::ProcessInterruption
        );
        assert_eq!(
            BackendError::OperationFailed {
                reason: BackendOperationError::ExecutionFailed,
            }
            .failure_kind(),
            TransferFailureKind::ProcessFailure
        );
        assert_eq!(
            BackendError::InvalidRequest(BackendRequestError::EmptyPaths).failure_kind(),
            TransferFailureKind::InvalidRequest
        );
        assert!(
            TransferFailureKind::Network.is_retryable()
                && TransferFailureKind::ProcessInterruption.is_retryable()
        );
        assert!(!TransferFailureKind::ProcessFailure.is_retryable());
    }

    #[test]
    fn backend_capabilities_are_explicit_for_pause_resume() {
        let backend = crate::CrocBackend::default();
        let info = backend.info();

        assert_eq!(info.name, "croc");
        assert_eq!(info.version, Some("11.2.x"));
        assert_eq!(info.availability, BackendAvailability::Ready);
        assert!(!backend.capabilities().supports(BackendCapability::Pause));
        assert!(!backend.capabilities().supports(BackendCapability::Resume));
        assert!(backend.capabilities().supports(BackendCapability::Direct));
        assert!(backend.capabilities().supports(BackendCapability::Relay));
        assert_eq!(backend.version(), Some("11.2.x"));
    }

    #[test]
    fn control_result_distinguishes_confirmed_unsupported_and_terminal() {
        assert_ne!(
            BackendControlResult::Confirmed,
            BackendControlResult::Unsupported
        );
        assert_ne!(
            BackendControlResult::Unsupported,
            BackendControlResult::Terminal
        );
    }
}
