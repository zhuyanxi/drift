use async_trait::async_trait;
use drift_core::TransferFailureKind;
use std::{fmt, io, path::PathBuf, time::Duration};
use thiserror::Error;

use crate::TransferHandle;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("invalid backend request: {0}")]
    InvalidRequest(String),
    #[error(
        "croc executable not found at {executable}; install Croc v11.2.x or configure its path"
    )]
    ExecutableMissing { executable: PathBuf },
    #[error("failed to spawn backend process")]
    Spawn(#[source] io::Error),
    #[error("backend process I/O failed")]
    Io(#[source] io::Error),
    #[error("backend output task failed")]
    OutputTask(#[source] tokio::task::JoinError),
    #[error("backend process timed out after {timeout:?}")]
    Timeout { timeout: Duration },
    #[error("backend process cancelled")]
    Cancelled,
    #[error("croc version invocation failed")]
    VersionInvocation,
    #[error("unsupported croc version {found}; Drift supports {supported}")]
    UnsupportedVersion {
        found: String,
        supported: &'static str,
    },
    #[error("croc version output was not recognized")]
    InvalidVersionOutput,
    #[error("croc {stream} output violated the supported contract: {reason}")]
    OutputParse {
        stream: &'static str,
        reason: &'static str,
    },
    #[error("croc did not provide the required {signal} signal")]
    MissingSignal { signal: &'static str },
    #[error("croc {stream} output exceeded the bounded diagnostic limit")]
    OutputLimit { stream: &'static str },
    #[error("backend process exited with code {code:?}: {stderr}")]
    ProcessFailed { code: Option<i32>, stderr: String },
    #[error("backend process did not provide {stream} pipe")]
    MissingPipe { stream: &'static str },
}

impl BackendError {
    pub fn failure_kind(&self) -> TransferFailureKind {
        match self {
            Self::InvalidRequest(_) => TransferFailureKind::InvalidRequest,
            Self::ExecutableMissing { .. } | Self::Spawn(_) | Self::MissingPipe { .. } => {
                TransferFailureKind::Filesystem
            }
            Self::Io(_) | Self::OutputTask(_) => TransferFailureKind::ProcessInterruption,
            Self::Timeout { .. } => TransferFailureKind::Network,
            Self::Cancelled => TransferFailureKind::Unknown,
            Self::VersionInvocation
            | Self::UnsupportedVersion { .. }
            | Self::InvalidVersionOutput
            | Self::OutputParse { .. }
            | Self::MissingSignal { .. }
            | Self::OutputLimit { .. }
            | Self::ProcessFailed { .. } => TransferFailureKind::ProcessFailure,
        }
    }

    pub fn safe_message(&self) -> String {
        match self {
            Self::InvalidRequest(_) => "invalid backend request".into(),
            Self::ExecutableMissing { executable } => format!(
                "croc executable unavailable at {}; install Croc v11.2.x or configure its path",
                executable.display()
            ),
            Self::Spawn(_) => "failed to start the croc process".into(),
            Self::Io(_) => "croc process I/O failed".into(),
            Self::OutputTask(_) => "croc output reader failed".into(),
            Self::Timeout { .. } => "croc process timed out".into(),
            Self::Cancelled => "croc process cancelled".into(),
            Self::VersionInvocation => "croc version check failed".into(),
            Self::UnsupportedVersion { found, supported } => {
                format!("unsupported croc version {found}; Drift supports {supported}")
            }
            Self::InvalidVersionOutput => "croc version output was not recognized".into(),
            Self::OutputParse { .. } => "croc output was not recognized".into(),
            Self::MissingSignal { signal } => format!("croc did not provide {signal}"),
            Self::OutputLimit { .. } => "croc output exceeded the diagnostic limit".into(),
            Self::ProcessFailed { .. } => "croc process failed".into(),
            Self::MissingPipe { .. } => "croc process output was unavailable".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendCapability {
    Progress,
}

impl fmt::Display for BackendCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Progress => formatter.write_str("progress reporting"),
        }
    }
}

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
                "send request must contain at least one path".into(),
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
            return Err(BackendError::InvalidRequest(
                "receive request code must not be empty".into(),
            ));
        }
        let output_directory = output_directory.into();
        if output_directory.as_os_str().is_empty() {
            return Err(BackendError::InvalidRequest(
                "receive request output directory must not be empty".into(),
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
pub trait TransferBackend: Send + Sync {
    async fn send(&self, request: SendRequest) -> Result<TransferHandle, BackendError>;

    async fn receive(&self, request: ReceiveRequest) -> Result<TransferHandle, BackendError>;
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
            BackendError::ProcessFailed {
                code: Some(1),
                stderr: String::new(),
            }
            .failure_kind(),
            TransferFailureKind::ProcessFailure
        );
        assert_eq!(
            BackendError::InvalidRequest("bad request".into()).failure_kind(),
            TransferFailureKind::InvalidRequest
        );
        assert!(
            TransferFailureKind::Network.is_retryable()
                && TransferFailureKind::ProcessInterruption.is_retryable()
        );
        assert!(!TransferFailureKind::ProcessFailure.is_retryable());
    }
}
