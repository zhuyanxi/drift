use async_trait::async_trait;
use std::{fmt, io, path::PathBuf, time::Duration};
use thiserror::Error;

use crate::TransferHandle;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("invalid backend request: {0}")]
    InvalidRequest(String),
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
    #[error("backend process exited with code {code:?}: {stderr}")]
    ProcessFailed { code: Option<i32>, stderr: String },
    #[error("backend process did not provide {stream} pipe")]
    MissingPipe { stream: &'static str },
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
        Ok(Self {
            code,
            output_directory: output_directory.into(),
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
            .field("output_directory", &self.output_directory)
            .field("relay", &self.relay)
            .finish()
    }
}

#[async_trait]
pub trait TransferBackend: Send + Sync {
    async fn send(&self, request: SendRequest) -> Result<TransferHandle, BackendError>;

    async fn receive(&self, request: ReceiveRequest) -> Result<TransferHandle, BackendError>;
}
