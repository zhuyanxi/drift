use crate::{Progress, TransferManifest};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransferId(Uuid);

impl TransferId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for TransferId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TransferId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Sender,
    Receiver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    Created,
    Connecting,
    Connected,
    Authenticating,
    Negotiating,
    Transferring,
    Paused,
    Resuming,
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

impl TransferState {
    pub fn can_transition_to(self, next: Self) -> bool {
        if matches!(self, Self::Completed | Self::Failed | Self::Cancelled) {
            return false;
        }
        if matches!(next, Self::Failed | Self::Cancelled) {
            return true;
        }
        matches!(
            (self, next),
            (Self::Created, Self::Connecting)
                | (Self::Connecting, Self::Connected)
                | (Self::Connected, Self::Authenticating)
                | (Self::Connecting, Self::Authenticating)
                | (Self::Authenticating, Self::Negotiating)
                | (Self::Authenticating, Self::Transferring)
                | (Self::Negotiating, Self::Transferring)
                | (Self::Transferring, Self::Paused)
                | (Self::Transferring, Self::Verifying)
                | (Self::Paused, Self::Resuming)
                | (Self::Resuming, Self::Transferring)
                | (Self::Verifying, Self::Completed)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferFailureKind {
    Network,
    ProcessInterruption,
    ProcessFailure,
    InvalidRequest,
    Filesystem,
    Security,
    Integrity,
    Unknown,
}

impl TransferFailureKind {
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Network | Self::ProcessInterruption)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("invalid transfer state transition: {from:?} -> {to:?}")]
pub struct StateTransitionError {
    pub from: TransferState,
    pub to: TransferState,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransferError {
    #[error("invalid state transition")]
    InvalidStateTransition(#[from] StateTransitionError),
    #[error("invalid transfer progress")]
    InvalidProgress(#[from] crate::ProgressError),
    #[error("progress update is not allowed in transfer state {0:?}")]
    ProgressNotAllowed(TransferState),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("transfer cancelled")]
    Cancelled,
    #[error("transfer cannot be cancelled in state {0:?}")]
    CancelNotAllowed(TransferState),
    #[error("transfer cannot be retried in state {0:?}")]
    RetryNotAllowed(TransferState),
    #[error("transfer capability unavailable: {0:?}")]
    CapabilityUnavailable(TransferCapability),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferCapability {
    Progress,
    Pause,
    Resume,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransferEvent {
    Created,
    Connecting,
    Connected,
    Authenticating,
    Negotiating,
    Started,
    CodeAvailable,
    MetadataReady,
    Progress {
        transferred: u64,
        total: u64,
        speed_bps: u64,
    },
    Paused,
    Resumed,
    CapabilityUnavailable {
        capability: TransferCapability,
    },
    Verifying,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferSession {
    pub id: TransferId,
    pub role: Role,
    pub state: TransferState,
    #[serde(skip)]
    pub code: Option<String>,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub manifest: Option<TransferManifest>,
    pub progress: Progress,
    pub backend: String,
    pub error: Option<String>,
    pub failure_kind: Option<TransferFailureKind>,
}

impl fmt::Debug for TransferSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransferSession")
            .field("id", &self.id)
            .field("role", &self.role)
            .field("state", &self.state)
            .field("code", &self.code.as_ref().map(|_| "[REDACTED]"))
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("manifest", &self.manifest)
            .field("progress", &self.progress)
            .field("backend", &self.backend)
            .field("error", &self.error)
            .field("failure_kind", &self.failure_kind)
            .finish()
    }
}

impl TransferSession {
    pub fn new(role: Role, backend: impl Into<String>) -> Self {
        Self {
            id: TransferId::new(),
            role,
            state: TransferState::Created,
            code: None,
            created_at: now_seconds(),
            expires_at: None,
            manifest: None,
            progress: Progress::new(0, 0, 0).expect("zero progress is valid"),
            backend: backend.into(),
            error: None,
            failure_kind: None,
        }
    }

    pub fn transition(&mut self, next: TransferState) -> Result<(), StateTransitionError> {
        if !self.state.can_transition_to(next) {
            return Err(StateTransitionError {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        Ok(())
    }

    pub fn set_code(&mut self, code: impl Into<String>) {
        self.code = Some(code.into());
    }

    pub fn set_manifest(&mut self, manifest: TransferManifest) {
        self.progress = Progress::new(0, manifest.total_size, 0)
            .expect("manifest total cannot be less than zero");
        self.manifest = Some(manifest);
    }

    pub fn update_progress(
        &mut self,
        transferred: u64,
        speed_bps: u64,
    ) -> Result<(), crate::ProgressError> {
        let total = self.progress.total_bytes;
        self.update_progress_with_total(transferred, total, speed_bps)
    }

    pub fn update_progress_with_total(
        &mut self,
        transferred: u64,
        total: u64,
        speed_bps: u64,
    ) -> Result<(), crate::ProgressError> {
        self.progress = self.progress.update(transferred, total, speed_bps)?;
        Ok(())
    }
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follows_transfer_lifecycle() {
        let mut session = TransferSession::new(Role::Sender, "croc");
        for state in [
            TransferState::Connecting,
            TransferState::Authenticating,
            TransferState::Negotiating,
            TransferState::Transferring,
            TransferState::Verifying,
            TransferState::Completed,
        ] {
            session.transition(state).unwrap();
        }
        assert_eq!(session.state, TransferState::Completed);
        assert!(session.transition(TransferState::Failed).is_err());
    }

    #[test]
    fn debug_redacts_transfer_code() {
        let mut session = TransferSession::new(Role::Sender, "croc");
        session.set_code("secret-code");
        let debug = format!("{session:?}");
        assert!(!debug.contains("secret-code"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn does_not_serialize_transfer_code() {
        let mut session = TransferSession::new(Role::Sender, "croc");
        session.set_code("secret-code");
        let serialized = serde_json::to_string(&session).unwrap();
        assert!(!serialized.contains("secret-code"));
        assert!(!serialized.contains("\"code\""));
    }

    #[test]
    fn allows_backend_without_metadata_signal_to_start_transfer() {
        let mut session = TransferSession::new(Role::Sender, "croc");
        session.transition(TransferState::Connecting).unwrap();
        session.transition(TransferState::Authenticating).unwrap();
        session.transition(TransferState::Transferring).unwrap();
    }
}
