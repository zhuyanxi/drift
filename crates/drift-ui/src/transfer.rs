use drift_core::{Progress, Role, TransferFailureKind, TransferId, TransferState};
use std::{collections::HashSet, fmt, future::Future, pin::Pin};

use crate::RecoveryCandidate;

pub type TransferCommandFuture =
    Pin<Box<dyn Future<Output = Result<TransferId, TransferCommandError>> + Send + 'static>>;

pub type TransferListFuture = Pin<
    Box<dyn Future<Output = Result<Vec<TransferSnapshot>, TransferCommandError>> + Send + 'static>,
>;

pub type TransferEventFuture<'a> =
    Pin<Box<dyn Future<Output = Option<TransferSnapshot>> + Send + 'a>>;

pub trait TransferEventStream: Send {
    fn next(&mut self) -> TransferEventFuture<'_>;
}

pub trait TransferController: Send + Sync {
    fn load(&self) -> TransferListFuture {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn cancel(&self, transfer_id: TransferId) -> TransferCommandFuture;

    fn retry(&self, transfer_id: TransferId) -> TransferCommandFuture;

    fn pause(&self, transfer_id: TransferId) -> TransferCommandFuture;

    fn resume(&self, transfer_id: TransferId) -> TransferCommandFuture;

    fn reveal_destination(&self, transfer_id: TransferId) -> TransferCommandFuture;

    fn subscribe(&self) -> Box<dyn TransferEventStream>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferCommandErrorKind {
    Unavailable,
    Failed,
    Unsupported,
    DestinationUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferCommandError {
    kind: TransferCommandErrorKind,
}

impl TransferCommandError {
    pub const fn new(kind: TransferCommandErrorKind) -> Self {
        Self { kind }
    }

    pub const fn kind(self) -> TransferCommandErrorKind {
        self.kind
    }

    pub const fn message(self) -> &'static str {
        match self.kind {
            TransferCommandErrorKind::Unavailable => "Transfer controls are unavailable.",
            TransferCommandErrorKind::Failed => "The transfer action could not be completed.",
            TransferCommandErrorKind::Unsupported => "This action is unavailable for this backend.",
            TransferCommandErrorKind::DestinationUnavailable => {
                "The completed receive destination is unavailable."
            }
        }
    }
}

impl fmt::Display for TransferCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for TransferCommandError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    Sending,
    Receiving,
}

impl TransferDirection {
    pub const fn from_role(role: Role) -> Self {
        match role {
            Role::Sender => Self::Sending,
            Role::Receiver => Self::Receiving,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Sending => "Sending",
            Self::Receiving => "Receiving",
        }
    }

    pub const fn object_label(self) -> &'static str {
        match self {
            Self::Sending => "send",
            Self::Receiving => "receive",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayStatus {
    Default,
    Custom,
    Unknown,
}

impl RelayStatus {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Default => "Default relay",
            Self::Custom => "Custom relay",
            Self::Unknown => "Relay status unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferControls {
    pub cancel: bool,
    pub retry: bool,
    pub pause: bool,
    pub resume: bool,
    pub reveal_destination: bool,
}

impl TransferControls {
    fn from_snapshot(snapshot: &TransferSnapshot) -> Self {
        let terminal = matches!(
            snapshot.state,
            TransferState::Completed | TransferState::Failed | TransferState::Cancelled
        );
        Self {
            cancel: !terminal,
            retry: snapshot.state == TransferState::Failed && snapshot.retryable,
            pause: snapshot.state == TransferState::Transferring && snapshot.pause_supported,
            resume: snapshot.state == TransferState::Paused && snapshot.resume_supported,
            reveal_destination: snapshot.role == Role::Receiver
                && snapshot.state == TransferState::Completed
                && snapshot.destination_available,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferSnapshot {
    pub transfer_id: TransferId,
    pub role: Role,
    pub state: TransferState,
    pub progress: Progress,
    pub progress_supported: bool,
    pub pause_supported: bool,
    pub resume_supported: bool,
    pub retryable: bool,
    pub display_name: Option<String>,
    pub file_count: Option<usize>,
    pub relay: RelayStatus,
    pub error: Option<String>,
    pub destination_available: bool,
}

impl TransferSnapshot {
    pub fn minimal(transfer_id: TransferId, role: Role, state: TransferState) -> Self {
        Self {
            transfer_id,
            role,
            state,
            progress: Progress {
                transferred_bytes: 0,
                total_bytes: 0,
                speed_bps: 0,
            },
            progress_supported: false,
            pause_supported: false,
            resume_supported: false,
            retryable: false,
            display_name: None,
            file_count: None,
            relay: RelayStatus::Unknown,
            error: None,
            destination_available: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferSummary {
    pub transfer_id: TransferId,
    pub direction: TransferDirection,
    pub display_name: String,
    pub file_count: Option<usize>,
    pub state: TransferState,
    pub progress: Progress,
    pub progress_supported: bool,
    pub speed_bps: Option<u64>,
    pub eta_seconds: Option<u64>,
    pub error: Option<String>,
    pub relay: RelayStatus,
    pub controls: TransferControls,
    pub recovery_available: bool,
}

impl TransferSummary {
    pub fn from_snapshot(snapshot: TransferSnapshot) -> Self {
        let direction = TransferDirection::from_role(snapshot.role);
        let display_name = safe_display_name(snapshot.display_name.as_deref(), direction);
        let active = matches!(
            snapshot.state,
            TransferState::Transferring | TransferState::Resuming
        );
        let speed_bps = snapshot
            .progress_supported
            .then_some(snapshot.progress.speed_bps)
            .filter(|speed| *speed > 0 && active);
        let eta_seconds = snapshot
            .progress_supported
            .then(|| snapshot.progress.eta_seconds())
            .flatten()
            .filter(|_| active);
        let error = safe_error(
            snapshot.state,
            snapshot.error.as_deref(),
            snapshot.retryable,
        );
        let controls = TransferControls::from_snapshot(&snapshot);

        Self {
            transfer_id: snapshot.transfer_id,
            direction,
            display_name,
            file_count: snapshot.file_count,
            state: snapshot.state,
            progress: snapshot.progress,
            progress_supported: snapshot.progress_supported,
            speed_bps,
            eta_seconds,
            error,
            relay: snapshot.relay,
            controls,
            recovery_available: false,
        }
    }

    pub fn recovery(transfer_id: TransferId, direction: TransferDirection) -> Self {
        let role = match direction {
            TransferDirection::Sending => Role::Sender,
            TransferDirection::Receiving => Role::Receiver,
        };
        let mut summary = Self::from_snapshot(TransferSnapshot {
            transfer_id,
            role,
            state: TransferState::Failed,
            progress: Progress {
                transferred_bytes: 0,
                total_bytes: 0,
                speed_bps: 0,
            },
            progress_supported: false,
            pause_supported: false,
            resume_supported: false,
            retryable: false,
            display_name: Some(format!("Interrupted {}", direction.object_label())),
            file_count: None,
            relay: RelayStatus::Unknown,
            error: None,
            destination_available: false,
        });
        summary.recovery_available = true;
        summary
    }

    pub fn state_label(&self) -> &'static str {
        state_label(self.state)
    }

    pub fn file_count_label(&self) -> String {
        match self.file_count {
            Some(1) => "1 file".to_owned(),
            Some(count) => format!("{count} files"),
            None => "Files".to_owned(),
        }
    }

    pub fn progress_percent(&self) -> Option<u8> {
        if !self.progress_supported || self.progress.total_bytes == 0 {
            return None;
        }
        Some(self.progress.percent().clamp(0.0, 100.0) as u8)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferDetail {
    pub summary: TransferSummary,
    pub destination_available: bool,
}

impl TransferDetail {
    pub fn from_snapshot(snapshot: TransferSnapshot) -> Self {
        let destination_available = snapshot.destination_available;
        Self {
            summary: TransferSummary::from_snapshot(snapshot),
            destination_available,
        }
    }

    pub fn from_summary(summary: TransferSummary) -> Self {
        let destination_available = summary.controls.reveal_destination;
        Self {
            summary,
            destination_available,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferListState {
    Empty,
    Loading,
    Ready,
    Error,
    RecoveryAvailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferAction {
    Cancel,
    Retry,
    Pause,
    Resume,
    RevealDestination,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TransferListModel {
    rows: Vec<TransferSummary>,
    selected: Option<TransferId>,
    loading: bool,
    error: bool,
    recovery_count: usize,
    recovery_only_ids: HashSet<TransferId>,
}

impl TransferListModel {
    pub fn rows(&self) -> &[TransferSummary] {
        &self.rows
    }

    pub fn selected(&self) -> Option<TransferId> {
        self.selected
    }

    pub fn selected_detail(&self) -> Option<TransferDetail> {
        let selected = self.selected?;
        self.rows
            .iter()
            .find(|row| row.transfer_id == selected)
            .cloned()
            .map(TransferDetail::from_summary)
    }

    pub fn state(&self) -> TransferListState {
        if self.loading {
            TransferListState::Loading
        } else if self.error {
            TransferListState::Error
        } else if self.recovery_count > 0 {
            TransferListState::RecoveryAvailable
        } else if self.rows.is_empty() {
            TransferListState::Empty
        } else {
            TransferListState::Ready
        }
    }

    pub fn set_loading(&mut self, loading: bool) {
        self.loading = loading;
        if loading {
            self.error = false;
        }
    }

    pub fn set_error(&mut self, error: Option<&str>) {
        self.error = error.is_some();
        self.loading = false;
    }

    pub fn replace_recoveries(&mut self, candidates: &[RecoveryCandidate]) {
        let candidate_ids = candidates
            .iter()
            .map(|candidate| candidate.transfer_id)
            .collect::<HashSet<_>>();
        for row in &mut self.rows {
            row.recovery_available = false;
        }
        self.rows.retain(|row| {
            !self.recovery_only_ids.contains(&row.transfer_id)
                || candidate_ids.contains(&row.transfer_id)
        });
        self.recovery_only_ids
            .retain(|transfer_id| candidate_ids.contains(transfer_id));
        self.recovery_count = 0;
        for candidate in candidates.iter().copied() {
            self.add_recovery(candidate);
        }
    }

    pub fn recovery_count(&self) -> usize {
        self.recovery_count
    }

    pub fn upsert(&mut self, snapshot: TransferSnapshot) {
        let summary = TransferSummary::from_snapshot(snapshot);
        if let Some(row) = self
            .rows
            .iter_mut()
            .find(|row| row.transfer_id == summary.transfer_id)
        {
            self.recovery_only_ids.remove(&summary.transfer_id);
            let recovery_available = row.recovery_available;
            *row = summary;
            row.recovery_available = recovery_available;
        } else {
            self.rows.push(summary);
        }
        self.loading = false;
        self.error = false;
    }

    pub fn add_recovery(&mut self, candidate: RecoveryCandidate) {
        let direction = match candidate.kind {
            crate::RecoveryKind::Send => TransferDirection::Sending,
            crate::RecoveryKind::Receive => TransferDirection::Receiving,
        };
        if let Some(row) = self
            .rows
            .iter_mut()
            .find(|row| row.transfer_id == candidate.transfer_id)
        {
            if row.recovery_available {
                return;
            }
            row.recovery_available = true;
        } else {
            self.rows
                .push(TransferSummary::recovery(candidate.transfer_id, direction));
            self.recovery_only_ids.insert(candidate.transfer_id);
        }
        self.recovery_count = self.recovery_count.saturating_add(1);
    }

    pub fn remove_recovery(&mut self, transfer_id: TransferId) -> bool {
        let Some(row) = self
            .rows
            .iter_mut()
            .find(|row| row.transfer_id == transfer_id)
        else {
            return false;
        };
        if !row.recovery_available {
            return false;
        }
        row.recovery_available = false;
        self.recovery_count = self.recovery_count.saturating_sub(1);
        if self.recovery_only_ids.remove(&transfer_id) {
            self.remove(transfer_id);
        }
        true
    }

    pub fn select(&mut self, transfer_id: Option<TransferId>) {
        self.selected = transfer_id.filter(|id| self.rows.iter().any(|row| row.transfer_id == *id));
    }

    pub fn remove(&mut self, transfer_id: TransferId) -> bool {
        let previous_len = self.rows.len();
        if self
            .rows
            .iter()
            .any(|row| row.transfer_id == transfer_id && row.recovery_available)
        {
            self.recovery_count = self.recovery_count.saturating_sub(1);
        }
        self.recovery_only_ids.remove(&transfer_id);
        self.rows.retain(|row| row.transfer_id != transfer_id);
        if self.selected == Some(transfer_id) {
            self.selected = None;
        }
        previous_len != self.rows.len()
    }
}

fn state_label(state: TransferState) -> &'static str {
    match state {
        TransferState::Created => "Preparing",
        TransferState::Connecting => "Connecting",
        TransferState::Connected => "Connected",
        TransferState::Authenticating => "Authenticating",
        TransferState::Negotiating => "Negotiating",
        TransferState::Transferring => "Transferring",
        TransferState::Paused => "Paused",
        TransferState::Resuming => "Resuming",
        TransferState::Verifying => "Verifying",
        TransferState::Completed => "Completed",
        TransferState::Failed => "Failed",
        TransferState::Cancelled => "Cancelled",
    }
}

fn safe_display_name(display_name: Option<&str>, direction: TransferDirection) -> String {
    let fallback = match direction {
        TransferDirection::Sending => "Outgoing files",
        TransferDirection::Receiving => "Incoming files",
    };
    let Some(display_name) = display_name else {
        return fallback.to_owned();
    };
    let normalized = display_name.replace('\\', "/");
    let name = normalized.rsplit('/').next().unwrap_or_default();
    let sanitized = name
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let sanitized = sanitized.trim();
    if sanitized.is_empty() {
        fallback.to_owned()
    } else {
        truncate_label(sanitized)
    }
}

fn truncate_label(value: &str) -> String {
    const MAX_LABEL_CHARS: usize = 96;
    if value.chars().count() <= MAX_LABEL_CHARS {
        return value.to_owned();
    }
    let prefix = value
        .chars()
        .take(MAX_LABEL_CHARS.saturating_sub(3))
        .collect::<String>();
    format!("{prefix}...")
}

fn safe_error(state: TransferState, _error: Option<&str>, retryable: bool) -> Option<String> {
    if state != TransferState::Failed {
        return None;
    }
    let message = if retryable {
        "The transfer stopped unexpectedly. Retry is available."
    } else {
        "The transfer failed."
    };
    Some(message.to_owned())
}

pub fn failure_label(failure_kind: Option<TransferFailureKind>) -> Option<&'static str> {
    failure_kind.map(|kind| match kind {
        TransferFailureKind::Network => "Network connection failed.",
        TransferFailureKind::ProcessInterruption => "The transfer stopped unexpectedly.",
        TransferFailureKind::ProcessFailure => "Croc could not complete the transfer.",
        TransferFailureKind::InvalidRequest => "The transfer request is invalid.",
        TransferFailureKind::Filesystem => "The received files could not be finalized.",
        TransferFailureKind::Security => "The received output was rejected for safety.",
        TransferFailureKind::Integrity => "The received files failed integrity verification.",
        TransferFailureKind::Unknown => "The transfer failed.",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RecoveryKind;

    fn snapshot(state: TransferState) -> TransferSnapshot {
        TransferSnapshot {
            display_name: Some("/Users/example/private/very-long-name.txt".into()),
            file_count: Some(1),
            relay: RelayStatus::Custom,
            progress: Progress {
                transferred_bytes: 25,
                total_bytes: 100,
                speed_bps: 10,
            },
            progress_supported: true,
            pause_supported: false,
            resume_supported: false,
            retryable: false,
            error: Some("/Users/example/private/backend-output-secret".into()),
            ..TransferSnapshot::minimal(TransferId::new(), Role::Sender, state)
        }
    }

    #[test]
    fn summary_maps_terminal_states_to_distinct_labels() {
        let completed = TransferSummary::from_snapshot(snapshot(TransferState::Completed));
        let failed = TransferSummary::from_snapshot(snapshot(TransferState::Failed));
        let cancelled = TransferSummary::from_snapshot(snapshot(TransferState::Cancelled));

        assert_eq!(completed.state_label(), "Completed");
        assert_eq!(failed.state_label(), "Failed");
        assert_eq!(cancelled.state_label(), "Cancelled");
        assert!(!completed.controls.cancel);
        assert!(!failed.controls.cancel);
        assert!(!cancelled.controls.cancel);
    }

    #[test]
    fn controls_follow_state_and_backend_capabilities() {
        let mut transferring = snapshot(TransferState::Transferring);
        transferring.pause_supported = true;
        let summary = TransferSummary::from_snapshot(transferring);
        assert!(summary.controls.cancel);
        assert!(summary.controls.pause);
        assert!(!summary.controls.resume);

        let mut paused = snapshot(TransferState::Paused);
        paused.resume_supported = true;
        let summary = TransferSummary::from_snapshot(paused);
        assert!(summary.controls.cancel);
        assert!(!summary.controls.pause);
        assert!(summary.controls.resume);

        let mut failed = snapshot(TransferState::Failed);
        failed.retryable = true;
        assert!(TransferSummary::from_snapshot(failed).controls.retry);
    }

    #[test]
    fn summary_redacts_paths_and_backend_errors() {
        let summary = TransferSummary::from_snapshot(snapshot(TransferState::Failed));

        assert_eq!(summary.display_name, "very-long-name.txt");
        assert_eq!(summary.error.as_deref(), Some("The transfer failed."));
        assert!(!summary.display_name.contains("/Users"));
        assert!(!summary
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("backend-output"));
    }

    #[test]
    fn list_replaces_rows_in_place_and_preserves_selection() {
        let first = snapshot(TransferState::Transferring);
        let first_id = first.transfer_id;
        let second = snapshot(TransferState::Connecting);
        let second_id = second.transfer_id;
        let mut list = TransferListModel::default();
        list.upsert(first.clone());
        list.upsert(second);
        list.select(Some(first_id));

        let mut updated = first;
        updated.progress.transferred_bytes = 75;
        list.upsert(updated);

        assert_eq!(list.rows()[0].transfer_id, first_id);
        assert_eq!(list.rows()[1].transfer_id, second_id);
        assert_eq!(list.selected(), Some(first_id));
        assert_eq!(list.rows()[0].progress.transferred_bytes, 75);
    }

    #[test]
    fn list_exposes_empty_loading_and_recovery_states() {
        let mut list = TransferListModel::default();
        assert_eq!(list.state(), TransferListState::Empty);
        list.set_loading(true);
        assert_eq!(list.state(), TransferListState::Loading);
        list.set_loading(false);
        list.add_recovery(RecoveryCandidate {
            transfer_id: TransferId::new(),
            kind: RecoveryKind::Send,
        });
        assert_eq!(list.state(), TransferListState::RecoveryAvailable);
    }

    #[test]
    fn recovery_rows_are_idempotent() {
        let candidate = RecoveryCandidate {
            transfer_id: TransferId::new(),
            kind: RecoveryKind::Send,
        };
        let mut list = TransferListModel::default();

        list.add_recovery(candidate);
        list.add_recovery(candidate);

        assert_eq!(list.recovery_count(), 1);
        assert_eq!(list.rows().len(), 1);
        assert!(list.rows()[0].recovery_available);
    }

    #[test]
    fn recovery_refresh_removes_stale_synthetic_rows() {
        let first = RecoveryCandidate {
            transfer_id: TransferId::new(),
            kind: RecoveryKind::Send,
        };
        let second = RecoveryCandidate {
            transfer_id: TransferId::new(),
            kind: RecoveryKind::Receive,
        };
        let mut list = TransferListModel::default();

        list.replace_recoveries(&[first]);
        list.replace_recoveries(&[second]);

        assert_eq!(list.recovery_count(), 1);
        assert_eq!(list.rows().len(), 1);
        assert_eq!(list.rows()[0].transfer_id, second.transfer_id);
    }

    #[test]
    fn long_display_names_are_bounded() {
        let mut snapshot = snapshot(TransferState::Completed);
        snapshot.display_name = Some("x".repeat(200));
        let summary = TransferSummary::from_snapshot(snapshot);
        assert_eq!(summary.display_name.chars().count(), 96);
        assert!(summary.display_name.ends_with("..."));
    }

    #[test]
    fn completed_receive_can_reveal_only_when_destination_is_available() {
        let mut snapshot = snapshot(TransferState::Completed);
        snapshot.role = Role::Receiver;
        assert!(
            !TransferSummary::from_snapshot(snapshot.clone())
                .controls
                .reveal_destination
        );
        snapshot.destination_available = true;
        assert!(
            TransferSummary::from_snapshot(snapshot)
                .controls
                .reveal_destination
        );
    }

    #[test]
    fn failure_labels_are_safe_and_specific() {
        assert_eq!(
            failure_label(Some(TransferFailureKind::Security)),
            Some("The received output was rejected for safety.")
        );
        assert_eq!(failure_label(None), None);
    }
}
