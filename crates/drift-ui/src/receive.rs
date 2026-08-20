use crate::progress::{accept_progress, eta_seconds};
use drift_core::{Progress, TransferCapability, TransferId};
use std::{fmt, future::Future, path::PathBuf, pin::Pin};

pub type ReceiveFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub type ReceiveEventFuture<'a> =
    Pin<Box<dyn Future<Output = Option<ReceiveEvent>> + Send + 'a>>;

pub trait ReceiveEventStream: Send {
    fn next(&mut self) -> ReceiveEventFuture<'_>;
}

pub trait ReceiveController: Send + Sync {
    fn default_destination(&self) -> Option<PathBuf> {
        None
    }

    fn choose_destination(&self) -> ReceiveFuture<Result<PathBuf, ReceiveCommandError>> {
        Box::pin(async { Err(ReceiveCommandError::destination_selection_unavailable()) })
    }

    fn validate_destination(
        &self,
        path: PathBuf,
    ) -> ReceiveFuture<Result<(), ReceiveCommandError>>;

    fn preflight(&self) -> ReceiveFuture<Result<(), ReceiveCommandError>>;

    fn start_receive(
        &self,
        code: String,
        destination: PathBuf,
    ) -> ReceiveFuture<Result<TransferId, ReceiveCommandError>>;

    fn cancel(&self, transfer_id: TransferId) -> ReceiveFuture<Result<(), ReceiveCommandError>>;

    fn subscribe(&self) -> Box<dyn ReceiveEventStream>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveCommandErrorKind {
    DestinationSelectionUnavailable,
    DestinationUnavailable,
    DestinationNotWritable,
    PreflightFailed,
    StartFailed,
    CancelFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveCommandError {
    kind: ReceiveCommandErrorKind,
}

impl ReceiveCommandError {
    pub fn new(kind: ReceiveCommandErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(self) -> ReceiveCommandErrorKind {
        self.kind
    }

    pub fn message(self) -> &'static str {
        match self.kind {
            ReceiveCommandErrorKind::DestinationSelectionUnavailable => {
                "Destination selection is unavailable."
            }
            ReceiveCommandErrorKind::DestinationUnavailable => {
                "The receive folder is unavailable."
            }
            ReceiveCommandErrorKind::DestinationNotWritable => {
                "The receive folder is not writable."
            }
            ReceiveCommandErrorKind::PreflightFailed => "Croc is not ready.",
            ReceiveCommandErrorKind::StartFailed => "The receive transfer could not start.",
            ReceiveCommandErrorKind::CancelFailed => "The receive transfer could not be cancelled.",
        }
    }

    pub fn destination_selection_unavailable() -> Self {
        Self::new(ReceiveCommandErrorKind::DestinationSelectionUnavailable)
    }

    pub fn destination_unavailable() -> Self {
        Self::new(ReceiveCommandErrorKind::DestinationUnavailable)
    }

    pub fn destination_not_writable() -> Self {
        Self::new(ReceiveCommandErrorKind::DestinationNotWritable)
    }

    pub fn preflight_failed() -> Self {
        Self::new(ReceiveCommandErrorKind::PreflightFailed)
    }

    pub fn start_failed() -> Self {
        Self::new(ReceiveCommandErrorKind::StartFailed)
    }

    pub fn cancel_failed() -> Self {
        Self::new(ReceiveCommandErrorKind::CancelFailed)
    }
}

impl fmt::Display for ReceiveCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for ReceiveCommandError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiveFailure {
    Preflight,
    Start,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationStatus {
    Missing,
    Checking,
    Valid,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceivePhase {
    Empty,
    CheckingDestination,
    AwaitingPreflight,
    Preflighting,
    Ready,
    Starting,
    Connecting,
    Connected,
    Authenticating,
    Negotiating,
    Transferring,
    Verifying,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl ReceivePhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Empty => "Enter transfer code",
            Self::CheckingDestination => "Checking receive folder",
            Self::AwaitingPreflight => "Ready to check Croc",
            Self::Preflighting => "Checking Croc",
            Self::Ready => "Ready to receive",
            Self::Starting => "Starting receive",
            Self::Connecting => "Connecting",
            Self::Connected => "Connected",
            Self::Authenticating => "Authenticating",
            Self::Negotiating => "Negotiating",
            Self::Transferring => "Receiving",
            Self::Verifying => "Verifying",
            Self::Cancelling => "Cancelling",
            Self::Completed => "Receive complete",
            Self::Failed => "Receive failed",
            Self::Cancelled => "Receive cancelled",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ReceiveAction {
    UpdateCode { code: String },
    ChooseDestination,
    Preflight,
    Start,
    Cancel,
}

impl fmt::Debug for ReceiveAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UpdateCode { .. } => formatter
                .debug_struct("UpdateCode")
                .field("code", &"[REDACTED]")
                .finish(),
            Self::ChooseDestination => formatter.write_str("ChooseDestination"),
            Self::Preflight => formatter.write_str("Preflight"),
            Self::Start => formatter.write_str("Start"),
            Self::Cancel => formatter.write_str("Cancel"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ReceiveIntent {
    ChooseDestination,
    ValidateDestination {
        generation: u64,
        path: PathBuf,
    },
    Preflight {
        generation: u64,
    },
    Start {
        code: String,
        destination: PathBuf,
    },
    Cancel {
        transfer_id: TransferId,
    },
}

impl fmt::Debug for ReceiveIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChooseDestination => formatter.write_str("ChooseDestination"),
            Self::ValidateDestination { generation, .. } => formatter
                .debug_struct("ValidateDestination")
                .field("generation", generation)
                .field("path_configured", &true)
                .finish(),
            Self::Preflight { generation } => formatter
                .debug_struct("Preflight")
                .field("generation", generation)
                .finish(),
            Self::Start { .. } => formatter
                .debug_struct("Start")
                .field("code", &"[REDACTED]")
                .field("destination_configured", &true)
                .finish(),
            Self::Cancel { transfer_id } => formatter
                .debug_struct("Cancel")
                .field("transfer_id", transfer_id)
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ReceiveEvent {
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
    Progress {
        transfer_id: TransferId,
        transferred: u64,
        total: u64,
        speed_bps: u64,
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
    },
    Cancelled {
        transfer_id: TransferId,
    },
}

impl fmt::Debug for ReceiveEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed {
                transfer_id,
                message,
            } => formatter
                .debug_struct("Failed")
                .field("transfer_id", transfer_id)
                .field("message", message)
                .finish(),
            Self::Progress {
                transfer_id,
                transferred,
                total,
                speed_bps,
            } => formatter
                .debug_struct("Progress")
                .field("transfer_id", transfer_id)
                .field("transferred", transferred)
                .field("total", total)
                .field("speed_bps", speed_bps)
                .finish(),
            Self::CapabilityUnavailable {
                transfer_id,
                capability,
            } => formatter
                .debug_struct("CapabilityUnavailable")
                .field("transfer_id", transfer_id)
                .field("capability", capability)
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

pub struct ReceiveViewState {
    code: String,
    destination: Option<PathBuf>,
    destination_status: DestinationStatus,
    phase: ReceivePhase,
    active_transfer_id: Option<TransferId>,
    progress: Option<(u64, u64, u64)>,
    progress_available: bool,
    code_error: Option<String>,
    destination_error: Option<String>,
    error: Option<String>,
    destination_generation: u64,
    input_generation: u64,
    failure: Option<ReceiveFailure>,
}

impl ReceiveViewState {
    pub fn new(default_destination: Option<PathBuf>) -> Self {
        let mut state = Self {
            code: String::new(),
            destination: None,
            destination_status: DestinationStatus::Missing,
            phase: ReceivePhase::Empty,
            active_transfer_id: None,
            progress: None,
            progress_available: true,
            code_error: None,
            destination_error: None,
            error: None,
            destination_generation: 0,
            input_generation: 0,
            failure: None,
        };
        if let Some(destination) = default_destination {
            state.set_destination(destination);
        }
        state
    }

    pub fn phase(&self) -> ReceivePhase {
        self.phase
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn destination(&self) -> Option<&PathBuf> {
        self.destination.as_ref()
    }

    pub fn active_transfer_id(&self) -> Option<TransferId> {
        self.active_transfer_id
    }

    pub fn progress(&self) -> Option<(u64, u64, u64)> {
        self.progress
    }

    pub fn progress_speed_bps(&self) -> Option<u64> {
        if !self.progress_available || self.phase != ReceivePhase::Transferring {
            return None;
        }
        self.progress
            .filter(|(_, _, speed_bps)| *speed_bps > 0)
            .map(|(_, _, speed_bps)| speed_bps)
    }

    pub fn progress_eta_seconds(&self) -> Option<u64> {
        if !self.progress_available || self.phase != ReceivePhase::Transferring {
            return None;
        }
        self.progress
            .and_then(|(transferred, total, speed_bps)| eta_seconds(transferred, total, speed_bps))
    }

    pub fn progress_available(&self) -> bool {
        self.progress_available
    }

    pub fn code_input_enabled(&self) -> bool {
        !matches!(
            self.phase,
            ReceivePhase::Preflighting
                | ReceivePhase::Starting
                | ReceivePhase::Connecting
                | ReceivePhase::Connected
                | ReceivePhase::Authenticating
                | ReceivePhase::Negotiating
                | ReceivePhase::Transferring
                | ReceivePhase::Verifying
                | ReceivePhase::Cancelling
        )
    }

    pub fn code_error(&self) -> Option<&str> {
        self.code_error.as_deref()
    }

    pub fn destination_error(&self) -> Option<&str> {
        self.destination_error.as_deref()
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn destination_validation_intent(&self) -> Option<ReceiveIntent> {
        self.destination.as_ref().map(|path| ReceiveIntent::ValidateDestination {
            generation: self.destination_generation,
            path: path.clone(),
        })
    }

    pub fn preflight_enabled(&self) -> bool {
        self.inputs_valid()
            && (self.phase == ReceivePhase::AwaitingPreflight
                || (self.phase == ReceivePhase::Failed
                    && self.failure == Some(ReceiveFailure::Preflight)))
    }

    pub fn start_enabled(&self) -> bool {
        self.inputs_valid()
            && (self.phase == ReceivePhase::Ready
                || (self.phase == ReceivePhase::Failed
                    && self.failure == Some(ReceiveFailure::Start)))
    }

    pub fn choose_destination_enabled(&self) -> bool {
        !matches!(
            self.phase,
            ReceivePhase::CheckingDestination
                | ReceivePhase::Preflighting
                | ReceivePhase::Starting
                | ReceivePhase::Connecting
                | ReceivePhase::Connected
                | ReceivePhase::Authenticating
                | ReceivePhase::Negotiating
                | ReceivePhase::Transferring
                | ReceivePhase::Verifying
                | ReceivePhase::Cancelling
        )
    }

    pub fn cancel_enabled(&self) -> bool {
        self.active_transfer_id.is_some()
            && matches!(
                self.phase,
                ReceivePhase::Starting
                    | ReceivePhase::Connecting
                    | ReceivePhase::Connected
                    | ReceivePhase::Authenticating
                    | ReceivePhase::Negotiating
                    | ReceivePhase::Transferring
                    | ReceivePhase::Verifying
            )
    }

    pub fn set_code(&mut self, code: impl Into<String>) {
        if !self.code_input_enabled() {
            return;
        }
        self.code = code.into();
        self.input_generation = self.input_generation.saturating_add(1);
        self.code_error = if self.code.trim().is_empty() {
            Some("Enter a transfer code.".to_owned())
        } else {
            None
        };
        self.error = None;
        self.failure = None;
        self.refresh_input_phase();
    }

    pub fn set_destination(&mut self, destination: PathBuf) -> ReceiveIntent {
        self.destination = Some(destination.clone());
        self.destination_generation = self.destination_generation.saturating_add(1);
        self.input_generation = self.input_generation.saturating_add(1);
        self.destination_status = DestinationStatus::Checking;
        self.destination_error = None;
        self.error = None;
        self.failure = None;
        self.active_transfer_id = None;
        self.progress = None;
        self.phase = ReceivePhase::CheckingDestination;
        ReceiveIntent::ValidateDestination {
            generation: self.destination_generation,
            path: destination,
        }
    }

    pub fn handle_action(&mut self, action: ReceiveAction) -> Option<ReceiveIntent> {
        match action {
            ReceiveAction::UpdateCode { code } => {
                self.set_code(code);
                None
            }
            ReceiveAction::ChooseDestination if self.choose_destination_enabled() => {
                Some(ReceiveIntent::ChooseDestination)
            }
            ReceiveAction::Preflight if self.preflight_enabled() => {
                self.phase = ReceivePhase::Preflighting;
                self.error = None;
                Some(ReceiveIntent::Preflight {
                    generation: self.input_generation,
                })
            }
            ReceiveAction::Start if self.start_enabled() => {
                let destination = self.destination.clone()?;
                self.phase = ReceivePhase::Starting;
                self.progress = None;
                self.progress_available = true;
                self.error = None;
                self.failure = None;
                Some(ReceiveIntent::Start {
                    code: self.code.clone(),
                    destination,
                })
            }
            ReceiveAction::Cancel if self.cancel_enabled() => {
                let transfer_id = self.active_transfer_id?;
                self.phase = ReceivePhase::Cancelling;
                Some(ReceiveIntent::Cancel { transfer_id })
            }
            _ => None,
        }
    }

    pub fn mark_destination_valid(&mut self, generation: u64) {
        if generation != self.destination_generation
            || self.destination_status != DestinationStatus::Checking
        {
            return;
        }
        self.destination_status = DestinationStatus::Valid;
        self.destination_error = None;
        self.refresh_input_phase();
    }

    pub fn mark_destination_failed(&mut self, generation: u64, error: ReceiveCommandError) {
        if generation != self.destination_generation
            || self.destination_status != DestinationStatus::Checking
        {
            return;
        }
        self.destination_status = DestinationStatus::Invalid;
        self.phase = ReceivePhase::Empty;
        self.destination_error = Some(error.message().to_owned());
        self.failure = None;
    }

    pub fn mark_destination_selection_failed(&mut self) {
        self.error = Some(
            ReceiveCommandError::destination_selection_unavailable()
                .message()
                .to_owned(),
        );
        self.refresh_input_phase();
    }

    pub fn mark_preflight_succeeded(&mut self, generation: u64) {
        if generation == self.input_generation && self.phase == ReceivePhase::Preflighting {
            self.phase = ReceivePhase::Ready;
            self.error = None;
            self.failure = None;
        }
    }

    pub fn mark_preflight_failed(&mut self, generation: u64) {
        if generation == self.input_generation && self.phase == ReceivePhase::Preflighting {
            self.phase = ReceivePhase::Failed;
            self.failure = Some(ReceiveFailure::Preflight);
            self.error = Some(ReceiveCommandError::preflight_failed().message().to_owned());
        }
    }

    pub fn mark_start_succeeded(&mut self, transfer_id: TransferId) {
        if self.phase == ReceivePhase::Starting && self.active_transfer_id.is_none() {
            self.active_transfer_id = Some(transfer_id);
        }
    }

    pub fn mark_start_failed(&mut self) {
        if self.phase == ReceivePhase::Starting {
            self.phase = ReceivePhase::Failed;
            self.failure = Some(ReceiveFailure::Start);
            self.active_transfer_id = None;
            self.error = Some(ReceiveCommandError::start_failed().message().to_owned());
        }
    }

    pub fn mark_cancel_failed(&mut self) {
        if self.phase == ReceivePhase::Cancelling {
            self.phase = ReceivePhase::Transferring;
            self.error = Some(ReceiveCommandError::cancel_failed().message().to_owned());
        }
    }

    pub fn apply_event(&mut self, event: ReceiveEvent) {
        match event {
            ReceiveEvent::Created { transfer_id } => {
                if self.phase == ReceivePhase::Starting && self.active_transfer_id.is_none() {
                    self.active_transfer_id = Some(transfer_id);
                    self.progress = None;
                    self.progress_available = true;
                }
            }
            ReceiveEvent::Connecting { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Connecting;
                }
            }
            ReceiveEvent::Connected { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Connected;
                }
            }
            ReceiveEvent::Authenticating { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Authenticating;
                }
            }
            ReceiveEvent::Negotiating { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Negotiating;
                }
            }
            ReceiveEvent::Started { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Transferring;
                }
            }
            ReceiveEvent::Progress {
                transfer_id,
                transferred,
                total,
                speed_bps,
            } => {
                if self.accepts_transfer(transfer_id) && self.phase == ReceivePhase::Transferring {
                    let previous = self.progress.map(|(transferred, total, speed_bps)| {
                        Progress {
                            transferred_bytes: transferred,
                            total_bytes: total,
                            speed_bps,
                        }
                    });
                    let Some(progress) = accept_progress(
                        previous,
                        transferred,
                        total,
                        speed_bps,
                    ) else {
                        return;
                    };
                    self.progress = Some((
                        progress.transferred_bytes,
                        progress.total_bytes,
                        progress.speed_bps,
                    ));
                    self.phase = ReceivePhase::Transferring;
                }
            }
            ReceiveEvent::CapabilityUnavailable {
                transfer_id,
                capability: TransferCapability::Progress,
            } => {
                if self.accepts_transfer(transfer_id) {
                    self.progress_available = false;
                }
            }
            ReceiveEvent::Verifying { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Verifying;
                }
            }
            ReceiveEvent::Completed { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Completed;
                    self.active_transfer_id = None;
                    self.error = None;
                }
            }
            ReceiveEvent::Failed {
                transfer_id,
                message,
            } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Failed;
                    self.failure = Some(ReceiveFailure::Start);
                    self.active_transfer_id = None;
                    self.error = Some(message);
                }
            }
            ReceiveEvent::Cancelled { transfer_id } => {
                if self.accepts_transfer(transfer_id) {
                    self.phase = ReceivePhase::Cancelled;
                    self.active_transfer_id = None;
                    self.error = None;
                }
            }
        }
    }

    fn inputs_valid(&self) -> bool {
        !self.code.trim().is_empty() && self.destination_status == DestinationStatus::Valid
    }

    fn refresh_input_phase(&mut self) {
        if self.active_transfer_id.is_some() {
            return;
        }
        self.phase = if self.destination_status == DestinationStatus::Checking {
            ReceivePhase::CheckingDestination
        } else if self.inputs_valid() {
            ReceivePhase::AwaitingPreflight
        } else {
            ReceivePhase::Empty
        };
    }

    fn accepts_transfer(&self, transfer_id: TransferId) -> bool {
        self.active_transfer_id == Some(transfer_id)
    }
}

impl fmt::Debug for ReceiveViewState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReceiveViewState")
            .field("code_configured", &(!self.code.is_empty()))
            .field("destination_configured", &self.destination.is_some())
            .field("destination_status", &self.destination_status)
            .field("phase", &self.phase)
            .field("active_transfer_id", &self.active_transfer_id)
            .field("progress", &self.progress)
            .field("progress_available", &self.progress_available)
            .field("code_error", &self.code_error)
            .field("destination_error", &self.destination_error)
            .field("error", &self.error)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_state() -> ReceiveViewState {
        let mut state = ReceiveViewState::new(None);
        let destination_intent = state.set_destination(PathBuf::from("/tmp/receive"));
        let generation = match destination_intent {
            ReceiveIntent::ValidateDestination { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_destination_valid(generation);
        state.set_code("transfer-code");
        let preflight = state.handle_action(ReceiveAction::Preflight).unwrap();
        let generation = match preflight {
            ReceiveIntent::Preflight { generation } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);
        state
    }

    #[test]
    fn empty_code_or_destination_keeps_receive_disabled() {
        let mut state = ReceiveViewState::new(None);
        assert!(!state.start_enabled());

        state.set_code("transfer-code");
        assert!(!state.start_enabled());
        assert_eq!(state.code_error(), None);

        let intent = state.set_destination(PathBuf::from("/tmp/receive"));
        let generation = match intent {
            ReceiveIntent::ValidateDestination { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        assert!(!state.start_enabled());
        state.mark_destination_valid(generation);
        assert!(!state.start_enabled());
        assert!(state.preflight_enabled());

        state.set_code("   ");
        assert!(!state.start_enabled());
        assert_eq!(state.code_error(), Some("Enter a transfer code."));
    }

    #[test]
    fn preflight_precedes_receive_and_repeated_start_is_rejected() {
        let mut state = ready_state();
        assert!(state.start_enabled());

        let intent = state.handle_action(ReceiveAction::Start).unwrap();
        assert!(matches!(intent, ReceiveIntent::Start { .. }));
        assert!(!state.start_enabled());
        assert_eq!(state.handle_action(ReceiveAction::Start), None);
    }

    #[test]
    fn receiver_events_reach_connecting_and_terminal_states() {
        let mut state = ready_state();
        state.handle_action(ReceiveAction::Start);
        let transfer_id = TransferId::new();
        state.apply_event(ReceiveEvent::Created { transfer_id });
        state.apply_event(ReceiveEvent::Connecting { transfer_id });
        assert_eq!(state.phase(), ReceivePhase::Connecting);
        state.apply_event(ReceiveEvent::Authenticating { transfer_id });
        state.apply_event(ReceiveEvent::Started { transfer_id });
        assert_eq!(state.phase(), ReceivePhase::Transferring);
        state.apply_event(ReceiveEvent::Completed { transfer_id });
        assert_eq!(state.phase(), ReceivePhase::Completed);
        assert_eq!(state.active_transfer_id(), None);
    }

    #[test]
    fn receiver_progress_is_monotonic_and_eta_is_active_only() {
        let mut state = ready_state();
        state.handle_action(ReceiveAction::Start);
        let transfer_id = TransferId::new();
        state.mark_start_succeeded(transfer_id);
        state.apply_event(ReceiveEvent::Started { transfer_id });
        state.apply_event(ReceiveEvent::Progress {
            transfer_id,
            transferred: 25,
            total: 100,
            speed_bps: 25,
        });

        assert_eq!(state.progress(), Some((25, 100, 25)));
        assert_eq!(state.progress_speed_bps(), Some(25));
        assert_eq!(state.progress_eta_seconds(), Some(3));

        state.apply_event(ReceiveEvent::Progress {
            transfer_id,
            transferred: 10,
            total: 100,
            speed_bps: 10,
        });
        assert_eq!(state.progress(), Some((25, 100, 25)));

        state.apply_event(ReceiveEvent::Verifying { transfer_id });
        assert_eq!(state.progress_eta_seconds(), None);
        assert_eq!(state.progress_speed_bps(), None);
        state.apply_event(ReceiveEvent::Progress {
            transfer_id,
            transferred: 50,
            total: 100,
            speed_bps: 25,
        });
        assert_eq!(state.phase(), ReceivePhase::Verifying);
        assert_eq!(state.progress(), Some((25, 100, 25)));
    }

    #[test]
    fn receiver_zero_total_or_speed_has_no_eta() {
        let mut state = ready_state();
        state.handle_action(ReceiveAction::Start);
        let transfer_id = TransferId::new();
        state.mark_start_succeeded(transfer_id);
        state.apply_event(ReceiveEvent::Started { transfer_id });
        state.apply_event(ReceiveEvent::Progress {
            transfer_id,
            transferred: 0,
            total: 0,
            speed_bps: 0,
        });
        assert_eq!(state.progress_eta_seconds(), None);
        assert_eq!(state.progress_speed_bps(), None);
    }

    #[test]
    fn code_and_destination_stay_out_of_debug_output() {
        let mut state = ready_state();
        state.set_code("  meaningful-code  ");
        assert_eq!(state.code(), "  meaningful-code  ");
        let generation = match state.handle_action(ReceiveAction::Preflight).unwrap() {
            ReceiveIntent::Preflight { generation } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_succeeded(generation);
        let debug = format!("{state:?}");
        assert!(!debug.contains("transfer-code"));
        assert!(!debug.contains("/tmp/receive"));

        let intent = state.handle_action(ReceiveAction::Start).unwrap();
        assert!(!format!("{intent:?}").contains("transfer-code"));
        assert!(!format!("{intent:?}").contains("/tmp/receive"));
    }

    #[test]
    fn failed_preflight_can_retry_without_starting_duplicate_session() {
        let mut state = ReceiveViewState::new(Some(PathBuf::from("/tmp/receive")));
        let destination_generation = match state.destination_validation_intent().unwrap() {
            ReceiveIntent::ValidateDestination { generation, .. } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_destination_valid(destination_generation);
        state.set_code("transfer-code");
        let preflight_generation = match state.handle_action(ReceiveAction::Preflight).unwrap() {
            ReceiveIntent::Preflight { generation } => generation,
            other => panic!("unexpected intent: {other:?}"),
        };
        state.mark_preflight_failed(preflight_generation);
        assert!(state.preflight_enabled());
        assert!(!state.start_enabled());
        assert!(matches!(
            state.handle_action(ReceiveAction::Preflight),
            Some(ReceiveIntent::Preflight { .. })
        ));
    }

    #[test]
    fn code_input_is_locked_while_preflighting_or_receiving() {
        let mut state = ready_state();
        state.handle_action(ReceiveAction::Start);
        assert!(!state.code_input_enabled());
        state.set_code("replacement-code");
        assert_eq!(state.code(), "transfer-code");

        let transfer_id = TransferId::new();
        state.mark_start_succeeded(transfer_id);
        state.apply_event(ReceiveEvent::Started { transfer_id });
        state.set_code("another-code");
        assert_eq!(state.code(), "transfer-code");
    }
}
