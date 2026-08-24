use drift_core::{
    Progress, Role, TransferEvent, TransferFailureKind, TransferId, TransferSession, TransferState,
};
use drift_transfer::{TransferManager, TransferNotification};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{broadcast, RwLock},
    time,
};
use tracing::{debug, warn};

const EVENT_CHANNEL_CAPACITY: usize = 128;
const PROGRESS_FLUSH_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq)]
pub struct TransferPresentation {
    pub transfer_id: TransferId,
    pub role: Role,
    pub state: TransferState,
    pub progress: Progress,
    pub code_available: bool,
    pub error: Option<String>,
    pub failure_kind: Option<TransferFailureKind>,
}

impl TransferPresentation {
    pub fn eta_seconds(&self) -> Option<u64> {
        if matches!(
            self.state,
            TransferState::Transferring | TransferState::Resuming
        ) {
            self.progress.eta_seconds()
        } else {
            None
        }
    }

    pub fn retryable(&self) -> bool {
        self.failure_kind
            .is_some_and(TransferFailureKind::is_retryable)
    }

    fn from_session(session: &TransferSession) -> Self {
        Self {
            transfer_id: session.id,
            role: session.role,
            state: session.state,
            progress: session.progress,
            code_available: session.code.is_some(),
            error: session.error.clone(),
            failure_kind: session.failure_kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppTransferUpdate {
    pub transfer_id: TransferId,
    pub event: TransferEvent,
    pub presentation: TransferPresentation,
}

struct EventBus {
    sender: Mutex<Option<broadcast::Sender<AppTransferUpdate>>>,
}

#[derive(Clone)]
pub(crate) struct AppEventBridge {
    bus: Arc<EventBus>,
    presentations: Arc<RwLock<HashMap<TransferId, TransferPresentation>>>,
}

impl AppEventBridge {
    pub(crate) fn start(runtime: &Handle, manager: TransferManager) -> Self {
        let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let bus = Arc::new(EventBus {
            sender: Mutex::new(Some(sender.clone())),
        });
        let presentations = Arc::new(RwLock::new(HashMap::new()));
        let bridge = Self {
            bus: Arc::clone(&bus),
            presentations: Arc::clone(&presentations),
        };
        let receiver = manager.subscribe();
        runtime.spawn(run_event_bridge(
            manager,
            receiver,
            sender,
            bus,
            presentations,
        ));
        bridge
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<AppTransferUpdate> {
        let sender = self
            .bus
            .sender
            .lock()
            .ok()
            .and_then(|sender| sender.as_ref().cloned());
        match sender {
            Some(sender) => sender.subscribe(),
            None => closed_receiver(),
        }
    }

    pub(crate) async fn presentation(
        &self,
        transfer_id: TransferId,
    ) -> Option<TransferPresentation> {
        self.presentations.read().await.get(&transfer_id).cloned()
    }
}

async fn run_event_bridge(
    manager: TransferManager,
    mut receiver: broadcast::Receiver<TransferNotification>,
    sender: broadcast::Sender<AppTransferUpdate>,
    bus: Arc<EventBus>,
    presentations: Arc<RwLock<HashMap<TransferId, TransferPresentation>>>,
) {
    let mut pending_progress = HashMap::new();
    let mut flush = time::interval(PROGRESS_FLUSH_INTERVAL);

    loop {
        tokio::select! {
            result = receiver.recv() => match result {
                Ok(notification) => {
                    let Some(update) = build_update(&manager, &presentations, notification).await else {
                        continue;
                    };
                    queue_update(&sender, &mut pending_progress, update);
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "transfer event bridge lagged; refreshing sessions");
                    refresh_sessions(&manager, &presentations, &sender, &mut pending_progress).await;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    warn!("transfer event service shut down");
                    break;
                }
            },
            _ = flush.tick() => {
                flush_pending(&sender, &mut pending_progress);
            }
        }
    }

    if let Ok(mut current) = bus.sender.lock() {
        current.take();
    }
    debug!("transfer event bridge stopped");
}

async fn build_update(
    manager: &TransferManager,
    presentations: &RwLock<HashMap<TransferId, TransferPresentation>>,
    notification: TransferNotification,
) -> Option<AppTransferUpdate> {
    let transfer_id = notification.transfer_id;
    let session = manager.session(transfer_id).await?;
    let previous = presentations.read().await.get(&transfer_id).cloned();
    if let Some(previous) = &previous {
        if !accepts_event(previous.state, &notification.event) {
            warn!(%transfer_id, state = ?previous.state, "ignored late or out-of-order transfer event");
            return None;
        }
    }
    let had_previous = previous.is_some();
    let mut presentation = previous.unwrap_or_else(|| TransferPresentation::from_session(&session));
    apply_event_to_presentation(
        &mut presentation,
        &session,
        &notification.event,
        had_previous,
    )?;
    presentations
        .write()
        .await
        .insert(transfer_id, presentation.clone());
    Some(AppTransferUpdate {
        transfer_id,
        event: notification.event,
        presentation,
    })
}

fn apply_event_to_presentation(
    presentation: &mut TransferPresentation,
    session: &TransferSession,
    event: &TransferEvent,
    had_previous: bool,
) -> Option<()> {
    let next_state = state_for_event(presentation.state, event);
    match event {
        TransferEvent::Created
        | TransferEvent::Connecting
        | TransferEvent::Connected
        | TransferEvent::Authenticating
        | TransferEvent::Negotiating
        | TransferEvent::Started => {
            if !had_previous || next_state != presentation.state {
                presentation.progress = Progress {
                    transferred_bytes: 0,
                    total_bytes: 0,
                    speed_bps: 0,
                };
                presentation.code_available = false;
                presentation.error = None;
                presentation.failure_kind = None;
            }
            presentation.state = next_state;
        }
        TransferEvent::Progress {
            transferred,
            total,
            speed_bps,
        } => {
            presentation.progress = if had_previous {
                presentation
                    .progress
                    .update(*transferred, *total, *speed_bps)
                    .ok()?
            } else {
                Progress::new(*transferred, *total, *speed_bps).ok()?
            };
            presentation.state = next_state;
            presentation.error = None;
            presentation.failure_kind = None;
            if !had_previous {
                presentation.code_available = session.code.is_some();
            }
        }
        TransferEvent::CodeAvailable => {
            presentation.code_available = true;
        }
        TransferEvent::MetadataReady => {
            presentation.state = next_state;
        }
        TransferEvent::Paused | TransferEvent::Resumed => {
            presentation.state = next_state;
        }
        TransferEvent::CapabilityUnavailable { .. } => {}
        TransferEvent::Verifying => {
            presentation.state = next_state;
        }
        TransferEvent::Completed | TransferEvent::Cancelled => {
            presentation.state = next_state;
            presentation.code_available = false;
            presentation.error = None;
            presentation.failure_kind = None;
        }
        TransferEvent::Failed => {
            presentation.state = next_state;
            presentation.code_available = false;
            presentation.error = session.error.clone();
            presentation.failure_kind = session.failure_kind;
        }
    }
    Some(())
}

fn state_for_event(current: TransferState, event: &TransferEvent) -> TransferState {
    match event {
        TransferEvent::Created => TransferState::Created,
        TransferEvent::Connecting => TransferState::Connecting,
        TransferEvent::Connected => TransferState::Connected,
        TransferEvent::Authenticating => TransferState::Authenticating,
        TransferEvent::Negotiating => TransferState::Negotiating,
        TransferEvent::Started | TransferEvent::MetadataReady => TransferState::Transferring,
        TransferEvent::Progress { .. } => match current {
            TransferState::Transferring | TransferState::Resuming => current,
            _ => TransferState::Transferring,
        },
        TransferEvent::Paused => TransferState::Paused,
        TransferEvent::Resumed => TransferState::Resuming,
        TransferEvent::CapabilityUnavailable { .. } | TransferEvent::CodeAvailable => current,
        TransferEvent::Verifying => TransferState::Verifying,
        TransferEvent::Completed => TransferState::Completed,
        TransferEvent::Failed => TransferState::Failed,
        TransferEvent::Cancelled => TransferState::Cancelled,
    }
}

fn state_rank(state: TransferState) -> u8 {
    match state {
        TransferState::Created => 0,
        TransferState::Connecting => 1,
        TransferState::Connected => 2,
        TransferState::Authenticating => 3,
        TransferState::Negotiating => 4,
        TransferState::Transferring => 5,
        TransferState::Paused => 6,
        TransferState::Resuming => 7,
        TransferState::Verifying => 8,
        TransferState::Completed | TransferState::Failed | TransferState::Cancelled => 9,
    }
}

fn accepts_event(previous: TransferState, event: &TransferEvent) -> bool {
    let next = state_for_event(previous, event);
    if is_terminal(previous) {
        return is_terminal_event(event) && next == previous;
    }
    state_rank(next) >= state_rank(previous)
}

async fn refresh_sessions(
    manager: &TransferManager,
    presentations: &RwLock<HashMap<TransferId, TransferPresentation>>,
    sender: &broadcast::Sender<AppTransferUpdate>,
    pending_progress: &mut HashMap<TransferId, AppTransferUpdate>,
) {
    let sessions = manager.sessions().await;
    let current_ids = sessions
        .iter()
        .map(|session| session.id)
        .collect::<HashSet<_>>();
    let stale_ids = presentations
        .read()
        .await
        .keys()
        .filter(|transfer_id| !current_ids.contains(transfer_id))
        .copied()
        .collect::<Vec<_>>();
    for transfer_id in stale_ids {
        presentations.write().await.remove(&transfer_id);
        pending_progress.remove(&transfer_id);
    }
    for session in sessions {
        let transfer_id = session.id;
        let event = event_for_session(&session);
        let presentation = TransferPresentation::from_session(&session);
        presentations
            .write()
            .await
            .insert(transfer_id, presentation.clone());
        let update = AppTransferUpdate {
            transfer_id,
            event,
            presentation,
        };
        pending_progress.remove(&transfer_id);
        publish(sender, update);
    }
}

fn queue_update(
    sender: &broadcast::Sender<AppTransferUpdate>,
    pending_progress: &mut HashMap<TransferId, AppTransferUpdate>,
    update: AppTransferUpdate,
) {
    if matches!(update.event, TransferEvent::Progress { .. }) {
        pending_progress.insert(update.transfer_id, update);
    } else {
        if let Some(progress) = pending_progress.remove(&update.transfer_id) {
            publish(sender, progress);
        }
        publish(sender, update);
    }
}

fn flush_pending(
    sender: &broadcast::Sender<AppTransferUpdate>,
    pending_progress: &mut HashMap<TransferId, AppTransferUpdate>,
) {
    for (_, update) in pending_progress.drain() {
        publish(sender, update);
    }
}

fn publish(sender: &broadcast::Sender<AppTransferUpdate>, update: AppTransferUpdate) {
    let _ = sender.send(update);
}

fn event_for_session(session: &TransferSession) -> TransferEvent {
    match session.state {
        TransferState::Created => TransferEvent::Created,
        TransferState::Connecting => TransferEvent::Connecting,
        TransferState::Connected => TransferEvent::Connected,
        TransferState::Authenticating => TransferEvent::Authenticating,
        TransferState::Negotiating => TransferEvent::Negotiating,
        TransferState::Transferring | TransferState::Resuming => TransferEvent::Progress {
            transferred: session.progress.transferred_bytes,
            total: session.progress.total_bytes,
            speed_bps: session.progress.speed_bps,
        },
        TransferState::Paused => TransferEvent::Paused,
        TransferState::Verifying => TransferEvent::Verifying,
        TransferState::Completed => TransferEvent::Completed,
        TransferState::Failed => TransferEvent::Failed,
        TransferState::Cancelled => TransferEvent::Cancelled,
    }
}

fn is_terminal(state: TransferState) -> bool {
    matches!(
        state,
        TransferState::Completed | TransferState::Failed | TransferState::Cancelled
    )
}

fn is_terminal_event(event: &TransferEvent) -> bool {
    matches!(
        event,
        TransferEvent::Completed | TransferEvent::Failed | TransferEvent::Cancelled
    )
}

fn closed_receiver() -> broadcast::Receiver<AppTransferUpdate> {
    let (sender, receiver) = broadcast::channel(1);
    drop(sender);
    receiver
}

#[cfg(test)]
mod tests {
    use super::*;
    use drift_protocol::CrocBackend;

    fn presentation(transfer_id: TransferId, state: TransferState) -> TransferPresentation {
        TransferPresentation {
            transfer_id,
            role: Role::Sender,
            state,
            progress: Progress {
                transferred_bytes: 0,
                total_bytes: 0,
                speed_bps: 0,
            },
            code_available: false,
            error: None,
            failure_kind: None,
        }
    }

    fn progress_update(transfer_id: TransferId, transferred: u64) -> AppTransferUpdate {
        AppTransferUpdate {
            transfer_id,
            event: TransferEvent::Progress {
                transferred,
                total: 100,
                speed_bps: 10,
            },
            presentation: TransferPresentation {
                progress: Progress {
                    transferred_bytes: transferred,
                    total_bytes: 100,
                    speed_bps: 10,
                },
                ..presentation(transfer_id, TransferState::Transferring)
            },
        }
    }

    #[test]
    fn presentation_eta_only_exists_for_active_transfer() {
        let mut session = TransferSession::new(Role::Sender, "croc");
        session.transition(TransferState::Connecting).unwrap();
        session.transition(TransferState::Connected).unwrap();
        session.transition(TransferState::Authenticating).unwrap();
        session.transition(TransferState::Negotiating).unwrap();
        session.transition(TransferState::Transferring).unwrap();
        session.update_progress_with_total(25, 100, 25).unwrap();
        let presentation = TransferPresentation::from_session(&session);
        assert_eq!(presentation.eta_seconds(), Some(3));

        session.transition(TransferState::Verifying).unwrap();
        let presentation = TransferPresentation::from_session(&session);
        assert_eq!(presentation.eta_seconds(), None);
    }

    #[test]
    fn refresh_event_preserves_distinct_lifecycle_states() {
        let session = TransferSession::new(Role::Receiver, "croc");
        assert_eq!(event_for_session(&session), TransferEvent::Created);
    }

    #[tokio::test]
    async fn unknown_transfer_notifications_are_ignored() {
        let manager = TransferManager::new(CrocBackend::default());
        let presentations = RwLock::new(HashMap::new());
        let update = build_update(
            &manager,
            &presentations,
            TransferNotification {
                transfer_id: TransferId::new(),
                event: TransferEvent::Created,
            },
        )
        .await;

        assert_eq!(update, None);
        assert!(presentations.read().await.is_empty());
    }

    #[test]
    fn late_events_cannot_reopen_terminal_presentations() {
        assert!(!accepts_event(
            TransferState::Completed,
            &TransferEvent::Progress {
                transferred: 1,
                total: 2,
                speed_bps: 1,
            }
        ));
        assert!(!accepts_event(
            TransferState::Failed,
            &TransferEvent::Completed
        ));
        assert!(accepts_event(
            TransferState::Completed,
            &TransferEvent::Completed
        ));
    }

    #[test]
    fn invalid_progress_does_not_replace_the_last_snapshot() {
        let transfer_id = TransferId::new();
        let session = TransferSession::new(Role::Sender, "croc");
        let mut current = presentation(transfer_id, TransferState::Transferring);
        current.progress = Progress {
            transferred_bytes: 25,
            total_bytes: 100,
            speed_bps: 25,
        };

        assert_eq!(
            apply_event_to_presentation(
                &mut current,
                &session,
                &TransferEvent::Progress {
                    transferred: 10,
                    total: 100,
                    speed_bps: 10,
                },
                true,
            ),
            None
        );
        assert_eq!(current.progress.transferred_bytes, 25);
    }

    #[test]
    fn coalescing_keeps_latest_progress_before_terminal_event() {
        let transfer_id = TransferId::new();
        let (sender, mut receiver) = broadcast::channel(8);
        let mut pending = HashMap::new();

        queue_update(&sender, &mut pending, progress_update(transfer_id, 10));
        queue_update(&sender, &mut pending, progress_update(transfer_id, 20));
        queue_update(
            &sender,
            &mut pending,
            AppTransferUpdate {
                transfer_id,
                event: TransferEvent::Completed,
                presentation: presentation(transfer_id, TransferState::Completed),
            },
        );

        assert_eq!(
            receiver.try_recv().unwrap().event,
            TransferEvent::Progress {
                transferred: 20,
                total: 100,
                speed_bps: 10,
            }
        );
        assert_eq!(receiver.try_recv().unwrap().event, TransferEvent::Completed);
        assert!(pending.is_empty());
    }

    #[test]
    fn high_frequency_progress_keeps_one_latest_update() {
        let transfer_id = TransferId::new();
        let (sender, mut receiver) = broadcast::channel(8);
        let mut pending = HashMap::new();
        for transferred in 1..=100 {
            queue_update(
                &sender,
                &mut pending,
                progress_update(transfer_id, transferred),
            );
        }

        flush_pending(&sender, &mut pending);

        assert_eq!(
            receiver.try_recv().unwrap().event,
            TransferEvent::Progress {
                transferred: 100,
                total: 100,
                speed_bps: 10,
            }
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn lag_refresh_removes_missing_presentations_safely() {
        let manager = TransferManager::new(CrocBackend::default());
        let transfer_id = TransferId::new();
        let presentations = RwLock::new(HashMap::from([(
            transfer_id,
            presentation(transfer_id, TransferState::Connecting),
        )]));
        let (sender, mut receiver) = broadcast::channel(8);
        let mut pending = HashMap::new();

        refresh_sessions(&manager, &presentations, &sender, &mut pending).await;

        assert!(presentations.read().await.is_empty());
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn shutdown_returns_a_closed_receiver() {
        let mut receiver = closed_receiver();
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
    }
}
