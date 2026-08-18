use drift_core::{Role, TransferError, TransferEvent, TransferId, TransferSession, TransferState};
use drift_protocol::{BackendError, ReceiveRequest, SendRequest, TransferBackend, TransferHandle};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{broadcast, oneshot, Mutex, RwLock};
use tracing::{debug, warn};

#[derive(Debug, Clone, PartialEq)]
pub struct TransferNotification {
    pub transfer_id: TransferId,
    pub event: TransferEvent,
}

struct ManagerInner<B> {
    backend: Arc<B>,
    backend_name: String,
    sessions: RwLock<HashMap<TransferId, TransferSession>>,
    active: Mutex<HashMap<TransferId, oneshot::Sender<()>>>,
    cancelled: Mutex<HashSet<TransferId>>,
    events: broadcast::Sender<TransferNotification>,
}

#[derive(Clone)]
pub struct TransferManager<B> {
    inner: Arc<ManagerInner<B>>,
}

impl<B> TransferManager<B>
where
    B: TransferBackend + 'static,
{
    pub fn new(backend: B) -> Self {
        Self::with_backend_name(backend, "croc")
    }

    pub fn with_backend_name(backend: B, backend_name: impl Into<String>) -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            inner: Arc::new(ManagerInner {
                backend: Arc::new(backend),
                backend_name: backend_name.into(),
                sessions: RwLock::new(HashMap::new()),
                active: Mutex::new(HashMap::new()),
                cancelled: Mutex::new(HashSet::new()),
                events,
            }),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TransferNotification> {
        self.inner.events.subscribe()
    }

    pub async fn session(&self, transfer_id: TransferId) -> Option<TransferSession> {
        self.inner.sessions.read().await.get(&transfer_id).cloned()
    }

    pub async fn start_send(&self, request: SendRequest) -> Result<TransferId, TransferError> {
        let transfer_id = self.create_session(Role::Sender).await;
        self.advance(
            transfer_id,
            TransferState::Connecting,
            TransferEvent::Connecting,
        )
        .await?;
        let handle = match self.inner.backend.send(request).await {
            Ok(handle) => handle,
            Err(error) => return self.fail(transfer_id, error).await,
        };
        self.emit(transfer_id, TransferEvent::Connected);
        self.advance(
            transfer_id,
            TransferState::Authenticating,
            TransferEvent::Authenticating,
        )
        .await?;
        self.advance(
            transfer_id,
            TransferState::Negotiating,
            TransferEvent::MetadataReady,
        )
        .await?;
        self.advance(
            transfer_id,
            TransferState::Transferring,
            TransferEvent::Progress {
                transferred: 0,
                total: 0,
                speed_bps: 0,
            },
        )
        .await?;
        self.track_and_monitor(transfer_id, handle).await;
        Ok(transfer_id)
    }

    pub async fn start_receive(
        &self,
        request: ReceiveRequest,
    ) -> Result<TransferId, TransferError> {
        let transfer_id = self.create_session(Role::Receiver).await;
        self.advance(
            transfer_id,
            TransferState::Connecting,
            TransferEvent::Connecting,
        )
        .await?;
        let handle = match self.inner.backend.receive(request).await {
            Ok(handle) => handle,
            Err(error) => return self.fail(transfer_id, error).await,
        };
        self.emit(transfer_id, TransferEvent::Connected);
        self.advance(
            transfer_id,
            TransferState::Authenticating,
            TransferEvent::Authenticating,
        )
        .await?;
        self.advance(
            transfer_id,
            TransferState::Negotiating,
            TransferEvent::MetadataReady,
        )
        .await?;
        self.advance(
            transfer_id,
            TransferState::Transferring,
            TransferEvent::Progress {
                transferred: 0,
                total: 0,
                speed_bps: 0,
            },
        )
        .await?;
        self.track_and_monitor(transfer_id, handle).await;
        Ok(transfer_id)
    }

    pub async fn cancel(&self, transfer_id: TransferId) -> Result<(), TransferError> {
        self.inner.cancelled.lock().await.insert(transfer_id);
        let active = self.inner.active.lock().await.remove(&transfer_id);
        if let Some(cancel) = active {
            let _ = cancel.send(());
        }
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(&transfer_id)
            .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
        session.transition(TransferState::Cancelled)?;
        drop(sessions);
        self.emit(transfer_id, TransferEvent::Cancelled);
        Ok(())
    }

    async fn create_session(&self, role: Role) -> TransferId {
        let session = TransferSession::new(role, self.inner.backend_name.clone());
        let transfer_id = session.id;
        self.inner
            .sessions
            .write()
            .await
            .insert(transfer_id, session);
        self.emit(transfer_id, TransferEvent::Created);
        transfer_id
    }

    async fn track_and_monitor(&self, transfer_id: TransferId, handle: TransferHandle) {
        let (cancel, cancellation) = oneshot::channel();
        let manager = Self {
            inner: Arc::clone(&self.inner),
        };
        self.inner.active.lock().await.insert(transfer_id, cancel);
        tokio::spawn(async move {
            let result = handle.wait_with_cancel(cancellation).await;
            manager.finish(transfer_id, result).await;
        });
    }

    async fn finish(
        &self,
        transfer_id: TransferId,
        result: Result<drift_protocol::TransferOutput, BackendError>,
    ) {
        self.inner.active.lock().await.remove(&transfer_id);
        if self.inner.cancelled.lock().await.remove(&transfer_id) {
            return;
        }
        match result {
            Ok(_) => {
                if let Err(error) = self
                    .advance(
                        transfer_id,
                        TransferState::Verifying,
                        TransferEvent::Verifying,
                    )
                    .await
                {
                    warn!(%transfer_id, %error, "failed to enter verification state");
                    return;
                }
                if let Err(error) = self
                    .advance(
                        transfer_id,
                        TransferState::Completed,
                        TransferEvent::Completed,
                    )
                    .await
                {
                    warn!(%transfer_id, %error, "failed to complete transfer");
                }
            }
            Err(error) => {
                warn!(%transfer_id, error = %error, "transfer backend failed");
                let _ = self.fail(transfer_id, error).await;
            }
        }
    }

    async fn fail(
        &self,
        transfer_id: TransferId,
        error: BackendError,
    ) -> Result<TransferId, TransferError> {
        let message = error.to_string();
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(&transfer_id)
            .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
        session.transition(TransferState::Failed)?;
        session.error = Some(message);
        drop(sessions);
        self.emit(transfer_id, TransferEvent::Failed);
        Ok(transfer_id)
    }

    async fn advance(
        &self,
        transfer_id: TransferId,
        next: TransferState,
        event: TransferEvent,
    ) -> Result<(), TransferError> {
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(&transfer_id)
            .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
        session.transition(next).map_err(TransferError::from)?;
        drop(sessions);
        self.emit(transfer_id, event);
        Ok(())
    }

    fn emit(&self, transfer_id: TransferId, event: TransferEvent) {
        let _ = self
            .inner
            .events
            .send(TransferNotification { transfer_id, event });
        debug!(%transfer_id, "transfer event emitted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use drift_core::StateTransitionError;
    use drift_protocol::CrocBackend;

    #[tokio::test]
    async fn serializes_state_changes_and_events() {
        let manager = TransferManager::new(CrocBackend::default());
        let mut events = manager.subscribe();
        let transfer_id = manager.create_session(Role::Sender).await;
        manager
            .advance(
                transfer_id,
                TransferState::Connecting,
                TransferEvent::Connecting,
            )
            .await
            .unwrap();
        assert_eq!(events.recv().await.unwrap().event, TransferEvent::Created);
        assert_eq!(
            events.recv().await.unwrap().event,
            TransferEvent::Connecting
        );
        assert_eq!(
            manager.session(transfer_id).await.unwrap().state,
            TransferState::Connecting
        );
    }

    #[tokio::test]
    async fn cancels_registered_session() {
        let manager = TransferManager::new(CrocBackend::default());
        let mut events = manager.subscribe();
        let transfer_id = manager.create_session(Role::Receiver).await;
        manager.cancel(transfer_id).await.unwrap();
        assert_eq!(
            manager.session(transfer_id).await.unwrap().state,
            TransferState::Cancelled
        );
        assert_eq!(events.recv().await.unwrap().event, TransferEvent::Created);
        assert_eq!(events.recv().await.unwrap().event, TransferEvent::Cancelled);
    }

    #[test]
    fn keeps_state_transition_error_in_domain_layer() {
        let error = StateTransitionError {
            from: TransferState::Completed,
            to: TransferState::Failed,
        };
        assert_eq!(
            TransferError::from(error).to_string(),
            "invalid state transition"
        );
    }
}
