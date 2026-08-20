use drift_core::{
    Role, TransferCapability, TransferError, TransferEvent, TransferFailureKind, TransferId,
    TransferManifest, TransferSession, TransferState,
};
use drift_protocol::{
    BackendCapability, BackendError, BackendEvent, ReceiveRequest, SendRequest, TransferBackend,
    TransferHandle,
};
use std::{collections::HashMap, future::Future, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, watch, Mutex, RwLock};
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
    active: Mutex<HashMap<TransferId, ActiveAttempt>>,
    retry_requests: Mutex<HashMap<TransferId, RetryRequest>>,
    events: broadcast::Sender<TransferNotification>,
}

struct ActiveAttempt {
    cancellation: watch::Sender<bool>,
    completion: watch::Sender<bool>,
}

#[derive(Clone)]
enum RetryRequest {
    Send {
        request: SendRequest,
        manifest: Option<TransferManifest>,
    },
    Receive {
        request: ReceiveRequest,
    },
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
                retry_requests: Mutex::new(HashMap::new()),
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

    pub async fn sessions(&self) -> Vec<TransferSession> {
        self.inner.sessions.read().await.values().cloned().collect()
    }

    pub async fn start_send(&self, request: SendRequest) -> Result<TransferId, TransferError> {
        self.start_send_with_manifest(request, None).await
    }

    pub async fn start_send_with_manifest(
        &self,
        request: SendRequest,
        manifest: Option<TransferManifest>,
    ) -> Result<TransferId, TransferError> {
        if let Some(manifest) = &manifest {
            manifest
                .validate()
                .map_err(|_| TransferError::Backend("invalid sender manifest".into()))?;
        }
        self.start_send_attempt(request, manifest).await
    }

    pub async fn start_receive(
        &self,
        request: ReceiveRequest,
    ) -> Result<TransferId, TransferError> {
        self.start_receive_attempt(request).await
    }

    pub async fn cancel(&self, transfer_id: TransferId) -> Result<(), TransferError> {
        let state = self
            .session(transfer_id)
            .await
            .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?
            .state;
        if state == TransferState::Cancelled {
            return Ok(());
        }
        if matches!(state, TransferState::Completed | TransferState::Failed) {
            return Err(TransferError::CancelNotAllowed(state));
        }

        let control = self
            .inner
            .active
            .lock()
            .await
            .get(&transfer_id)
            .map(|attempt| (attempt.cancellation.clone(), attempt.completion.subscribe()));
        let Some((cancellation, completion)) = control else {
            return self.cancel_untracked(transfer_id).await;
        };
        let _ = cancellation.send(true);
        wait_for_completion(completion).await;
        Ok(())
    }

    pub async fn retry(&self, transfer_id: TransferId) -> Result<TransferId, TransferError> {
        self.retry_with_receive_validation(transfer_id, |_output_directory| async { Ok(()) })
            .await
    }

    pub async fn retry_with_receive_validation<F, Fut, E>(
        &self,
        transfer_id: TransferId,
        validate_receive: F,
    ) -> Result<TransferId, E>
    where
        E: From<TransferError> + Send,
        F: FnOnce(PathBuf) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let session = self
            .session(transfer_id)
            .await
            .ok_or_else(|| E::from(TransferError::Backend("transfer session not found".into())))?;
        if session.state != TransferState::Failed
            || !session
                .failure_kind
                .is_some_and(TransferFailureKind::is_retryable)
        {
            return Err(E::from(TransferError::RetryNotAllowed(session.state)));
        }
        let request = self
            .inner
            .retry_requests
            .lock()
            .await
            .get(&transfer_id)
            .cloned()
            .ok_or_else(|| E::from(TransferError::RetryNotAllowed(session.state)))?;
        if let RetryRequest::Receive { request } = request {
            validate_receive(request.output_directory).await?;
        }
        let request = self
            .inner
            .retry_requests
            .lock()
            .await
            .remove(&transfer_id)
            .ok_or_else(|| E::from(TransferError::RetryNotAllowed(session.state)))?;
        match request {
            RetryRequest::Send { request, manifest } => self
                .start_send_attempt(request, manifest)
                .await
                .map_err(E::from),
            RetryRequest::Receive { request } => {
                self.start_receive_attempt(request).await.map_err(E::from)
            }
        }
    }

    pub async fn update_progress(
        &self,
        transfer_id: TransferId,
        transferred: u64,
        total: u64,
        speed_bps: u64,
    ) -> Result<(), TransferError> {
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(&transfer_id)
            .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
        if !matches!(session.state, TransferState::Transferring | TransferState::Resuming) {
            return Err(TransferError::ProgressNotAllowed(session.state));
        }
        session.update_progress_with_total(transferred, total, speed_bps)?;
        drop(sessions);
        self.emit(
            transfer_id,
            TransferEvent::Progress {
                transferred,
                total,
                speed_bps,
            },
        );
        Ok(())
    }

    async fn start_send_attempt(
        &self,
        request: SendRequest,
        manifest: Option<TransferManifest>,
    ) -> Result<TransferId, TransferError> {
        let retry_request = RetryRequest::Send {
            request: request.clone(),
            manifest: manifest.clone(),
        };
        let (transfer_id, cancellation) = self
            .create_active_session(Role::Sender, retry_request, manifest, None)
            .await;
        if cancellation_requested(&cancellation) {
            self.finish_attempt(transfer_id, Err(BackendError::Cancelled), true)
                .await;
            return Ok(transfer_id);
        }
        self.advance(
            transfer_id,
            TransferState::Connecting,
            TransferEvent::Connecting,
        )
        .await?;
        let handle_result = tokio::select! {
            result = self.inner.backend.send(request) => result,
            _ = wait_for_cancellation(cancellation.clone()) => Err(BackendError::Cancelled),
        };
        let mut handle = match handle_result {
            Ok(handle) => handle,
            Err(error) => {
                let cancelled = cancellation_requested(&cancellation);
                self.finish_attempt(transfer_id, Err(error), cancelled)
                    .await;
                return Ok(transfer_id);
            }
        };
        if cancellation_requested(&cancellation) {
            let _ = handle.cancel().await;
            self.finish_attempt(transfer_id, Err(BackendError::Cancelled), true)
                .await;
            return Ok(transfer_id);
        }
        for (state, event) in [
            (TransferState::Connected, TransferEvent::Connected),
            (TransferState::Authenticating, TransferEvent::Authenticating),
            (TransferState::Negotiating, TransferEvent::Negotiating),
            (TransferState::Transferring, TransferEvent::Started),
        ] {
            if cancellation_requested(&cancellation) {
                let _ = handle.cancel().await;
                self.finish_attempt(transfer_id, Err(BackendError::Cancelled), true)
                    .await;
                return Ok(transfer_id);
            }
            self.advance(transfer_id, state, event).await?;
        }
        self.track_and_monitor(transfer_id, handle, cancellation)
            .await;
        Ok(transfer_id)
    }

    async fn start_receive_attempt(
        &self,
        request: ReceiveRequest,
    ) -> Result<TransferId, TransferError> {
        let retry_request = RetryRequest::Receive {
            request: request.clone(),
        };
        let (transfer_id, cancellation) = self
            .create_active_session(
                Role::Receiver,
                retry_request,
                None,
                Some(request.code.clone()),
            )
            .await;
        self.emit(transfer_id, TransferEvent::CodeAvailable);
        if cancellation_requested(&cancellation) {
            self.finish_attempt(transfer_id, Err(BackendError::Cancelled), true)
                .await;
            return Ok(transfer_id);
        }
        self.advance(
            transfer_id,
            TransferState::Connecting,
            TransferEvent::Connecting,
        )
        .await?;
        let handle_result = tokio::select! {
            result = self.inner.backend.receive(request) => result,
            _ = wait_for_cancellation(cancellation.clone()) => Err(BackendError::Cancelled),
        };
        let mut handle = match handle_result {
            Ok(handle) => handle,
            Err(error) => {
                let cancelled = cancellation_requested(&cancellation);
                self.finish_attempt(transfer_id, Err(error), cancelled)
                    .await;
                return Ok(transfer_id);
            }
        };
        if cancellation_requested(&cancellation) {
            let _ = handle.cancel().await;
            self.finish_attempt(transfer_id, Err(BackendError::Cancelled), true)
                .await;
            return Ok(transfer_id);
        }
        for (state, event) in [
            (TransferState::Connected, TransferEvent::Connected),
            (TransferState::Authenticating, TransferEvent::Authenticating),
            (TransferState::Negotiating, TransferEvent::Negotiating),
            (TransferState::Transferring, TransferEvent::Started),
        ] {
            if cancellation_requested(&cancellation) {
                let _ = handle.cancel().await;
                self.finish_attempt(transfer_id, Err(BackendError::Cancelled), true)
                    .await;
                return Ok(transfer_id);
            }
            self.advance(transfer_id, state, event).await?;
        }
        self.track_and_monitor(transfer_id, handle, cancellation)
            .await;
        Ok(transfer_id)
    }

    async fn create_active_session(
        &self,
        role: Role,
        retry_request: RetryRequest,
        manifest: Option<TransferManifest>,
        code: Option<String>,
    ) -> (TransferId, watch::Receiver<bool>) {
        let mut session = TransferSession::new(role, self.inner.backend_name.clone());
        if let Some(mut manifest) = manifest {
            manifest.transfer_id = session.id;
            session.set_manifest(manifest);
        }
        if let Some(code) = code {
            session.set_code(code);
        }
        let transfer_id = session.id;
        let (cancellation, cancellation_receiver) = watch::channel(false);
        let (completion, _) = watch::channel(false);
        self.inner.active.lock().await.insert(
            transfer_id,
            ActiveAttempt {
                cancellation,
                completion,
            },
        );
        self.inner
            .retry_requests
            .lock()
            .await
            .insert(transfer_id, retry_request);
        self.inner
            .sessions
            .write()
            .await
            .insert(transfer_id, session);
        self.emit(transfer_id, TransferEvent::Created);
        (transfer_id, cancellation_receiver)
    }

    #[cfg(test)]
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

    async fn track_and_monitor(
        &self,
        transfer_id: TransferId,
        mut handle: TransferHandle,
        cancellation: watch::Receiver<bool>,
    ) {
        let mut updates = handle.take_updates();
        let manager = Self {
            inner: Arc::clone(&self.inner),
        };
        tokio::spawn(async move {
            let cancellation_wait = wait_for_cancellation(cancellation.clone());
            let mut completion = Box::pin(handle.wait_with_cancel_signal(cancellation_wait));
            let mut update_error = None;
            let result = loop {
                let Some(receiver) = updates.as_mut() else {
                    break completion.await;
                };
                tokio::select! {
                    result = &mut completion => break result,
                    event = receiver.recv() => match event {
                        Some(event) => {
                            if let Err(error) = manager.apply_backend_event(transfer_id, event).await {
                                update_error = Some(error);
                            }
                        }
                        None => updates = None,
                    },
                }
            };
            if let Some(receiver) = updates.as_mut() {
                while let Ok(event) = receiver.try_recv() {
                    if let Err(error) = manager.apply_backend_event(transfer_id, event).await {
                        update_error = Some(error);
                    }
                }
            }
            let result = match update_error {
                Some(error) => Err(error),
                None => result,
            };
            let cancelled = cancellation_requested(&cancellation)
                || matches!(&result, Err(BackendError::Cancelled));
            manager.finish_attempt(transfer_id, result, cancelled).await;
        });
    }

    async fn apply_backend_event(
        &self,
        transfer_id: TransferId,
        event: BackendEvent,
    ) -> Result<(), BackendError> {
        match event {
            BackendEvent::CodeGenerated { code } => {
                self.store_code(transfer_id, code).await;
            }
            BackendEvent::MetadataReady => {
                let state = self.session(transfer_id).await.map(|session| session.state);
                if state == Some(TransferState::Negotiating) {
                    self.advance(
                        transfer_id,
                        TransferState::Transferring,
                        TransferEvent::MetadataReady,
                    )
                    .await
                    .map_err(|_| BackendError::OutputParse {
                        stream: "metadata",
                        reason: "invalid metadata state",
                    })?;
                }
            }
            BackendEvent::Progress {
                transferred,
                total,
                speed_bps,
            } => {
                match self
                    .update_progress(transfer_id, transferred, total, speed_bps)
                    .await
                {
                    Ok(()) => {}
                    Err(error @ TransferError::InvalidProgress(_)) => {
                        warn!(%transfer_id, %error, "invalid backend progress event");
                        return Err(BackendError::OutputParse {
                            stream: "progress",
                            reason: "invalid progress update",
                        });
                    }
                    Err(error @ TransferError::ProgressNotAllowed(_)) => {
                        warn!(%transfer_id, %error, "ignored invalid or late progress event");
                    }
                    Err(_) => {
                        return Err(BackendError::OutputParse {
                            stream: "progress",
                            reason: "missing transfer session",
                        });
                    }
                }
            }
            BackendEvent::CapabilityUnavailable { capability } => {
                let capability = match capability {
                    BackendCapability::Progress => TransferCapability::Progress,
                };
                self.emit(
                    transfer_id,
                    TransferEvent::CapabilityUnavailable { capability },
                );
            }
        }
        Ok(())
    }

    async fn store_code(&self, transfer_id: TransferId, code: String) {
        let mut sessions = self.inner.sessions.write().await;
        let Some(session) = sessions.get_mut(&transfer_id) else {
            return;
        };
        session.set_code(code);
        drop(sessions);
        self.emit(transfer_id, TransferEvent::CodeAvailable);
    }

    #[cfg(test)]
    async fn finish(
        &self,
        transfer_id: TransferId,
        result: Result<drift_protocol::TransferOutput, BackendError>,
    ) {
        self.finish_attempt(transfer_id, result, false).await;
    }

    async fn finish_attempt(
        &self,
        transfer_id: TransferId,
        result: Result<drift_protocol::TransferOutput, BackendError>,
        cancelled: bool,
    ) {
        if cancelled || matches!(&result, Err(BackendError::Cancelled)) {
            self.cancel_untracked(transfer_id).await.ok();
            self.cleanup_attempt(transfer_id, false).await;
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
                    let _ = self
                        .fail(
                            transfer_id,
                            BackendError::OutputParse {
                                stream: "state",
                                reason: "invalid verification state",
                            },
                        )
                        .await;
                    self.cleanup_attempt(transfer_id, false).await;
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
                self.cleanup_attempt(transfer_id, false).await;
            }
            Err(error) => {
                let message = error.safe_message();
                warn!(%transfer_id, error = %message, "transfer backend failed");
                let _ = self.fail(transfer_id, error).await;
                let retryable = self
                    .session(transfer_id)
                    .await
                    .and_then(|session| session.failure_kind)
                    .is_some_and(TransferFailureKind::is_retryable);
                self.cleanup_attempt(transfer_id, retryable).await;
            }
        }
    }

    async fn cleanup_attempt(&self, transfer_id: TransferId, keep_retry_request: bool) {
        let completion = self
            .inner
            .active
            .lock()
            .await
            .remove(&transfer_id)
            .map(|attempt| attempt.completion);
        if !keep_retry_request {
            self.inner.retry_requests.lock().await.remove(&transfer_id);
        }
        if let Some(completion) = completion {
            let _ = completion.send(true);
        }
    }

    async fn cancel_untracked(&self, transfer_id: TransferId) -> Result<(), TransferError> {
        let cancelled = {
            let mut sessions = self.inner.sessions.write().await;
            let session = sessions
                .get_mut(&transfer_id)
                .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
            if session.state == TransferState::Cancelled {
                false
            } else if matches!(
                session.state,
                TransferState::Completed | TransferState::Failed
            ) {
                return Err(TransferError::CancelNotAllowed(session.state));
            } else {
                session.transition(TransferState::Cancelled)?;
                true
            }
        };
        if cancelled {
            self.inner.retry_requests.lock().await.remove(&transfer_id);
            self.emit(transfer_id, TransferEvent::Cancelled);
        }
        Ok(())
    }

    async fn fail(
        &self,
        transfer_id: TransferId,
        error: BackendError,
    ) -> Result<TransferId, TransferError> {
        let message = error.safe_message();
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(&transfer_id)
            .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
        session.transition(TransferState::Failed)?;
        session.error = Some(message);
        session.failure_kind = Some(error.failure_kind());
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

async fn wait_for_cancellation(mut cancellation: watch::Receiver<bool>) {
    while !*cancellation.borrow() {
        if cancellation.changed().await.is_err() {
            return;
        }
    }
}

fn cancellation_requested(cancellation: &watch::Receiver<bool>) -> bool {
    *cancellation.borrow()
}

async fn wait_for_completion(mut completion: watch::Receiver<bool>) {
    while !*completion.borrow() {
        if completion.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use drift_core::{
        FileEntry, ProgressError, StateTransitionError, TransferCapability, TransferManifest,
    };
    use drift_protocol::CrocBackend;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT_SCRIPT_ID: AtomicU64 = AtomicU64::new(0);

    #[cfg(unix)]
    fn write_script(body: &str) -> PathBuf {
        use std::{fs, os::unix::fs::PermissionsExt};

        let suffix = NEXT_SCRIPT_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "drift-transfer-test-{}-{suffix}",
            std::process::id()
        ));
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    #[cfg(unix)]
    fn versioned_script(body: &str) -> PathBuf {
        write_script(&format!(
            "if [ \"$1\" = \"--version\" ]; then printf 'v11.2.2-build\\n'; exit 0; fi\n{body}"
        ))
    }

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

    #[tokio::test]
    async fn validates_progress_order_and_ignores_late_updates() {
        let manager = TransferManager::new(CrocBackend::default());
        let transfer_id = manager.create_session(Role::Sender).await;

        assert_eq!(
            manager.update_progress(transfer_id, 1, 10, 1).await,
            Err(TransferError::ProgressNotAllowed(TransferState::Created))
        );

        for (state, event) in [
            (TransferState::Connecting, TransferEvent::Connecting),
            (TransferState::Connected, TransferEvent::Connected),
            (TransferState::Authenticating, TransferEvent::Authenticating),
            (TransferState::Negotiating, TransferEvent::Negotiating),
            (TransferState::Transferring, TransferEvent::Started),
        ] {
            manager.advance(transfer_id, state, event).await.unwrap();
        }

        manager
            .update_progress(transfer_id, 4, 10, 2)
            .await
            .unwrap();
        assert_eq!(
            manager.session(transfer_id).await.unwrap().progress.transferred_bytes,
            4
        );
        assert_eq!(
            manager.update_progress(transfer_id, 3, 10, 2).await,
            Err(TransferError::InvalidProgress(
                ProgressError::TransferredDecreased
            ))
        );
        assert_eq!(
            manager.update_progress(transfer_id, 5, 11, 2).await,
            Err(TransferError::InvalidProgress(ProgressError::TotalChanged))
        );

        manager
            .advance(
                transfer_id,
                TransferState::Verifying,
                TransferEvent::Verifying,
            )
            .await
            .unwrap();
        manager
            .advance(
                transfer_id,
                TransferState::Completed,
                TransferEvent::Completed,
            )
            .await
            .unwrap();
        assert_eq!(
            manager.update_progress(transfer_id, 10, 10, 2).await,
            Err(TransferError::ProgressNotAllowed(TransferState::Completed))
        );
        assert_eq!(
            manager.session(transfer_id).await.unwrap().progress.transferred_bytes,
            4
        );
    }

    #[tokio::test]
    async fn fails_transfer_when_backend_total_conflicts_with_manifest() {
        let manager = TransferManager::new(CrocBackend::default());
        let transfer_id = manager.create_session(Role::Sender).await;
        let manifest =
            TransferManifest::new(transfer_id, vec![FileEntry::new("file.txt", 10).unwrap()])
                .unwrap();
        manager
            .inner
            .sessions
            .write()
            .await
            .get_mut(&transfer_id)
            .unwrap()
            .set_manifest(manifest);

        for (state, event) in [
            (TransferState::Connecting, TransferEvent::Connecting),
            (TransferState::Connected, TransferEvent::Connected),
            (TransferState::Authenticating, TransferEvent::Authenticating),
            (TransferState::Negotiating, TransferEvent::Negotiating),
            (TransferState::Transferring, TransferEvent::Started),
        ] {
            manager.advance(transfer_id, state, event).await.unwrap();
        }

        let error = manager
            .apply_backend_event(
                transfer_id,
                BackendEvent::Progress {
                    transferred: 1,
                    total: 11,
                    speed_bps: 1,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            BackendError::OutputParse {
                stream: "progress",
                reason: "invalid progress update",
            }
        ));

        manager.finish(transfer_id, Err(error)).await;
        let session = manager.session(transfer_id).await.unwrap();
        assert_eq!(session.state, TransferState::Failed);
        assert_eq!(session.progress.transferred_bytes, 0);
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

    #[cfg(unix)]
    #[tokio::test]
    async fn propagates_code_and_capability_without_leaking_code_in_events() {
        let script = versioned_script("sleep 0.05; printf 'Code is: manager-code\\n' >&2");
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap();
        let mut code_available = false;
        let mut capability_unavailable = false;
        let mut completed = false;
        while !completed {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            match notification.event {
                TransferEvent::CodeAvailable => code_available = true,
                TransferEvent::CapabilityUnavailable {
                    capability: TransferCapability::Progress,
                } => capability_unavailable = true,
                TransferEvent::Completed => completed = true,
                _ => {}
            }
        }
        let session = manager.session(transfer_id).await.unwrap();
        assert!(code_available);
        assert!(capability_unavailable);
        assert_eq!(session.code.as_deref(), Some("manager-code"));
        assert_eq!(session.state, TransferState::Completed);
        let debug = format!("{session:?}");
        assert!(!debug.contains("manager-code"));
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stores_validated_sender_manifest_on_the_transfer_session() {
        let script = versioned_script("sleep 0.05; printf 'Code is: manifest-code\\n' >&2");
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let manifest = TransferManifest::new(
            TransferId::new(),
            vec![FileEntry::new("folder/file.txt", 4).unwrap()],
        )
        .unwrap();

        let transfer_id = manager
            .start_send_with_manifest(
                SendRequest::new(vec![PathBuf::from("folder")]).unwrap(),
                Some(manifest),
            )
            .await
            .unwrap();
        loop {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if notification.transfer_id == transfer_id
                && notification.event == TransferEvent::Completed
            {
                break;
            }
        }
        let session = manager.session(transfer_id).await.unwrap();
        let session_manifest = session.manifest.as_ref().unwrap();

        assert_eq!(session_manifest.transfer_id, transfer_id);
        assert_eq!(session_manifest.total_size, 4);
        assert_eq!(
            session_manifest.files[0].relative_path,
            PathBuf::from("folder/file.txt")
        );

        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stores_safe_message_for_backend_failure() {
        let script = versioned_script("printf 'private diagnostic\\n' >&2; exit 7");
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap();
        loop {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if notification.event == TransferEvent::Failed {
                break;
            }
        }
        let session = manager.session(transfer_id).await.unwrap();
        assert_eq!(session.error.as_deref(), Some("croc process failed"));
        assert!(!session
            .error
            .as_deref()
            .unwrap()
            .contains("private diagnostic"));
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_reaps_process_and_emits_one_terminal_event() {
        let script = versioned_script("sleep 30");
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap();

        manager.cancel(transfer_id).await.unwrap();
        manager.cancel(transfer_id).await.unwrap();

        assert_eq!(
            manager.session(transfer_id).await.unwrap().state,
            TransferState::Cancelled
        );
        assert!(manager.inner.active.lock().await.is_empty());
        let terminal_events = std::iter::from_fn(|| events.try_recv().ok())
            .filter(|notification| {
                notification.transfer_id == transfer_id
                    && matches!(
                        notification.event,
                        TransferEvent::Completed | TransferEvent::Failed | TransferEvent::Cancelled
                    )
            })
            .count();
        assert_eq!(terminal_events, 1);

        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retryable_failure_starts_new_attempt_with_same_safe_request() {
        let script = versioned_script("sleep 1");
        let manager = TransferManager::new(
            CrocBackend::new(&script).with_timeout(std::time::Duration::from_millis(50)),
        );
        let mut events = manager.subscribe();
        let old_transfer_id = manager
            .start_send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap();
        loop {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if notification.transfer_id == old_transfer_id
                && notification.event == TransferEvent::Failed
            {
                break;
            }
        }

        let old_session = manager.session(old_transfer_id).await.unwrap();
        assert_eq!(old_session.state, TransferState::Failed);
        assert_eq!(old_session.failure_kind, Some(TransferFailureKind::Network));
        let new_transfer_id = manager.retry(old_transfer_id).await.unwrap();
        assert_ne!(old_transfer_id, new_transfer_id);
        assert_eq!(
            manager.session(old_transfer_id).await.unwrap().state,
            TransferState::Failed
        );

        manager.cancel(new_transfer_id).await.unwrap();
        assert!(manager.inner.active.lock().await.is_empty());
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_failure_is_not_automatically_retryable() {
        let script = versioned_script("exit 7");
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap();
        loop {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if notification.transfer_id == transfer_id
                && notification.event == TransferEvent::Failed
            {
                break;
            }
        }

        assert_eq!(
            manager.session(transfer_id).await.unwrap().failure_kind,
            Some(TransferFailureKind::ProcessFailure)
        );
        assert_eq!(
            manager.retry(transfer_id).await,
            Err(TransferError::RetryNotAllowed(TransferState::Failed))
        );
        let _ = std::fs::remove_file(script);
    }
}
