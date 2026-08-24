use drift_core::{
    ResumeCapabilities, ResumeRequest, ResumeState, Role, TransferCapability, TransferError,
    TransferEvent, TransferFailureKind, TransferId, TransferManifest, TransferSession,
    TransferState, DEFAULT_RESUME_CHUNK_SIZE, RESUME_SCHEMA_VERSION,
};
use drift_protocol::{
    BackendCancellation, BackendCapabilities, BackendCapability, BackendError, BackendEvent,
    BackendProtocolError, ReceiveRequest, SendRequest, TransferBackend, TransferHandle,
};
use drift_storage::{
    DestinationError, JsonStore, ReceiveStaging, ReceiveStagingError, StorageError,
};
use std::{collections::HashMap, future::Future, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, watch, Mutex, RwLock};
use tracing::{debug, warn};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct TransferNotification {
    pub transfer_id: TransferId,
    pub event: TransferEvent,
}

struct ManagerInner {
    backend: Arc<dyn TransferBackend>,
    backend_name: String,
    sessions: RwLock<HashMap<TransferId, TransferSession>>,
    completed_receive_destinations: RwLock<HashMap<TransferId, PathBuf>>,
    active: Mutex<HashMap<TransferId, ActiveAttempt>>,
    retry_requests: Mutex<HashMap<TransferId, RetryRequest>>,
    resume_store: Option<JsonStore>,
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
        staging: ReceiveStaging,
    },
}

#[derive(Clone)]
pub struct TransferManager {
    inner: Arc<ManagerInner>,
}

impl TransferManager {
    pub fn new<B>(backend: B) -> Self
    where
        B: TransferBackend + 'static,
    {
        let backend_name = backend.info().name.to_owned();
        Self::with_backend_arc(Arc::new(backend), backend_name, None)
    }

    pub fn with_backend_name<B>(backend: B, backend_name: impl Into<String>) -> Self
    where
        B: TransferBackend + 'static,
    {
        Self::with_backend_arc(Arc::new(backend), backend_name, None)
    }

    pub fn with_resume_store<B>(
        backend: B,
        backend_name: impl Into<String>,
        resume_store: JsonStore,
    ) -> Self
    where
        B: TransferBackend + 'static,
    {
        Self::with_backend_arc(Arc::new(backend), backend_name, Some(resume_store))
    }

    pub fn with_resume_store_arc(
        backend: Arc<dyn TransferBackend>,
        backend_name: impl Into<String>,
        resume_store: JsonStore,
    ) -> Self {
        Self::with_backend_arc(backend, backend_name, Some(resume_store))
    }

    fn with_backend_arc(
        backend: Arc<dyn TransferBackend>,
        backend_name: impl Into<String>,
        resume_store: Option<JsonStore>,
    ) -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            inner: Arc::new(ManagerInner {
                backend,
                backend_name: backend_name.into(),
                sessions: RwLock::new(HashMap::new()),
                completed_receive_destinations: RwLock::new(HashMap::new()),
                active: Mutex::new(HashMap::new()),
                retry_requests: Mutex::new(HashMap::new()),
                resume_store,
                events,
            }),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TransferNotification> {
        self.inner.events.subscribe()
    }

    pub fn capabilities(&self) -> BackendCapabilities {
        self.inner.backend.capabilities()
    }

    pub fn backend_info(&self) -> drift_protocol::BackendInfo {
        self.inner.backend.info()
    }

    pub fn backend_version(&self) -> Option<&'static str> {
        self.inner.backend.info().version
    }

    pub async fn pause(&self, transfer_id: TransferId) -> Result<(), TransferError> {
        self.require_capability(transfer_id, TransferCapability::Pause)
            .await
    }

    pub async fn resume(&self, transfer_id: TransferId) -> Result<(), TransferError> {
        self.require_capability(transfer_id, TransferCapability::Resume)
            .await
    }

    pub async fn session(&self, transfer_id: TransferId) -> Option<TransferSession> {
        self.inner.sessions.read().await.get(&transfer_id).cloned()
    }

    pub async fn sessions(&self) -> Vec<TransferSession> {
        self.inner.sessions.read().await.values().cloned().collect()
    }

    pub async fn completed_receive_destination(&self, transfer_id: TransferId) -> Option<PathBuf> {
        self.inner
            .completed_receive_destinations
            .read()
            .await
            .get(&transfer_id)
            .cloned()
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
        if let RetryRequest::Receive { request, .. } = request {
            validate_receive(request.output_directory).await?;
        }
        let request = self
            .inner
            .retry_requests
            .lock()
            .await
            .remove(&transfer_id)
            .ok_or_else(|| E::from(TransferError::RetryNotAllowed(session.state)))?;
        let result = match request {
            RetryRequest::Send { request, manifest } => self
                .start_send_attempt(request, manifest)
                .await
                .map_err(E::from),
            RetryRequest::Receive { request, staging } => {
                let retry_request = RetryRequest::Receive {
                    request: request.clone(),
                    staging: staging.clone(),
                };
                let result = self.start_receive_attempt(request).await.map_err(E::from);
                if result.is_ok() {
                    if let Err(error) = staging.cleanup().await {
                        warn!(
                            error = %safe_receive_staging_error(&error),
                            "failed to clean superseded receive staging"
                        );
                    }
                } else {
                    self.inner
                        .retry_requests
                        .lock()
                        .await
                        .insert(transfer_id, retry_request);
                }
                result
            }
        };
        if result.is_ok() {
            self.remove_recovery_metadata(transfer_id).await;
        }
        result
    }

    pub async fn recover(
        &self,
        state: ResumeState,
        receive_code: Option<String>,
        receive_output_directory: Option<PathBuf>,
    ) -> Result<TransferId, TransferError> {
        state
            .validate()
            .map_err(|_| TransferError::Backend("invalid resume metadata".into()))?;
        if state.backend != self.inner.backend_name
            || state.backend_version.as_deref() != self.inner.backend.version()
        {
            return Err(TransferError::Backend(
                "resume backend is incompatible".into(),
            ));
        }
        if state.capabilities.resume
            != self
                .inner
                .backend
                .capabilities()
                .supports(BackendCapability::Resume)
        {
            return Err(TransferError::CapabilityUnavailable(
                TransferCapability::Resume,
            ));
        }
        let old_transfer_id = state.transfer_id;
        let result = match state.request {
            ResumeRequest::Send { source_paths } => {
                let request = SendRequest::new(source_paths)
                    .map_err(|_| TransferError::Backend("invalid resume request".into()))?;
                self.start_send_with_manifest(request, state.manifest).await
            }
            ResumeRequest::Receive { output_directory } => {
                let code = receive_code.ok_or(TransferError::Backend(
                    "receive recovery requires transfer code".into(),
                ))?;
                let output_directory = receive_output_directory.unwrap_or(output_directory);
                let request = ReceiveRequest::new(code, output_directory)
                    .map_err(|_| TransferError::Backend("invalid resume request".into()))?;
                self.start_receive(request).await
            }
        };
        if result.is_ok() {
            self.discard_recovery_data(old_transfer_id).await;
        }
        result
    }

    pub async fn discard_recovery(&self, transfer_id: TransferId) -> Result<(), TransferError> {
        let Some(store) = &self.inner.resume_store else {
            return Ok(());
        };
        store
            .discard_resume(transfer_id)
            .await
            .map_err(map_storage_error)
    }

    async fn remove_recovery_metadata(&self, transfer_id: TransferId) {
        if let Some(store) = &self.inner.resume_store {
            if let Err(error) = store.remove_resume(transfer_id).await {
                warn!(%transfer_id, error = %safe_storage_error(&error), "failed to clear recovery metadata");
            }
        }
    }

    async fn discard_recovery_data(&self, transfer_id: TransferId) {
        if let Some(store) = &self.inner.resume_store {
            if let Err(error) = store.discard_resume(transfer_id).await {
                warn!(%transfer_id, error = %safe_storage_error(&error), "failed to clear recovery metadata");
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
        if !matches!(
            session.state,
            TransferState::Transferring | TransferState::Resuming
        ) {
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
        let staging = ReceiveStaging::create(&request.output_directory)
            .await
            .map_err(map_receive_staging_creation_error)?;
        let retry_request = RetryRequest::Receive {
            request: request.clone(),
            staging: staging.clone(),
        };
        let backend_request = ReceiveRequest {
            output_directory: staging.path().to_path_buf(),
            ..request.clone()
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
        if self
            .advance(
                transfer_id,
                TransferState::Connecting,
                TransferEvent::Connecting,
            )
            .await
            .is_err()
        {
            warn!(%transfer_id, "failed to start receive transfer");
            if self
                .fail_with(
                    transfer_id,
                    "receive could not start",
                    TransferFailureKind::Unknown,
                )
                .await
                .is_err()
            {
                warn!(%transfer_id, "failed to record receive startup failure");
            }
            self.cleanup_attempt(transfer_id, false).await;
            return Ok(transfer_id);
        }
        let handle_result = tokio::select! {
            result = self.inner.backend.receive(backend_request) => result,
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
        let retry_request = match retry_request {
            RetryRequest::Send {
                request,
                mut manifest,
            } => {
                if let Some(manifest) = &mut manifest {
                    manifest.transfer_id = transfer_id;
                }
                RetryRequest::Send { request, manifest }
            }
            RetryRequest::Receive { request, staging } => {
                RetryRequest::Receive { request, staging }
            }
        };
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
        self.persist_recovery(transfer_id).await;
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
        mut handle: Box<dyn TransferHandle>,
        cancellation: watch::Receiver<bool>,
    ) {
        let mut updates = handle.take_updates();
        let manager = Self {
            inner: Arc::clone(&self.inner),
        };
        tokio::spawn(async move {
            let cancellation_wait: BackendCancellation =
                Box::pin(wait_for_cancellation(cancellation.clone()));
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
                    .map_err(|_| BackendError::Protocol {
                        reason: BackendProtocolError::MalformedMessage,
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
                        return Err(BackendError::Protocol {
                            reason: BackendProtocolError::MalformedMessage,
                        });
                    }
                    Err(error @ TransferError::ProgressNotAllowed(_)) => {
                        warn!(%transfer_id, %error, "ignored invalid or late progress event");
                    }
                    Err(_) => {
                        return Err(BackendError::Protocol {
                            reason: BackendProtocolError::MalformedMessage,
                        });
                    }
                }
            }
            BackendEvent::CapabilityUnavailable { capability } => {
                let capability = match capability {
                    BackendCapability::Progress => TransferCapability::Progress,
                    BackendCapability::Pause => TransferCapability::Pause,
                    BackendCapability::Resume => TransferCapability::Resume,
                    BackendCapability::Direct | BackendCapability::Relay => return Ok(()),
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
    async fn finish(&self, transfer_id: TransferId, result: Result<(), BackendError>) {
        self.finish_attempt(transfer_id, result, false).await;
    }

    async fn finish_attempt(
        &self,
        transfer_id: TransferId,
        result: Result<(), BackendError>,
        cancelled: bool,
    ) {
        if cancelled || matches!(&result, Err(BackendError::Cancelled)) {
            self.cancel_untracked(transfer_id).await.ok();
            self.cleanup_attempt(transfer_id, false).await;
            return;
        }
        let receive_staging = self.receive_staging(transfer_id).await;
        let receive_destination = receive_staging
            .as_ref()
            .map(|staging| staging.destination().to_path_buf());
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
                            BackendError::Protocol {
                                reason: BackendProtocolError::MalformedMessage,
                            },
                        )
                        .await;
                    self.cleanup_attempt(transfer_id, false).await;
                    return;
                }
                if let Some(staging) = receive_staging {
                    match staging.finalize().await {
                        Ok(report) if report.cleanup_failed() => {
                            warn!(%transfer_id, "receive staging cleanup failed after publish");
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(
                                %transfer_id,
                                error = %safe_receive_staging_error(&error),
                                "receive finalization failed"
                            );
                            let _ = self
                                .fail_with(
                                    transfer_id,
                                    safe_receive_staging_error(&error),
                                    receive_staging_failure_kind(&error),
                                )
                                .await;
                            self.cleanup_attempt(transfer_id, false).await;
                            return;
                        }
                    }
                }
                if let Some(destination) = receive_destination {
                    self.inner
                        .completed_receive_destinations
                        .write()
                        .await
                        .insert(transfer_id, destination);
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
                    self.inner
                        .completed_receive_destinations
                        .write()
                        .await
                        .remove(&transfer_id);
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
                if retryable {
                    self.persist_recovery(transfer_id).await;
                }
                self.cleanup_attempt(transfer_id, retryable).await;
            }
        }
    }

    async fn receive_staging(&self, transfer_id: TransferId) -> Option<ReceiveStaging> {
        self.inner
            .retry_requests
            .lock()
            .await
            .get(&transfer_id)
            .and_then(|request| match request {
                RetryRequest::Receive { staging, .. } => Some(staging.clone()),
                RetryRequest::Send { .. } => None,
            })
    }

    async fn cleanup_attempt(&self, transfer_id: TransferId, keep_retry_request: bool) {
        if !keep_retry_request {
            let retry_request = self.inner.retry_requests.lock().await.remove(&transfer_id);
            if let Some(RetryRequest::Receive { staging, .. }) = retry_request {
                if let Err(error) = staging.cleanup().await {
                    warn!(
                        %transfer_id,
                        error = %safe_receive_staging_error(&error),
                        "failed to clean receive staging"
                    );
                }
            }
            self.remove_recovery_metadata(transfer_id).await;
        }
        let completion = self
            .inner
            .active
            .lock()
            .await
            .remove(&transfer_id)
            .map(|attempt| attempt.completion);
        if let Some(completion) = completion {
            let _ = completion.send(true);
        }
    }

    async fn require_capability(
        &self,
        transfer_id: TransferId,
        capability: TransferCapability,
    ) -> Result<(), TransferError> {
        if self
            .inner
            .backend
            .capabilities()
            .supports(match capability {
                TransferCapability::Progress => BackendCapability::Progress,
                TransferCapability::Pause => BackendCapability::Pause,
                TransferCapability::Resume => BackendCapability::Resume,
            })
        {
            let _ = self
                .session(transfer_id)
                .await
                .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
            Err(TransferError::Backend(
                "backend control handshake is not implemented".into(),
            ))
        } else {
            Err(TransferError::CapabilityUnavailable(capability))
        }
    }

    async fn persist_recovery(&self, transfer_id: TransferId) {
        let Some(store) = &self.inner.resume_store else {
            return;
        };
        let Some(session) = self.session(transfer_id).await else {
            return;
        };
        let request = self
            .inner
            .retry_requests
            .lock()
            .await
            .get(&transfer_id)
            .cloned();
        let Some(request) = request else {
            return;
        };
        let (request, manifest, file_id, file_size, file_digest, temp_file_path) = match request {
            RetryRequest::Send { request, manifest } => {
                let (file_id, file_size, file_digest) = manifest
                    .as_ref()
                    .and_then(|manifest| manifest.files.first())
                    .map_or((Uuid::nil(), 0, None), |file| {
                        (file.file_id, file.size, file.digest.clone())
                    });
                (
                    ResumeRequest::Send {
                        source_paths: request.paths,
                    },
                    manifest,
                    file_id,
                    file_size,
                    file_digest,
                    None,
                )
            }
            RetryRequest::Receive { request, staging } => (
                ResumeRequest::Receive {
                    output_directory: request.output_directory,
                },
                None,
                Uuid::nil(),
                0,
                None,
                Some(staging.relative_path().to_path_buf()),
            ),
        };
        let state = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            transfer_id,
            backend: session.backend,
            backend_version: self.inner.backend.version().map(str::to_owned),
            capabilities: ResumeCapabilities {
                pause: self
                    .inner
                    .backend
                    .capabilities()
                    .supports(BackendCapability::Pause),
                resume: self
                    .inner
                    .backend
                    .capabilities()
                    .supports(BackendCapability::Resume),
            },
            request,
            manifest,
            file_id,
            chunk_size: DEFAULT_RESUME_CHUNK_SIZE,
            file_size,
            completed_chunks: Vec::new(),
            file_digest,
            temp_file_path,
        };
        if let Err(error) = store.save_resume(&state).await {
            warn!(%transfer_id, error = %safe_storage_error(&error), "failed to persist resume metadata");
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
        self.fail_with(transfer_id, error.safe_message(), error.failure_kind())
            .await
    }

    async fn fail_with(
        &self,
        transfer_id: TransferId,
        message: impl Into<String>,
        failure_kind: TransferFailureKind,
    ) -> Result<TransferId, TransferError> {
        let mut sessions = self.inner.sessions.write().await;
        let session = sessions
            .get_mut(&transfer_id)
            .ok_or_else(|| TransferError::Backend("transfer session not found".into()))?;
        session.transition(TransferState::Failed)?;
        session.error = Some(message.into());
        session.failure_kind = Some(failure_kind);
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

fn map_storage_error(error: StorageError) -> TransferError {
    TransferError::Backend(safe_storage_error(&error).into())
}

fn map_receive_staging_creation_error(error: ReceiveStagingError) -> TransferError {
    let message = match &error {
        ReceiveStagingError::Io(_) => "receive staging is unavailable",
        _ => safe_receive_staging_error(&error),
    };
    TransferError::Filesystem(message.into())
}

fn receive_staging_failure_kind(error: &ReceiveStagingError) -> TransferFailureKind {
    match error {
        ReceiveStagingError::Destination(DestinationError::SymlinkNotAllowed)
        | ReceiveStagingError::InvalidOutputPath
        | ReceiveStagingError::SymlinkOutput => TransferFailureKind::Security,
        ReceiveStagingError::EmptyOutput => TransferFailureKind::Integrity,
        ReceiveStagingError::Destination(_)
        | ReceiveStagingError::DestinationChanged
        | ReceiveStagingError::Conflict
        | ReceiveStagingError::Io(_)
        | ReceiveStagingError::Rollback(_) => TransferFailureKind::Filesystem,
    }
}

fn safe_receive_staging_error(error: &ReceiveStagingError) -> &'static str {
    match error {
        ReceiveStagingError::Destination(_) => "receive destination is unavailable",
        ReceiveStagingError::DestinationChanged => "receive destination changed during transfer",
        ReceiveStagingError::EmptyOutput => "received output was empty",
        ReceiveStagingError::InvalidOutputPath => "received output path was unsafe",
        ReceiveStagingError::SymlinkOutput => "received output contained a symbolic link",
        ReceiveStagingError::Conflict => "received output already exists",
        ReceiveStagingError::Io(_) | ReceiveStagingError::Rollback(_) => {
            "receive finalization failed"
        }
    }
}

fn safe_storage_error(error: &StorageError) -> &'static str {
    match error {
        StorageError::Io(_) => "resume storage unavailable",
        StorageError::Serialization(_) => "resume metadata is unreadable",
        StorageError::InvalidResume(_) => "resume metadata is invalid",
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
            manager
                .session(transfer_id)
                .await
                .unwrap()
                .progress
                .transferred_bytes,
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
            manager
                .session(transfer_id)
                .await
                .unwrap()
                .progress
                .transferred_bytes,
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
            BackendError::Protocol {
                reason: BackendProtocolError::MalformedMessage,
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
        assert_eq!(session.error.as_deref(), Some("backend operation failed"));
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

    #[tokio::test]
    async fn croc_pause_and_resume_report_capability_unavailable() {
        let manager = TransferManager::new(CrocBackend::default());
        let transfer_id = manager.create_session(Role::Sender).await;

        assert_eq!(
            manager.pause(transfer_id).await,
            Err(TransferError::CapabilityUnavailable(
                TransferCapability::Pause
            ))
        );
        assert_eq!(
            manager.resume(transfer_id).await,
            Err(TransferError::CapabilityUnavailable(
                TransferCapability::Resume
            ))
        );
    }

    #[tokio::test]
    async fn receive_rejects_file_destination_as_filesystem_error() {
        let root = std::env::temp_dir().join(format!(
            "drift-transfer-invalid-receive-destination-{}",
            TransferId::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("not-a-directory");
        std::fs::write(&file, b"existing output").unwrap();

        let error = TransferManager::new(CrocBackend::default())
            .start_receive(ReceiveRequest::new("receive-code", &file).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            TransferError::Filesystem(message) if message == "receive destination is unavailable"
        ));

        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recovery_uses_explicit_receive_destination() {
        let root = std::env::temp_dir().join(format!(
            "drift-transfer-recovery-destination-{}",
            TransferId::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let arguments_path = root.join("arguments.txt");
        let persisted_directory = root.join("persisted");
        let selected_directory = root.join("selected");
        let script = versioned_script(&format!(
            "printf '%s\\n' \"$@\" > \"{}\"; exit 0",
            arguments_path.display()
        ));
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let state = ResumeState {
            schema_version: RESUME_SCHEMA_VERSION,
            transfer_id: TransferId::new(),
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: ResumeRequest::Receive {
                output_directory: persisted_directory.clone(),
            },
            manifest: None,
            file_id: Uuid::nil(),
            chunk_size: DEFAULT_RESUME_CHUNK_SIZE,
            file_size: 0,
            completed_chunks: Vec::new(),
            file_digest: None,
            temp_file_path: None,
        };

        let new_transfer_id = manager
            .recover(
                state,
                Some("secret-transfer-code".into()),
                Some(selected_directory.clone()),
            )
            .await
            .unwrap();
        loop {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if notification.transfer_id == new_transfer_id
                && matches!(
                    notification.event,
                    TransferEvent::Completed | TransferEvent::Failed
                )
            {
                break;
            }
        }

        let arguments = std::fs::read_to_string(&arguments_path).unwrap();
        let selected_text = selected_directory.to_string_lossy().into_owned();
        let persisted_text = persisted_directory.to_string_lossy().into_owned();
        let arguments = arguments.lines().collect::<Vec<_>>();
        let output_index = arguments
            .iter()
            .position(|argument| *argument == "--out")
            .unwrap();
        let backend_output = arguments[output_index + 1];
        assert!(backend_output.starts_with(&format!("{selected_text}/.drift-staging-")));
        assert!(!arguments.iter().any(|argument| *argument == persisted_text));

        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn successful_receive_publishes_only_after_staging_finalization() {
        let root = std::env::temp_dir().join(format!(
            "drift-transfer-receive-finalize-{}",
            TransferId::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let script = versioned_script(
            r#"output=""
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--out" ]; then output="$2"; fi
    shift
done
mkdir -p "$output"
printf 'received output' > "$output/file.txt"
exit 0"#,
        );
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_receive(ReceiveRequest::new("receive-code", &root).unwrap())
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

        assert_eq!(
            std::fs::read(root.join("file.txt")).unwrap(),
            b"received output"
        );
        assert!(!std::fs::read_dir(&root).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".drift-staging-")));
        assert_eq!(
            manager.session(transfer_id).await.unwrap().state,
            TransferState::Completed
        );
        assert_eq!(
            manager.completed_receive_destination(transfer_id).await,
            Some(root.clone())
        );

        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_receive_does_not_publish_partial_final_file() {
        let root = std::env::temp_dir().join(format!(
            "drift-transfer-receive-failure-{}",
            TransferId::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let script = versioned_script(
            r#"output=""
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--out" ]; then output="$2"; fi
    shift
done
mkdir -p "$output"
printf 'partial output' > "$output/file.txt"
exit 7"#,
        );
        let manager = TransferManager::new(CrocBackend::new(&script));
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_receive(ReceiveRequest::new("receive-code", &root).unwrap())
            .await
            .unwrap();

        loop {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if notification.transfer_id == transfer_id
                && notification.event == TransferEvent::Failed
            {
                break;
            }
        }
        while manager.inner.active.lock().await.contains_key(&transfer_id) {
            tokio::task::yield_now().await;
        }

        assert!(!root.join("file.txt").exists());
        assert!(!std::fs::read_dir(&root).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".drift-staging-")));
        assert_eq!(
            manager.session(transfer_id).await.unwrap().failure_kind,
            Some(TransferFailureKind::ProcessFailure)
        );

        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retryable_receive_failure_keeps_private_partial_and_resume_metadata() {
        let root = std::env::temp_dir().join(format!(
            "drift-transfer-receive-recovery-{}",
            TransferId::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let state_root = root.join("state");
        let script = versioned_script(
            r#"output=""
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--out" ]; then output="$2"; fi
    shift
done
mkdir -p "$output"
    printf 'partial output' > "$output/file.txt"
    sleep 1"#,
        );
        let manager = TransferManager::with_resume_store(
            CrocBackend::new(&script).with_timeout(std::time::Duration::from_millis(50)),
            "croc",
            JsonStore::new(&state_root),
        );
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_receive(ReceiveRequest::new("receive-code", &root).unwrap())
            .await
            .unwrap();

        loop {
            let notification =
                tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
            if notification.transfer_id == transfer_id
                && notification.event == TransferEvent::Failed
            {
                break;
            }
        }

        let resume = JsonStore::new(&state_root)
            .load_resume(transfer_id)
            .await
            .unwrap()
            .unwrap();
        let staging_name = resume.temp_file_path.unwrap();
        assert!(!root.join("file.txt").exists());
        assert!(root.join(&staging_name).join("file.txt").exists());
        assert_eq!(
            manager.session(transfer_id).await.unwrap().failure_kind,
            Some(TransferFailureKind::Network)
        );

        manager.discard_recovery(transfer_id).await.unwrap();
        assert!(!root.join(&staging_name).exists());
        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persists_secret_free_recovery_and_restarts_only_after_explicit_action() {
        let script = versioned_script("sleep 1");
        let root =
            std::env::temp_dir().join(format!("drift-transfer-recovery-{}", TransferId::new()));
        let store = JsonStore::new(&root);
        let manager = TransferManager::with_resume_store(
            CrocBackend::new(&script).with_timeout(std::time::Duration::from_millis(50)),
            "croc",
            store.clone(),
        );
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_receive(ReceiveRequest::new("secret-transfer-code", &root).unwrap())
            .await
            .unwrap();
        assert!(store.load_resume(transfer_id).await.unwrap().is_some());
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
        let state = loop {
            if let Some(state) = store.load_resume(transfer_id).await.unwrap() {
                break state;
            }
            tokio::task::yield_now().await;
        };
        let serialized = tokio::fs::read(root.join(format!("{transfer_id}.resume.json")))
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&serialized).contains("secret-transfer-code"));
        assert!(!state.capabilities.resume);
        assert!(matches!(state.request, ResumeRequest::Receive { .. }));

        let new_transfer_id = manager
            .recover(state, Some("secret-transfer-code".into()), None)
            .await
            .unwrap();
        assert_ne!(new_transfer_id, transfer_id);
        manager.cancel(new_transfer_id).await.unwrap();
        assert_eq!(store.load_resume(new_transfer_id).await.unwrap(), None);
        manager.discard_recovery(transfer_id).await.unwrap();
        assert_eq!(store.load_resume(transfer_id).await.unwrap(), None);
        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sender_recovery_persists_manifest_for_new_attempt_id() {
        let script = versioned_script("sleep 1");
        let root = std::env::temp_dir().join(format!(
            "drift-transfer-sender-recovery-{}",
            TransferId::new()
        ));
        let store = JsonStore::new(&root);
        let manager = TransferManager::with_resume_store(
            CrocBackend::new(&script).with_timeout(std::time::Duration::from_millis(50)),
            "croc",
            store.clone(),
        );
        let manifest = TransferManifest::new(
            TransferId::new(),
            vec![FileEntry::new("source.bin", 1).unwrap()],
        )
        .unwrap();
        let transfer_id = manager
            .start_send_with_manifest(
                SendRequest::new(vec![PathBuf::from("source.bin")]).unwrap(),
                Some(manifest),
            )
            .await
            .unwrap();
        let state = store.load_resume(transfer_id).await.unwrap().unwrap();
        assert_eq!(state.manifest.as_ref().unwrap().transfer_id, transfer_id);
        manager.cancel(transfer_id).await.unwrap();
        assert_eq!(store.load_resume(transfer_id).await.unwrap(), None);
        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminal_process_failure_removes_recovery_metadata() {
        let script = versioned_script("exit 7");
        let root =
            std::env::temp_dir().join(format!("drift-transfer-terminal-{}", TransferId::new()));
        let store = JsonStore::new(&root);
        let manager =
            TransferManager::with_resume_store(CrocBackend::new(&script), "croc", store.clone());
        let mut events = manager.subscribe();
        let transfer_id = manager
            .start_send(SendRequest::new(vec![PathBuf::from("source.bin")]).unwrap())
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
        assert_eq!(store.load_resume(transfer_id).await.unwrap(), None);
        let _ = std::fs::remove_file(script);
        let _ = std::fs::remove_dir_all(root);
    }
}
