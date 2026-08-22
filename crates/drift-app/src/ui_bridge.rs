use crate::event_bridge::{AppTransferUpdate, TransferPresentation};
use crate::settings::{RelaySettings, SettingsError, SettingsValidationError};
use crate::{AppCommand, AppCommandError, AppHandle};
use drift_core::{
    ResumeRequest, Role, TransferCapability, TransferEvent, TransferId, TransferManifest,
};
use drift_protocol::BackendCapability;
use drift_storage::{scan_send_paths, ScanCancellation, SourceScan, SourceScanError};
use drift_ui::{
    failure_label, ReceiveCommandError, ReceiveController, ReceiveEvent, ReceiveEventFuture,
    ReceiveEventStream, ReceiveFuture, RecoveryCandidate, RecoveryKind, RelaySettingsSnapshot,
    SelectedItem, SendCommandError, SendCommandErrorKind, SendController, SendEvent,
    SendEventFuture, SendEventStream, SendFuture, SendProgress, SendSelection,
    SettingsCommandError, SettingsCommandErrorKind, SettingsController, SettingsFuture,
    RelayStatus, TransferCommandError, TransferCommandErrorKind, TransferCommandFuture,
    TransferController, TransferEventFuture, TransferEventStream, TransferListFuture,
    TransferSnapshot,
};
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct AppSendController {
    handle: AppHandle,
    scan_cancellation: Arc<Mutex<Option<ScanCancellation>>>,
}

impl AppSendController {
    pub fn new(handle: AppHandle) -> Self {
        Self {
            handle,
            scan_cancellation: Arc::new(Mutex::new(None)),
        }
    }
}

impl SendController for AppSendController {
    fn recoveries(&self) -> SendFuture<Result<Vec<RecoveryCandidate>, SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.recoveries().await {
                Ok(Ok(discovery)) => Ok(recovery_candidates(&discovery)),
                Ok(Err(_)) | Err(_) => Err(SendCommandError::start_failed()),
            }
        })
    }

    fn scan(&self, paths: Vec<PathBuf>) -> SendFuture<Result<SendSelection, SendCommandError>> {
        let cancellation = ScanCancellation::new();
        if let Ok(mut current) = self.scan_cancellation.lock() {
            if let Some(previous) = current.replace(cancellation.clone()) {
                previous.cancel();
            }
        }
        Box::pin(async move {
            scan_send_paths(paths, cancellation)
                .await
                .map_err(map_source_scan_error)
                .and_then(source_scan_to_selection)
        })
    }

    fn cancel_scan(&self) {
        if let Ok(current) = self.scan_cancellation.lock() {
            if let Some(cancellation) = current.as_ref() {
                cancellation.cancel();
            }
        }
    }

    fn preflight(&self, _paths: Vec<PathBuf>) -> SendFuture<Result<(), SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.preflight().await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) | Err(_) => {
                    Err(SendCommandError::new(SendCommandErrorKind::PreflightFailed))
                }
            }
        })
    }

    fn start_send(
        &self,
        paths: Vec<PathBuf>,
        manifest: Option<TransferManifest>,
    ) -> SendFuture<Result<TransferId, SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            let Some(manifest) = manifest else {
                return Err(SendCommandError::new(SendCommandErrorKind::StartFailed));
            };
            match handle.dispatch(AppCommand::Send { paths, manifest }).await {
                Ok(Ok(transfer_id)) => Ok(transfer_id),
                Ok(Err(_)) | Err(_) => {
                    Err(SendCommandError::new(SendCommandErrorKind::StartFailed))
                }
            }
        })
    }

    fn cancel(&self, transfer_id: TransferId) -> SendFuture<Result<(), SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.dispatch(AppCommand::Cancel { transfer_id }).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(_)) | Err(_) => {
                    Err(SendCommandError::new(SendCommandErrorKind::CancelFailed))
                }
            }
        })
    }

    fn retry(&self, transfer_id: TransferId) -> SendFuture<Result<TransferId, SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::RetryTransfer { transfer_id })
                .await
            {
                Ok(Ok(transfer_id)) => Ok(transfer_id),
                Ok(Err(_)) | Err(_) => Err(SendCommandError::start_failed()),
            }
        })
    }

    fn recover(&self, transfer_id: TransferId) -> SendFuture<Result<TransferId, SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::RecoverTransfer {
                    transfer_id,
                    code: None,
                    output_directory: None,
                })
                .await
            {
                Ok(Ok(transfer_id)) => Ok(transfer_id),
                Ok(Err(_)) | Err(_) => Err(SendCommandError::start_failed()),
            }
        })
    }

    fn discard_recovery(
        &self,
        transfer_id: TransferId,
    ) -> SendFuture<Result<(), SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::DiscardRecovery { transfer_id })
                .await
            {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(SendCommandError::start_failed()),
            }
        })
    }

    fn subscribe(&self) -> Box<dyn SendEventStream> {
        Box::new(AppSendEventStream {
            handle: self.handle.clone(),
            receiver: self.handle.subscribe(),
        })
    }
}

fn source_scan_to_selection(scan: SourceScan) -> Result<SendSelection, SendCommandError> {
    let items = scan
        .roots()
        .iter()
        .map(|root| SelectedItem::new(root.path().to_path_buf(), root.total_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| SendCommandError::scan_failed())?;
    SendSelection::with_manifest(items, scan.manifest().clone())
        .map_err(|_| SendCommandError::scan_failed())
}

fn map_source_scan_error(error: SourceScanError) -> SendCommandError {
    let kind = match error {
        SourceScanError::Unavailable => SendCommandErrorKind::SourceUnavailable,
        SourceScanError::Unreadable => SendCommandErrorKind::SourceUnreadable,
        SourceScanError::SymlinkNotAllowed => SendCommandErrorKind::SymlinkNotAllowed,
        SourceScanError::UnsupportedFileType => SendCommandErrorKind::UnsupportedFileType,
        SourceScanError::EmptyDirectory => SendCommandErrorKind::EmptyDirectory,
        SourceScanError::TooManyEntries => SendCommandErrorKind::TooManyEntries,
        SourceScanError::DuplicatePath => SendCommandErrorKind::DuplicatePath,
        SourceScanError::InvalidRelativePath => SendCommandErrorKind::InvalidRelativePath,
        SourceScanError::Cancelled => SendCommandErrorKind::ScanCancelled,
        SourceScanError::EmptySelection
        | SourceScanError::InvalidRoot
        | SourceScanError::SizeOverflow => SendCommandErrorKind::ScanFailed,
    };
    SendCommandError::new(kind)
}

#[derive(Clone)]
pub struct AppReceiveController {
    handle: AppHandle,
}

#[derive(Clone)]
pub struct AppTransferController {
    handle: AppHandle,
}

impl AppTransferController {
    pub fn new(handle: AppHandle) -> Self {
        Self { handle }
    }
}

impl TransferController for AppTransferController {
    fn load(&self) -> TransferListFuture {
        let handle = self.handle.clone();
        Box::pin(async move {
            let sessions = handle.sessions().await;
            let mut snapshots = Vec::with_capacity(sessions.len());
            for session in sessions {
                let presentation = handle.presentation(session.id).await.unwrap_or_else(|| {
                    TransferPresentation {
                        transfer_id: session.id,
                        role: session.role,
                        state: session.state,
                        progress: session.progress,
                        code_available: session.code.is_some(),
                        error: session.error.clone(),
                        failure_kind: session.failure_kind,
                    }
                });
                snapshots.push(transfer_snapshot(&handle, presentation, Some(session)).await);
            }
            Ok(snapshots)
        })
    }

    fn cancel(&self, transfer_id: TransferId) -> TransferCommandFuture {
        app_transfer_command(&self.handle, AppCommand::Cancel { transfer_id })
    }

    fn retry(&self, transfer_id: TransferId) -> TransferCommandFuture {
        app_transfer_command(&self.handle, AppCommand::RetryTransfer { transfer_id })
    }

    fn pause(&self, transfer_id: TransferId) -> TransferCommandFuture {
        app_transfer_command(&self.handle, AppCommand::PauseTransfer { transfer_id })
    }

    fn resume(&self, transfer_id: TransferId) -> TransferCommandFuture {
        app_transfer_command(&self.handle, AppCommand::ResumeTransfer { transfer_id })
    }

    fn reveal_destination(&self, transfer_id: TransferId) -> TransferCommandFuture {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.reveal_destination(transfer_id).await {
                Ok(Ok(transfer_id)) => Ok(transfer_id),
                Ok(Err(AppCommandError::DestinationUnavailable)) => Err(
                    TransferCommandError::new(TransferCommandErrorKind::DestinationUnavailable),
                ),
                Ok(Err(_)) => Err(TransferCommandError::new(TransferCommandErrorKind::Failed)),
                Err(_) => Err(TransferCommandError::new(
                    TransferCommandErrorKind::Unavailable,
                )),
            }
        })
    }

    fn subscribe(&self) -> Box<dyn TransferEventStream> {
        Box::new(AppTransferEventStream {
            handle: self.handle.clone(),
            receiver: self.handle.subscribe(),
        })
    }
}

fn app_transfer_command(handle: &AppHandle, command: AppCommand) -> TransferCommandFuture {
    let handle = handle.clone();
    Box::pin(async move {
        match handle.dispatch(command).await {
            Ok(Ok(transfer_id)) => Ok(transfer_id),
            Ok(Err(error)) => Err(map_transfer_command_error(&error)),
            Err(_) => Err(TransferCommandError::new(
                TransferCommandErrorKind::Unavailable,
            )),
        }
    })
}

fn map_transfer_command_error(error: &AppCommandError) -> TransferCommandError {
    let kind = match error {
        AppCommandError::Transfer(drift_core::TransferError::CapabilityUnavailable(_)) => {
            TransferCommandErrorKind::Unsupported
        }
        AppCommandError::RecoveryUnavailable | AppCommandError::RecoveryInvalid => {
            TransferCommandErrorKind::Failed
        }
        _ => TransferCommandErrorKind::Failed,
    };
    TransferCommandError::new(kind)
}

struct AppTransferEventStream {
    handle: AppHandle,
    receiver: broadcast::Receiver<AppTransferUpdate>,
}

impl TransferEventStream for AppTransferEventStream {
    fn next(&mut self) -> TransferEventFuture<'_> {
        Box::pin(async move {
            loop {
                let update = match self.receiver.recv().await {
                    Ok(update) => update,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "transfer UI event stream lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                };
                return Some(transfer_snapshot(&self.handle, update.presentation, None).await);
            }
        })
    }
}

async fn transfer_snapshot(
    handle: &AppHandle,
    presentation: TransferPresentation,
    session: Option<drift_core::TransferSession>,
) -> TransferSnapshot {
    let session = match session {
        Some(session) => Some(session),
        None => handle.session(presentation.transfer_id).await,
    };
    let manifest = session.as_ref().and_then(|session| session.manifest.as_ref());
    let display_name = manifest
        .and_then(|manifest| manifest.files.first())
        .and_then(|file| file.relative_path.file_name())
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned);
    let file_count = manifest.map(|manifest| manifest.files.len());
    let capabilities = handle.backend_capabilities();
    let relay = match handle.relay_configured() {
        Some(true) => RelayStatus::Custom,
        Some(false) => RelayStatus::Default,
        None => RelayStatus::Unknown,
    };
    let retryable = presentation.retryable();
    let error = safe_failure_message(
        presentation.state,
        presentation.failure_kind,
        retryable,
        "The transfer failed.",
    );
    TransferSnapshot {
        transfer_id: presentation.transfer_id,
        role: presentation.role,
        state: presentation.state,
        progress: presentation.progress,
        progress_supported: capabilities.supports(BackendCapability::Progress),
        pause_supported: capabilities.supports(BackendCapability::Pause),
        resume_supported: capabilities.supports(BackendCapability::Resume),
        retryable,
        display_name,
        file_count,
        relay,
        error,
        destination_available: presentation.state == drift_core::TransferState::Completed
            && presentation.role == drift_core::Role::Receiver
            && handle
                .destination_available(presentation.transfer_id)
                .await,
    }
}

fn safe_failure_message(
    state: drift_core::TransferState,
    failure_kind: Option<drift_core::TransferFailureKind>,
    retryable: bool,
    fallback: &'static str,
) -> Option<String> {
    if state != drift_core::TransferState::Failed {
        return None;
    }
    Some(
        failure_label(failure_kind)
            .unwrap_or(if retryable {
                "The transfer stopped unexpectedly. Retry is available."
            } else {
                fallback
            })
            .to_owned(),
    )
}

#[derive(Clone)]
pub struct AppSettingsController {
    handle: AppHandle,
}

impl AppSettingsController {
    pub fn new(handle: AppHandle) -> Self {
        Self { handle }
    }
}

impl SettingsController for AppSettingsController {
    fn load(&self) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.settings().await {
                Ok(Ok(settings)) => Ok(relay_snapshot(&settings.relay)),
                Ok(Err(_)) | Err(_) => Err(settings_error(SettingsCommandErrorKind::LoadFailed)),
            }
        })
    }

    fn save(
        &self,
        enabled: bool,
        endpoint: Option<String>,
    ) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            let relay = RelaySettings {
                enabled,
                url: endpoint,
            };
            match handle.update_relay(relay).await {
                Ok(Ok(settings)) => Ok(relay_snapshot(&settings.relay)),
                Ok(Err(error)) => Err(map_settings_error(&error)),
                Err(_) => Err(settings_error(SettingsCommandErrorKind::SaveFailed)),
            }
        })
    }

    fn clear(&self) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.clear_relay().await {
                Ok(Ok(settings)) => Ok(relay_snapshot(&settings.relay)),
                Ok(Err(error)) => Err(map_settings_error(&error)),
                Err(_) => Err(settings_error(SettingsCommandErrorKind::SaveFailed)),
            }
        })
    }
}

fn relay_snapshot(relay: &RelaySettings) -> RelaySettingsSnapshot {
    RelaySettingsSnapshot::new(relay.enabled, relay.url.clone())
}

fn settings_error(kind: SettingsCommandErrorKind) -> SettingsCommandError {
    SettingsCommandError::new(kind)
}

fn map_settings_error(error: &AppCommandError) -> SettingsCommandError {
    match error {
        AppCommandError::Settings(SettingsError::Validation(
            SettingsValidationError::MissingRelayUrl | SettingsValidationError::InvalidRelayUrl,
        )) => settings_error(SettingsCommandErrorKind::InvalidRelay),
        _ => settings_error(SettingsCommandErrorKind::SaveFailed),
    }
}

impl AppReceiveController {
    pub fn new(handle: AppHandle) -> Self {
        Self { handle }
    }
}

impl ReceiveController for AppReceiveController {
    fn recoveries(&self) -> ReceiveFuture<Result<Vec<RecoveryCandidate>, ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.recoveries().await {
                Ok(Ok(discovery)) => Ok(recovery_candidates(&discovery)),
                Ok(Err(_)) | Err(_) => Err(ReceiveCommandError::start_failed()),
            }
        })
    }

    fn default_destination(&self) -> Option<PathBuf> {
        Some(self.handle.default_receive_directory())
    }

    fn validate_destination(
        &self,
        path: PathBuf,
    ) -> ReceiveFuture<Result<(), ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.validate_destination(path).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(map_destination_error(&error)),
                Err(_) => Err(ReceiveCommandError::destination_unavailable()),
            }
        })
    }

    fn preflight(&self) -> ReceiveFuture<Result<(), ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.preflight().await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(ReceiveCommandError::preflight_failed()),
            }
        })
    }

    fn start_receive(
        &self,
        code: String,
        destination: PathBuf,
    ) -> ReceiveFuture<Result<TransferId, ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::Receive {
                    code,
                    output_directory: Some(destination),
                })
                .await
            {
                Ok(Ok(transfer_id)) => Ok(transfer_id),
                Ok(Err(_)) | Err(_) => Err(ReceiveCommandError::start_failed()),
            }
        })
    }

    fn cancel(&self, transfer_id: TransferId) -> ReceiveFuture<Result<(), ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.dispatch(AppCommand::Cancel { transfer_id }).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(ReceiveCommandError::cancel_failed()),
            }
        })
    }

    fn retry(
        &self,
        transfer_id: TransferId,
    ) -> ReceiveFuture<Result<TransferId, ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::RetryTransfer { transfer_id })
                .await
            {
                Ok(Ok(transfer_id)) => Ok(transfer_id),
                Ok(Err(_)) | Err(_) => Err(ReceiveCommandError::start_failed()),
            }
        })
    }

    fn recover(
        &self,
        transfer_id: TransferId,
        code: String,
        destination: PathBuf,
    ) -> ReceiveFuture<Result<TransferId, ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::RecoverTransfer {
                    transfer_id,
                    code: Some(code),
                    output_directory: Some(destination),
                })
                .await
            {
                Ok(Ok(transfer_id)) => Ok(transfer_id),
                Ok(Err(_)) | Err(_) => Err(ReceiveCommandError::start_failed()),
            }
        })
    }

    fn discard_recovery(
        &self,
        transfer_id: TransferId,
    ) -> ReceiveFuture<Result<(), ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::DiscardRecovery { transfer_id })
                .await
            {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(ReceiveCommandError::start_failed()),
            }
        })
    }

    fn subscribe(&self) -> Box<dyn ReceiveEventStream> {
        Box::new(AppReceiveEventStream {
            handle: self.handle.clone(),
            receiver: self.handle.subscribe(),
        })
    }
}

fn map_destination_error(error: &AppCommandError) -> ReceiveCommandError {
    match error {
        AppCommandError::OutputDirectoryNotWritable => {
            ReceiveCommandError::destination_not_writable()
        }
        AppCommandError::EmptyOutputDirectory | AppCommandError::OutputDirectoryUnavailable => {
            ReceiveCommandError::destination_unavailable()
        }
        _ => ReceiveCommandError::destination_unavailable(),
    }
}

fn recovery_candidates(discovery: &drift_storage::ResumeDiscovery) -> Vec<RecoveryCandidate> {
    discovery
        .recoverable()
        .iter()
        .map(|state| RecoveryCandidate {
            transfer_id: state.transfer_id,
            kind: match state.request {
                ResumeRequest::Send { .. } => RecoveryKind::Send,
                ResumeRequest::Receive { .. } => RecoveryKind::Receive,
            },
        })
        .collect()
}

struct AppSendEventStream {
    handle: AppHandle,
    receiver: broadcast::Receiver<AppTransferUpdate>,
}

struct AppReceiveEventStream {
    handle: AppHandle,
    receiver: broadcast::Receiver<AppTransferUpdate>,
}

impl SendEventStream for AppSendEventStream {
    fn next(&mut self) -> SendEventFuture<'_> {
        Box::pin(async move {
            loop {
                let update = match self.receiver.recv().await {
                    Ok(update) => update,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "send UI event stream lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                };
                if let Some(event) = map_notification(&self.handle, update).await {
                    return Some(event);
                }
            }
        })
    }
}

impl ReceiveEventStream for AppReceiveEventStream {
    fn next(&mut self) -> ReceiveEventFuture<'_> {
        Box::pin(async move {
            loop {
                let update = match self.receiver.recv().await {
                    Ok(update) => update,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "receive UI event stream lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                };
                if let Some(event) = map_receive_notification(&self.handle, update).await {
                    return Some(event);
                }
            }
        })
    }
}

fn map_notification(
    handle: &AppHandle,
    update: AppTransferUpdate,
) -> impl Future<Output = Option<SendEvent>> + Send + '_ {
    async move {
        let AppTransferUpdate {
            transfer_id,
            event,
            presentation,
        } = update;
        if presentation.role != Role::Sender {
            return None;
        }
        let retryable = presentation.retryable();
        Some(match event {
            TransferEvent::Created => SendEvent::Created { transfer_id },
            TransferEvent::Connecting => SendEvent::Connecting { transfer_id },
            TransferEvent::Connected => SendEvent::Connected { transfer_id },
            TransferEvent::Authenticating => SendEvent::Authenticating { transfer_id },
            TransferEvent::Negotiating => SendEvent::Negotiating { transfer_id },
            TransferEvent::Started => SendEvent::Started { transfer_id },
            TransferEvent::CodeAvailable => SendEvent::CodeAvailable {
                transfer_id,
                code: handle.session(transfer_id).await?.code?,
            },
            TransferEvent::Progress {
                transferred,
                total,
                speed_bps,
            } => SendEvent::Progress {
                transfer_id,
                progress: SendProgress {
                    transferred,
                    total,
                    speed_bps,
                },
            },
            TransferEvent::CapabilityUnavailable {
                capability: TransferCapability::Progress,
            } => SendEvent::CapabilityUnavailable {
                transfer_id,
                capability: TransferCapability::Progress,
            },
            TransferEvent::CapabilityUnavailable {
                capability: TransferCapability::Pause | TransferCapability::Resume,
            } => return None,
            TransferEvent::Verifying => SendEvent::Verifying { transfer_id },
            TransferEvent::Completed => SendEvent::Completed { transfer_id },
            TransferEvent::Failed => SendEvent::Failed {
                transfer_id,
                message: safe_failure_message(
                    presentation.state,
                    presentation.failure_kind,
                    retryable,
                    "The transfer failed.",
                )
                .unwrap_or_else(|| "The transfer failed.".to_owned()),
                retryable,
            },
            TransferEvent::Cancelled => SendEvent::Cancelled { transfer_id },
            TransferEvent::MetadataReady | TransferEvent::Paused | TransferEvent::Resumed => {
                return None
            }
        })
    }
}

fn map_receive_notification(
    _handle: &AppHandle,
    update: AppTransferUpdate,
) -> impl Future<Output = Option<ReceiveEvent>> + Send + '_ {
    async move {
        let AppTransferUpdate {
            transfer_id,
            event,
            presentation,
        } = update;
        if presentation.role != Role::Receiver {
            return None;
        }
        let retryable = presentation.retryable();
        Some(match event {
            TransferEvent::Created => ReceiveEvent::Created { transfer_id },
            TransferEvent::Connecting => ReceiveEvent::Connecting { transfer_id },
            TransferEvent::Connected => ReceiveEvent::Connected { transfer_id },
            TransferEvent::Authenticating => ReceiveEvent::Authenticating { transfer_id },
            TransferEvent::Negotiating => ReceiveEvent::Negotiating { transfer_id },
            TransferEvent::Started => ReceiveEvent::Started { transfer_id },
            TransferEvent::Progress {
                transferred,
                total,
                speed_bps,
            } => ReceiveEvent::Progress {
                transfer_id,
                transferred,
                total,
                speed_bps,
            },
            TransferEvent::CapabilityUnavailable {
                capability: TransferCapability::Progress,
            } => ReceiveEvent::CapabilityUnavailable {
                transfer_id,
                capability: TransferCapability::Progress,
            },
            TransferEvent::CapabilityUnavailable {
                capability: TransferCapability::Pause | TransferCapability::Resume,
            } => return None,
            TransferEvent::Verifying => ReceiveEvent::Verifying { transfer_id },
            TransferEvent::Completed => ReceiveEvent::Completed { transfer_id },
            TransferEvent::Failed => ReceiveEvent::Failed {
                transfer_id,
                message: safe_failure_message(
                    presentation.state,
                    presentation.failure_kind,
                    retryable,
                    "The receive transfer failed.",
                )
                .unwrap_or_else(|| "The receive transfer failed.".to_owned()),
                retryable,
            },
            TransferEvent::Cancelled => ReceiveEvent::Cancelled { transfer_id },
            TransferEvent::CodeAvailable
            | TransferEvent::MetadataReady
            | TransferEvent::Paused
            | TransferEvent::Resumed => return None,
        })
    }
}

#[allow(dead_code)]
type _SendEventFutureCheck<'a> = Pin<Box<dyn Future<Output = Option<SendEvent>> + Send + 'a>>;

#[cfg(test)]
mod tests {
    use super::*;
    use drift_core::{FileEntry, TransferFailureKind, TransferManifest, TransferSession, TransferState};
    use drift_ui::{ReceiveEvent, SendEvent, SettingsCommandErrorKind, SettingsController};
    use std::fs;
    use uuid::Uuid;

    #[test]
    fn controller_error_messages_are_safe_for_ui() {
        assert_eq!(
            SendCommandError::new(SendCommandErrorKind::PreflightFailed).message(),
            "Croc is not ready."
        );
        assert!(!format!(
            "{:?}",
            SendEvent::CodeAvailable {
                transfer_id: TransferId::new(),
                code: "secret-code".into(),
            }
        )
        .contains("secret-code"));
        assert_eq!(
            ReceiveCommandError::destination_not_writable().message(),
            "The receive folder is not writable."
        );
        assert!(!format!(
            "{:?}",
            ReceiveEvent::Failed {
                transfer_id: TransferId::new(),
                message: "safe receive error".into(),
                retryable: false,
            }
        )
        .contains("secret-code"));
    }

    #[test]
    fn settings_controller_rejects_invalid_endpoint_before_persisting() {
        let root = std::env::temp_dir().join(format!("drift-app-ui-settings-{}", Uuid::new_v4()));
        let config_path = root.join("config").join("config.json");
        let settings = crate::settings::DriftSettings::default();
        crate::settings::SettingsLoader::with_path(&config_path)
            .save(&settings)
            .unwrap();
        let state = crate::AppState::bootstrap_with_config_path(&config_path).unwrap();
        let controller = AppSettingsController::new(state.handle());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let error = runtime
            .block_on(controller.save(true, Some("ftp://relay.example.test".into())))
            .unwrap_err();

        assert_eq!(error.kind(), SettingsCommandErrorKind::InvalidRelay);
        assert_eq!(
            crate::settings::SettingsLoader::with_path(&config_path)
                .load()
                .unwrap()
                .settings
                .relay,
            settings.relay
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn transfer_snapshot_exposes_safe_metadata_and_backend_capabilities() {
        let root = std::env::temp_dir().join(format!("drift-app-ui-transfer-{}", Uuid::new_v4()));
        let config_path = root.join("config").join("config.json");
        crate::settings::SettingsLoader::with_path(&config_path)
            .save(&crate::settings::DriftSettings::default())
            .unwrap();
        let state = crate::AppState::bootstrap_with_config_path(&config_path).unwrap();
        let handle = state.handle();
        let mut session = TransferSession::new(drift_core::Role::Sender, "croc");
        let manifest = TransferManifest::new(
            session.id,
            vec![FileEntry::new("nested/private.txt", 7).unwrap()],
        )
        .unwrap();
        session.set_manifest(manifest);
        session.state = TransferState::Failed;
        let presentation = TransferPresentation {
            transfer_id: session.id,
            role: session.role,
            state: TransferState::Failed,
            progress: session.progress,
            code_available: false,
            error: Some("/private/path/raw backend output".into()),
            failure_kind: Some(TransferFailureKind::ProcessFailure),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let snapshot = runtime.block_on(transfer_snapshot(&handle, presentation, Some(session)));

        assert_eq!(snapshot.display_name.as_deref(), Some("private.txt"));
        assert_eq!(snapshot.file_count, Some(1));
        assert_eq!(
            snapshot.error.as_deref(),
            Some("Croc could not complete the transfer.")
        );
        assert!(!snapshot
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("raw backend output"));
        assert!(!snapshot.progress_supported);
        assert!(!snapshot.pause_supported);
        assert!(!snapshot.resume_supported);
        fs::remove_dir_all(root).unwrap();
    }
}
