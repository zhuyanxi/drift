use crate::event_bridge::AppTransferUpdate;
use crate::{AppCommand, AppCommandError, AppHandle};
use drift_core::{
    ResumeRequest, Role, TransferCapability, TransferEvent, TransferId, TransferManifest,
};
use drift_storage::{scan_send_paths, ScanCancellation, SourceScan, SourceScanError};
use drift_ui::{
    ReceiveCommandError, ReceiveController, ReceiveEvent, ReceiveEventFuture, ReceiveEventStream,
    ReceiveFuture, RecoveryCandidate, RecoveryKind, SelectedItem, SendCommandError,
    SendCommandErrorKind, SendController, SendEvent, SendEventFuture, SendEventStream, SendFuture,
    SendProgress, SendSelection,
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
    ) -> ReceiveFuture<Result<TransferId, ReceiveCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle
                .dispatch(AppCommand::RecoverTransfer {
                    transfer_id,
                    code: Some(code),
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
                message: presentation
                    .error
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
                message: presentation
                    .error
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
    use drift_ui::{ReceiveEvent, SendEvent};

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
}
