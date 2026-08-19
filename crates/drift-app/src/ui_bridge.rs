use drift_core::{Role, TransferCapability, TransferEvent, TransferId};
use drift_transfer::TransferNotification;
use drift_ui::{
    SendCommandError, SendCommandErrorKind, SendController, SendEvent, SendEventFuture,
    SendEventStream, SendFuture, SendProgress,
};
use std::{future::Future, path::PathBuf, pin::Pin};
use tokio::sync::broadcast;

use crate::{AppCommand, AppHandle};

#[derive(Clone)]
pub struct AppSendController {
    handle: AppHandle,
}

impl AppSendController {
    pub fn new(handle: AppHandle) -> Self {
        Self { handle }
    }
}

impl SendController for AppSendController {
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

    fn start_send(&self, paths: Vec<PathBuf>) -> SendFuture<Result<TransferId, SendCommandError>> {
        let handle = self.handle.clone();
        Box::pin(async move {
            match handle.dispatch(AppCommand::Send { paths }).await {
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

    fn subscribe(&self) -> Box<dyn SendEventStream> {
        Box::new(AppSendEventStream {
            handle: self.handle.clone(),
            receiver: self.handle.subscribe(),
        })
    }
}

struct AppSendEventStream {
    handle: AppHandle,
    receiver: broadcast::Receiver<TransferNotification>,
}

impl SendEventStream for AppSendEventStream {
    fn next(&mut self) -> SendEventFuture<'_> {
        Box::pin(async move {
            loop {
                let notification = match self.receiver.recv().await {
                    Ok(notification) => notification,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                };
                if let Some(event) = map_notification(&self.handle, notification).await {
                    return Some(event);
                }
            }
        })
    }
}

fn map_notification(
    handle: &AppHandle,
    notification: TransferNotification,
) -> impl Future<Output = Option<SendEvent>> + Send + '_ {
    async move {
        let transfer_id = notification.transfer_id;
        let session = handle.session(transfer_id).await?;
        if session.role != Role::Sender {
            return None;
        }
        Some(match notification.event {
            TransferEvent::Created => SendEvent::Created { transfer_id },
            TransferEvent::Connecting => SendEvent::Connecting { transfer_id },
            TransferEvent::Authenticating => SendEvent::Authenticating { transfer_id },
            TransferEvent::Started => SendEvent::Started { transfer_id },
            TransferEvent::CodeAvailable => SendEvent::CodeAvailable {
                transfer_id,
                code: session.code?,
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
            TransferEvent::CapabilityUnavailable { capability } => {
                SendEvent::CapabilityUnavailable {
                    transfer_id,
                    capability: match capability {
                        TransferCapability::Progress => TransferCapability::Progress,
                    },
                }
            }
            TransferEvent::Verifying => SendEvent::Verifying { transfer_id },
            TransferEvent::Completed => SendEvent::Completed { transfer_id },
            TransferEvent::Failed => SendEvent::Failed {
                transfer_id,
                message: session
                    .error
                    .unwrap_or_else(|| "The transfer failed.".to_owned()),
            },
            TransferEvent::Cancelled => SendEvent::Cancelled { transfer_id },
            TransferEvent::Connected
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
    use drift_ui::SendEvent;

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
    }
}
