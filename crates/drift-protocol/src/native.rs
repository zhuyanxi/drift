use crate::backend::{
    BackendAvailability, BackendCapabilities, BackendError, BackendUnavailableReason,
    ReceiveRequest, SendRequest, TransferBackend, TransferHandle,
};
use async_trait::async_trait;

pub const NATIVE_PROTOCOL_VERSION: &str = "0.1";

#[derive(Clone, Debug, Default)]
pub struct NativeBackend;

impl NativeBackend {
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransferBackend for NativeBackend {
    fn name(&self) -> &'static str {
        "native"
    }

    fn version(&self) -> Option<&'static str> {
        Some(NATIVE_PROTOCOL_VERSION)
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::new(false, false, false).with_connection_modes(false, false)
    }

    fn availability(&self) -> BackendAvailability {
        BackendAvailability::Unavailable {
            reason: BackendUnavailableReason::NotImplemented,
        }
    }

    async fn send(&self, _request: SendRequest) -> Result<Box<dyn TransferHandle>, BackendError> {
        Err(BackendError::Unavailable {
            reason: BackendUnavailableReason::NotImplemented,
        })
    }

    async fn receive(
        &self,
        _request: ReceiveRequest,
    ) -> Result<Box<dyn TransferHandle>, BackendError> {
        Err(BackendError::Unavailable {
            reason: BackendUnavailableReason::NotImplemented,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_neutral_identity_and_unavailable_readiness() {
        let info = NativeBackend::new().info();

        assert_eq!(info.name, "native");
        assert_eq!(info.version, Some(NATIVE_PROTOCOL_VERSION));
        assert_eq!(
            info.availability,
            BackendAvailability::Unavailable {
                reason: BackendUnavailableReason::NotImplemented,
            }
        );
        assert!(!info.capabilities.supports(crate::BackendCapability::Direct));
        assert!(!info.capabilities.supports(crate::BackendCapability::Relay));
    }

    #[tokio::test]
    async fn fails_closed_without_protocol_dependencies() {
        let backend = NativeBackend::new();
        let request = SendRequest::new(vec![std::path::PathBuf::from("source")]).unwrap();

        assert!(matches!(
            backend.check_ready().await,
            Err(BackendError::Unavailable {
                reason: BackendUnavailableReason::NotImplemented,
            })
        ));
        assert!(matches!(
            backend.send(request).await,
            Err(BackendError::Unavailable {
                reason: BackendUnavailableReason::NotImplemented,
            })
        ));
    }
}
