use std::{future::Future, path::PathBuf, pin::Pin};
use thiserror::Error;
use tokio::process::Command;

pub type RevealFuture = Pin<Box<dyn Future<Output = Result<(), RevealError>> + Send + 'static>>;

pub trait DestinationRevealer: Send + Sync {
    fn reveal(&self, path: PathBuf) -> RevealFuture;
}

#[derive(Debug, Error)]
pub enum RevealError {
    #[error("destination reveal is unsupported")]
    Unsupported,
    #[error("destination reveal failed")]
    Failed,
}

pub struct SystemDestinationRevealer;

impl DestinationRevealer for SystemDestinationRevealer {
    fn reveal(&self, path: PathBuf) -> RevealFuture {
        Box::pin(async move {
            #[cfg(target_os = "macos")]
            let status = Command::new("open")
                .arg("-R")
                .arg(path)
                .status()
                .await
                .map_err(|_| RevealError::Failed)?;

            #[cfg(target_os = "linux")]
            let status = Command::new("xdg-open")
                .arg(path)
                .status()
                .await
                .map_err(|_| RevealError::Failed)?;

            #[cfg(target_os = "windows")]
            let status = Command::new("explorer")
                .arg(path)
                .status()
                .await
                .map_err(|_| RevealError::Failed)?;

            #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
            {
                let _ = path;
                Err(RevealError::Unsupported)
            }

            #[cfg(any(target_os = "macos", target_os = "linux", target_os = "windows"))]
            if status.success() {
                Ok(())
            } else {
                Err(RevealError::Failed)
            }
        })
    }
}
