use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProgressError {
    #[error("transferred bytes cannot exceed total bytes")]
    TransferredExceedsTotal,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Progress {
    pub transferred_bytes: u64,
    pub total_bytes: u64,
    pub speed_bps: u64,
}

impl Progress {
    pub fn new(
        transferred_bytes: u64,
        total_bytes: u64,
        speed_bps: u64,
    ) -> Result<Self, ProgressError> {
        if transferred_bytes > total_bytes {
            return Err(ProgressError::TransferredExceedsTotal);
        }
        Ok(Self {
            transferred_bytes,
            total_bytes,
            speed_bps,
        })
    }

    pub fn percent(self) -> f64 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.transferred_bytes as f64 / self.total_bytes as f64 * 100.0
    }

    pub fn eta_seconds(self) -> Option<u64> {
        if self.speed_bps == 0 || self.transferred_bytes >= self.total_bytes {
            return None;
        }
        Some((self.total_bytes - self.transferred_bytes).div_ceil(self.speed_bps))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculates_percent_and_eta() {
        let progress = Progress::new(25, 100, 25).unwrap();
        assert_eq!(progress.percent(), 25.0);
        assert_eq!(progress.eta_seconds(), Some(3));
    }

    #[test]
    fn rejects_invalid_progress() {
        assert_eq!(
            Progress::new(2, 1, 0),
            Err(ProgressError::TransferredExceedsTotal)
        );
    }
}
