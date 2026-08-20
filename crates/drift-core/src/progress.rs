use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProgressError {
    #[error("transferred bytes cannot exceed total bytes")]
    TransferredExceedsTotal,
    #[error("transferred bytes cannot decrease")]
    TransferredDecreased,
    #[error("progress total cannot change after it is known")]
    TotalChanged,
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

    pub fn update(
        self,
        transferred_bytes: u64,
        total_bytes: u64,
        speed_bps: u64,
    ) -> Result<Self, ProgressError> {
        let next = Self::new(transferred_bytes, total_bytes, speed_bps)?;
        if next.transferred_bytes < self.transferred_bytes {
            return Err(ProgressError::TransferredDecreased);
        }
        if self.total_bytes != 0 && next.total_bytes != self.total_bytes {
            return Err(ProgressError::TotalChanged);
        }
        Ok(next)
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

    #[test]
    fn allows_unknown_total_to_become_known() {
        let progress = Progress::new(0, 0, 0).unwrap();
        assert_eq!(
            progress.update(4, 10, 2).unwrap(),
            Progress {
                transferred_bytes: 4,
                total_bytes: 10,
                speed_bps: 2,
            }
        );
    }

    #[test]
    fn rejects_decreasing_bytes_and_known_total_changes() {
        let progress = Progress::new(5, 10, 5).unwrap();
        assert_eq!(
            progress.update(4, 10, 4),
            Err(ProgressError::TransferredDecreased)
        );
        assert_eq!(
            progress.update(6, 11, 4),
            Err(ProgressError::TotalChanged)
        );
    }
}
