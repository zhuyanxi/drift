use drift_core::Progress;

pub(crate) fn accept_progress(
    previous: Option<Progress>,
    transferred: u64,
    total: u64,
    speed_bps: u64,
) -> Option<Progress> {
    match previous {
        Some(previous) => previous.update(transferred, total, speed_bps).ok(),
        None => Progress::new(transferred, total, speed_bps).ok(),
    }
}

pub(crate) fn eta_seconds(transferred: u64, total: u64, speed_bps: u64) -> Option<u64> {
    Progress::new(transferred, total, speed_bps)
        .ok()
        .and_then(Progress::eta_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_or_unknown_progress_has_no_eta() {
        assert_eq!(eta_seconds(0, 0, 100), None);
        assert_eq!(eta_seconds(25, 100, 0), None);
        assert_eq!(eta_seconds(101, 100, 25), None);
    }

    #[test]
    fn progress_update_allows_unknown_total_to_become_known() {
        let next = accept_progress(Some(Progress::new(0, 0, 0).unwrap()), 25, 100, 25);
        assert_eq!(next.and_then(Progress::eta_seconds), Some(3));
    }
}
