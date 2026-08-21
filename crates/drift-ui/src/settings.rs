use std::{fmt, future::Future, pin::Pin};

pub type SettingsFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Clone, PartialEq, Eq)]
pub struct RelaySettingsSnapshot {
    enabled: bool,
    endpoint: Option<String>,
}

impl fmt::Debug for RelaySettingsSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelaySettingsSnapshot")
            .field("enabled", &self.enabled)
            .field("endpoint_configured", &self.endpoint.is_some())
            .finish()
    }
}

impl RelaySettingsSnapshot {
    pub fn new(enabled: bool, endpoint: Option<String>) -> Self {
        Self { enabled, endpoint }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettingsCommandErrorKind {
    LoadFailed,
    InvalidRelay,
    SaveFailed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SettingsCommandError {
    kind: SettingsCommandErrorKind,
}

impl SettingsCommandError {
    pub fn new(kind: SettingsCommandErrorKind) -> Self {
        Self { kind }
    }

    pub fn kind(self) -> SettingsCommandErrorKind {
        self.kind
    }

    pub fn message(self) -> &'static str {
        match self.kind {
            SettingsCommandErrorKind::LoadFailed => "Relay settings could not be loaded.",
            SettingsCommandErrorKind::InvalidRelay => "Enter a valid relay endpoint.",
            SettingsCommandErrorKind::SaveFailed => "Relay settings could not be saved.",
        }
    }
}

impl fmt::Display for SettingsCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for SettingsCommandError {}

pub trait SettingsController: Send + Sync {
    fn load(&self) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>>;

    fn save(
        &self,
        enabled: bool,
        endpoint: Option<String>,
    ) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>>;

    fn clear(&self) -> SettingsFuture<Result<RelaySettingsSnapshot, SettingsCommandError>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettingsPhase {
    Loading,
    Ready,
    Editing,
    Saving,
    Saved,
    Invalid,
    Failed,
}

impl SettingsPhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Loading => "Loading relay settings",
            Self::Ready => "Relay settings ready",
            Self::Editing => "Relay settings changed",
            Self::Saving => "Saving relay settings",
            Self::Saved => "Relay settings saved",
            Self::Invalid => "Relay endpoint is invalid",
            Self::Failed => "Relay settings could not be saved",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SettingsAction {
    SetEnabled { enabled: bool },
    UpdateEndpoint { endpoint: String },
    Save,
    Clear,
}

impl fmt::Debug for SettingsAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SetEnabled { enabled } => formatter
                .debug_struct("SetEnabled")
                .field("enabled", enabled)
                .finish(),
            Self::UpdateEndpoint { .. } => formatter
                .debug_struct("UpdateEndpoint")
                .field("endpoint_configured", &true)
                .finish(),
            Self::Save => formatter.write_str("Save"),
            Self::Clear => formatter.write_str("Clear"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum SettingsIntent {
    Save {
        enabled: bool,
        endpoint: Option<String>,
    },
    Clear,
}

impl fmt::Debug for SettingsIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Save { enabled, endpoint } => formatter
                .debug_struct("Save")
                .field("enabled", enabled)
                .field("endpoint_configured", &endpoint.is_some())
                .finish(),
            Self::Clear => formatter.write_str("Clear"),
        }
    }
}

pub struct SettingsViewState {
    enabled: bool,
    endpoint: String,
    phase: SettingsPhase,
    error: Option<String>,
}

impl SettingsViewState {
    pub fn new() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            phase: SettingsPhase::Loading,
            error: None,
        }
    }

    pub fn from_snapshot(snapshot: RelaySettingsSnapshot) -> Self {
        let mut state = Self::new();
        state.apply_loaded(snapshot);
        state
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn phase(&self) -> SettingsPhase {
        self.phase
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn input_enabled(&self) -> bool {
        !matches!(self.phase, SettingsPhase::Loading | SettingsPhase::Saving)
    }

    pub fn save_enabled(&self) -> bool {
        self.input_enabled()
    }

    pub fn clear_enabled(&self) -> bool {
        self.input_enabled()
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        if !self.input_enabled() {
            return;
        }
        self.enabled = enabled;
        self.phase = SettingsPhase::Editing;
        self.error = None;
    }

    pub fn set_endpoint(&mut self, endpoint: impl Into<String>) {
        if !self.input_enabled() {
            return;
        }
        self.endpoint = endpoint.into();
        self.phase = SettingsPhase::Editing;
        self.error = None;
    }

    pub fn handle_action(&mut self, action: SettingsAction) -> Option<SettingsIntent> {
        match action {
            SettingsAction::SetEnabled { enabled } => {
                self.set_enabled(enabled);
                None
            }
            SettingsAction::UpdateEndpoint { endpoint } => {
                self.set_endpoint(endpoint);
                None
            }
            SettingsAction::Save if self.save_enabled() => {
                self.phase = SettingsPhase::Saving;
                self.error = None;
                Some(SettingsIntent::Save {
                    enabled: self.enabled,
                    endpoint: (!self.endpoint.is_empty()).then(|| self.endpoint.clone()),
                })
            }
            SettingsAction::Clear if self.clear_enabled() => {
                self.phase = SettingsPhase::Saving;
                self.error = None;
                Some(SettingsIntent::Clear)
            }
            _ => None,
        }
    }

    pub fn apply_loaded(&mut self, snapshot: RelaySettingsSnapshot) {
        self.enabled = snapshot.enabled;
        self.endpoint = snapshot.endpoint.unwrap_or_default();
        self.phase = SettingsPhase::Ready;
        self.error = None;
    }

    pub fn mark_saved(&mut self, snapshot: RelaySettingsSnapshot) {
        self.apply_loaded(snapshot);
        self.phase = SettingsPhase::Saved;
    }

    pub fn mark_failed(&mut self, error: SettingsCommandError) {
        self.phase = if error.kind() == SettingsCommandErrorKind::InvalidRelay {
            SettingsPhase::Invalid
        } else {
            SettingsPhase::Failed
        };
        self.error = Some(error.message().to_owned());
    }
}

impl Default for SettingsViewState {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SettingsViewState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SettingsViewState")
            .field("enabled", &self.enabled)
            .field("endpoint_configured", &(!self.endpoint.is_empty()))
            .field("phase", &self.phase)
            .field("error", &self.error)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_state_loads_and_emits_a_redacted_save_intent() {
        let snapshot =
            RelaySettingsSnapshot::new(true, Some("https://relay.example.test:443".to_owned()));
        let mut state = SettingsViewState::from_snapshot(snapshot);
        state.set_endpoint("https://new-relay.example.test");

        assert_eq!(state.phase(), SettingsPhase::Editing);
        assert!(matches!(
            state.handle_action(SettingsAction::Save),
            Some(SettingsIntent::Save {
                enabled: true,
                endpoint: Some(endpoint),
            }) if endpoint == "https://new-relay.example.test"
        ));
        assert_eq!(state.phase(), SettingsPhase::Saving);
        assert!(!format!("{state:?}").contains("new-relay.example.test"));
    }

    #[test]
    fn relay_snapshot_debug_redacts_endpoint() {
        let snapshot = RelaySettingsSnapshot::new(
            true,
            Some("https://user:pass@relay.example.test".to_owned()),
        );
        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("user:pass@relay.example.test"));
        assert!(debug.contains("endpoint_configured: true"));
    }

    #[test]
    fn settings_state_clear_resets_after_success() {
        let snapshot =
            RelaySettingsSnapshot::new(true, Some("https://relay.example.test".to_owned()));
        let mut state = SettingsViewState::from_snapshot(snapshot);
        assert_eq!(
            state.handle_action(SettingsAction::Clear),
            Some(SettingsIntent::Clear)
        );
        assert_eq!(state.phase(), SettingsPhase::Saving);

        state.mark_saved(RelaySettingsSnapshot::new(false, None));
        assert!(!state.enabled());
        assert_eq!(state.endpoint(), "");
        assert_eq!(state.phase(), SettingsPhase::Saved);
    }

    #[test]
    fn settings_state_keeps_editable_values_after_save_failure() {
        let mut state = SettingsViewState::from_snapshot(RelaySettingsSnapshot::new(false, None));
        state.set_enabled(true);
        state.set_endpoint("ftp://relay.example.test");
        state.handle_action(SettingsAction::Save);
        state.mark_failed(SettingsCommandError::new(
            SettingsCommandErrorKind::InvalidRelay,
        ));

        assert!(state.enabled());
        assert_eq!(state.endpoint(), "ftp://relay.example.test");
        assert_eq!(state.phase(), SettingsPhase::Invalid);
        assert_eq!(state.error(), Some("Enter a valid relay endpoint."));
        assert!(state.save_enabled());
    }
}
