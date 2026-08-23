use crate::event_bridge::{AppEventBridge, AppTransferUpdate, TransferPresentation};
use crate::platform::{DestinationRevealer, RevealError, SystemDestinationRevealer};
use crate::settings::{
    ConfigPathError, DriftSettings, RelaySettings, SettingsError, SettingsLoader, SettingsSource,
    SettingsStore,
};
use drift_core::{
    ResumeRequest, ResumeState, TransferCapability, TransferError, TransferId, TransferManifest,
};
use drift_protocol::{BackendCapabilities, BackendError, CrocBackend, ReceiveRequest, SendRequest};
use drift_storage::{
    scan_send_paths, validate_receive_directory, DestinationError, JsonStore, ResumeDiscovery,
    ScanCancellation, SourceScan, SourceScanError, StorageError,
};
use drift_transfer::TransferManager;
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    runtime::{Builder, Handle, Runtime},
    sync::broadcast,
    task::JoinHandle,
};

#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration path is unavailable")]
    ConfigPath(#[source] ConfigPathError),
    #[error("configuration could not be loaded")]
    Settings(#[source] SettingsError),
    #[error("application runtime could not be initialized")]
    Runtime(#[source] std::io::Error),
}

impl AppError {
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::ConfigPath(_) => "Drift could not determine its configuration location.",
            Self::Settings(_) => {
                "Drift could not load valid settings. Check the configuration file."
            }
            Self::Runtime(_) => "Drift could not initialize its transfer runtime.",
        }
    }
}

pub struct AppState {
    settings: DriftSettings,
    settings_source: SettingsSource,
    config_path: PathBuf,
    backend: CrocBackend,
    transfer_manager: TransferManager<CrocBackend>,
    event_bridge: AppEventBridge,
    resume_store: JsonStore,
    settings_store: SettingsStore,
    destination_revealer: Arc<dyn DestinationRevealer>,
    runtime: Runtime,
}

impl AppState {
    pub fn bootstrap() -> Result<Self, AppError> {
        let loader = SettingsLoader::new().map_err(AppError::ConfigPath)?;
        Self::from_loader(loader)
    }

    pub fn bootstrap_with_config_path(path: impl Into<PathBuf>) -> Result<Self, AppError> {
        Self::from_loader(SettingsLoader::test_override(path))
    }

    pub fn settings(&self) -> &DriftSettings {
        &self.settings
    }

    pub fn settings_source(&self) -> SettingsSource {
        self.settings_source
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn backend_name(&self) -> &'static str {
        "croc"
    }

    pub fn custom_relay_configured(&self) -> bool {
        self.settings.relay.effective_url().is_some()
    }

    pub fn transfer_manager(&self) -> &TransferManager<CrocBackend> {
        &self.transfer_manager
    }

    pub fn resume_store(&self) -> &JsonStore {
        &self.resume_store
    }

    pub fn handle(&self) -> AppHandle {
        AppHandle {
            runtime: self.runtime.handle().clone(),
            backend: self.backend.clone(),
            transfer_manager: self.transfer_manager.clone(),
            event_bridge: self.event_bridge.clone(),
            resume_store: self.resume_store.clone(),
            settings_store: self.settings_store.clone(),
            default_receive_directory: self.settings.transfer.default_receive_directory.clone(),
            destination_revealer: Arc::clone(&self.destination_revealer),
        }
    }

    fn from_loader(loader: SettingsLoader) -> Result<Self, AppError> {
        let loaded = loader.load().map_err(AppError::Settings)?;
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(AppError::Runtime)?;
        let backend = CrocBackend::new(&loaded.settings.transfer.croc_executable)
            .with_timeout(loaded.settings.transfer.timeout);
        let settings_store = SettingsStore::new(loader.clone(), loaded.settings.clone());
        let resume_store = JsonStore::new(resume_root(loader.path()));
        let transfer_manager =
            TransferManager::with_resume_store(backend.clone(), "croc", resume_store.clone());
        let event_bridge = AppEventBridge::start(runtime.handle(), transfer_manager.clone());

        Ok(Self {
            settings: loaded.settings,
            settings_source: loaded.source,
            config_path: loader.path().to_path_buf(),
            backend,
            transfer_manager,
            event_bridge,
            resume_store,
            settings_store,
            destination_revealer: Arc::new(SystemDestinationRevealer),
            runtime,
        })
    }
}

#[derive(Clone)]
pub struct AppHandle {
    runtime: Handle,
    backend: CrocBackend,
    transfer_manager: TransferManager<CrocBackend>,
    event_bridge: AppEventBridge,
    resume_store: JsonStore,
    settings_store: SettingsStore,
    default_receive_directory: PathBuf,
    destination_revealer: Arc<dyn DestinationRevealer>,
}

impl AppHandle {
    /// Scans sender paths on Drift's Tokio runtime.
    ///
    /// UI futures run on GPUI's executor, which does not provide Tokio's filesystem reactor.
    pub fn scan_send_paths(
        &self,
        paths: Vec<PathBuf>,
        cancellation: ScanCancellation,
    ) -> JoinHandle<Result<SourceScan, SourceScanError>> {
        self.runtime
            .spawn(async move { scan_send_paths(paths, cancellation).await })
    }

    pub fn preflight(&self) -> JoinHandle<Result<(), AppCommandError>> {
        let backend = self.backend.clone();
        self.runtime.spawn(async move {
            backend
                .preflight()
                .await
                .map(|_| ())
                .map_err(AppCommandError::Preflight)
        })
    }

    pub fn default_receive_directory(&self) -> PathBuf {
        self.default_receive_directory.clone()
    }

    pub fn backend_capabilities(&self) -> BackendCapabilities {
        self.transfer_manager.capabilities()
    }

    pub fn relay_configured(&self) -> Option<bool> {
        self.settings_store
            .snapshot()
            .ok()
            .map(|settings| settings.relay.effective_url().is_some())
    }

    pub async fn sessions(&self) -> Vec<drift_core::TransferSession> {
        self.transfer_manager.sessions().await
    }

    pub async fn destination_available(&self, transfer_id: TransferId) -> bool {
        self.transfer_manager
            .completed_receive_destination(transfer_id)
            .await
            .is_some()
    }

    pub fn reveal_destination(
        &self,
        transfer_id: TransferId,
    ) -> JoinHandle<Result<TransferId, AppCommandError>> {
        let transfer_manager = self.transfer_manager.clone();
        let revealer = Arc::clone(&self.destination_revealer);
        self.runtime.spawn(async move {
            let destination = transfer_manager
                .completed_receive_destination(transfer_id)
                .await
                .ok_or(AppCommandError::DestinationUnavailable)?;
            revealer
                .reveal(destination)
                .await
                .map_err(AppCommandError::from)?;
            Ok(transfer_id)
        })
    }

    pub fn settings(&self) -> JoinHandle<Result<DriftSettings, AppCommandError>> {
        let settings_store = self.settings_store.clone();
        self.runtime.spawn_blocking(move || {
            settings_store
                .snapshot()
                .map_err(AppCommandError::SettingsLoad)
        })
    }

    pub fn update_relay(
        &self,
        relay: RelaySettings,
    ) -> JoinHandle<Result<DriftSettings, AppCommandError>> {
        let settings_store = self.settings_store.clone();
        self.runtime.spawn_blocking(move || {
            settings_store
                .update_relay(relay)
                .map_err(AppCommandError::Settings)
        })
    }

    pub fn clear_relay(&self) -> JoinHandle<Result<DriftSettings, AppCommandError>> {
        let settings_store = self.settings_store.clone();
        self.runtime.spawn_blocking(move || {
            settings_store
                .clear_relay()
                .map_err(AppCommandError::Settings)
        })
    }

    pub fn validate_destination(&self, path: PathBuf) -> JoinHandle<Result<(), AppCommandError>> {
        self.runtime.spawn(async move {
            validate_receive_directory(path)
                .await
                .map_err(AppCommandError::from)
        })
    }

    pub fn recoveries(&self) -> JoinHandle<Result<ResumeDiscovery, AppCommandError>> {
        let store = self.resume_store.clone();
        self.runtime.spawn(async move {
            store
                .discover_resumes()
                .await
                .map_err(AppCommandError::from)
        })
    }

    pub fn dispatch(&self, command: AppCommand) -> JoinHandle<Result<TransferId, AppCommandError>> {
        let transfer_manager = self.transfer_manager.clone();
        let resume_store = self.resume_store.clone();
        let settings_store = self.settings_store.clone();
        let default_receive_directory = self.default_receive_directory.clone();
        self.runtime.spawn(async move {
            dispatch_command(
                transfer_manager,
                resume_store,
                settings_store,
                default_receive_directory,
                command,
            )
            .await
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppTransferUpdate> {
        self.event_bridge.subscribe()
    }

    pub async fn session(&self, transfer_id: TransferId) -> Option<drift_core::TransferSession> {
        self.transfer_manager.session(transfer_id).await
    }

    pub async fn presentation(&self, transfer_id: TransferId) -> Option<TransferPresentation> {
        self.event_bridge.presentation(transfer_id).await
    }
}

pub enum AppCommand {
    Send {
        paths: Vec<PathBuf>,
        manifest: TransferManifest,
    },
    Receive {
        code: String,
        output_directory: Option<PathBuf>,
    },
    Cancel {
        transfer_id: TransferId,
    },
    RetryTransfer {
        transfer_id: TransferId,
    },
    PauseTransfer {
        transfer_id: TransferId,
    },
    ResumeTransfer {
        transfer_id: TransferId,
    },
    RecoverTransfer {
        transfer_id: TransferId,
        code: Option<String>,
        output_directory: Option<PathBuf>,
    },
    DiscardRecovery {
        transfer_id: TransferId,
    },
}

impl fmt::Debug for AppCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Send { paths, .. } => formatter
                .debug_struct("Send")
                .field("path_count", &paths.len())
                .finish(),
            Self::Receive {
                output_directory, ..
            } => formatter
                .debug_struct("Receive")
                .field("code", &"[REDACTED]")
                .field("output_directory_configured", &output_directory.is_some())
                .finish(),
            Self::Cancel { transfer_id } => formatter
                .debug_struct("Cancel")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::RetryTransfer { transfer_id } => formatter
                .debug_struct("RetryTransfer")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::PauseTransfer { transfer_id } => formatter
                .debug_struct("PauseTransfer")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::ResumeTransfer { transfer_id } => formatter
                .debug_struct("ResumeTransfer")
                .field("transfer_id", transfer_id)
                .finish(),
            Self::RecoverTransfer {
                transfer_id,
                code,
                output_directory,
            } => formatter
                .debug_struct("RecoverTransfer")
                .field("transfer_id", transfer_id)
                .field("code_configured", &code.is_some())
                .field("output_directory_configured", &output_directory.is_some())
                .finish(),
            Self::DiscardRecovery { transfer_id } => formatter
                .debug_struct("DiscardRecovery")
                .field("transfer_id", transfer_id)
                .finish(),
        }
    }
}

#[derive(Debug, Error)]
pub enum AppCommandError {
    #[error("backend preflight failed")]
    Preflight(#[source] BackendError),
    #[error("invalid transfer request")]
    InvalidRequest(#[source] BackendError),
    #[error("output directory must not be empty")]
    EmptyOutputDirectory,
    #[error("output directory is unavailable")]
    OutputDirectoryUnavailable,
    #[error("output directory is not writable")]
    OutputDirectoryNotWritable,
    #[error("completed receive destination is unavailable")]
    DestinationUnavailable,
    #[error("completed receive destination could not be revealed")]
    RevealFailed,
    #[error("transfer code must not be empty")]
    EmptyTransferCode,
    #[error("transfer command failed")]
    Transfer(#[source] drift_core::TransferError),
    #[error("recovery metadata is unavailable")]
    RecoveryUnavailable,
    #[error("recovery metadata is invalid")]
    RecoveryInvalid,
    #[error("recovery storage is unavailable")]
    RecoveryStorage(#[source] StorageError),
    #[error("settings could not be loaded")]
    SettingsLoad(#[source] SettingsError),
    #[error("settings could not be saved")]
    Settings(#[source] SettingsError),
}

impl AppCommandError {
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::Preflight(_) => "Croc is not ready.",
            Self::InvalidRequest(_) => "The transfer request is invalid.",
            Self::EmptyOutputDirectory => "The receive folder is unavailable.",
            Self::OutputDirectoryUnavailable => "The receive folder is unavailable.",
            Self::OutputDirectoryNotWritable => "The receive folder is not writable.",
            Self::DestinationUnavailable => "The completed receive destination is unavailable.",
            Self::RevealFailed => "The completed receive destination could not be revealed.",
            Self::EmptyTransferCode => "Enter a transfer code.",
            Self::Transfer(TransferError::CapabilityUnavailable(TransferCapability::Pause)) => {
                "Pause is unavailable for this backend."
            }
            Self::Transfer(TransferError::CapabilityUnavailable(TransferCapability::Resume)) => {
                "Resume is unavailable for this backend."
            }
            Self::Transfer(TransferError::Filesystem(_)) => {
                "The received files could not be finalized."
            }
            Self::Transfer(_) => "The transfer could not start.",
            Self::RecoveryUnavailable | Self::RecoveryInvalid => {
                "The transfer recovery is no longer available."
            }
            Self::RecoveryStorage(_) => "Transfer recovery storage is unavailable.",
            Self::SettingsLoad(_) => "Settings could not be loaded.",
            Self::Settings(_) => "Settings could not be saved.",
        }
    }
}

impl From<drift_core::TransferError> for AppCommandError {
    fn from(error: drift_core::TransferError) -> Self {
        Self::Transfer(error)
    }
}

impl From<DestinationError> for AppCommandError {
    fn from(error: DestinationError) -> Self {
        match error {
            DestinationError::Empty => Self::EmptyOutputDirectory,
            DestinationError::Unavailable | DestinationError::NotDirectory => {
                Self::OutputDirectoryUnavailable
            }
            DestinationError::NotWritable => Self::OutputDirectoryNotWritable,
            DestinationError::SymlinkNotAllowed => Self::OutputDirectoryUnavailable,
        }
    }
}

impl From<StorageError> for AppCommandError {
    fn from(error: StorageError) -> Self {
        Self::RecoveryStorage(error)
    }
}

impl From<RevealError> for AppCommandError {
    fn from(error: RevealError) -> Self {
        match error {
            RevealError::Unsupported | RevealError::Failed => Self::RevealFailed,
        }
    }
}

impl From<SettingsError> for AppCommandError {
    fn from(error: SettingsError) -> Self {
        Self::SettingsLoad(error)
    }
}

async fn dispatch_command(
    transfer_manager: TransferManager<CrocBackend>,
    resume_store: JsonStore,
    settings_store: SettingsStore,
    default_receive_directory: PathBuf,
    command: AppCommand,
) -> Result<TransferId, AppCommandError> {
    match command {
        AppCommand::Send { paths, manifest } => {
            let settings = settings_store
                .snapshot()
                .map_err(AppCommandError::SettingsLoad)?;
            let request = SendRequest::new(paths).map_err(AppCommandError::InvalidRequest)?;
            let request = apply_relay(request, &settings);
            transfer_manager
                .start_send_with_manifest(request, Some(manifest))
                .await
                .map_err(AppCommandError::Transfer)
        }
        AppCommand::Receive {
            code,
            output_directory,
        } => {
            let settings = settings_store
                .snapshot()
                .map_err(AppCommandError::SettingsLoad)?;
            let output_directory = output_directory.unwrap_or(default_receive_directory);
            if code.trim().is_empty() {
                return Err(AppCommandError::EmptyTransferCode);
            }
            validate_receive_directory(&output_directory)
                .await
                .map_err(AppCommandError::from)?;
            let request = ReceiveRequest::new(code, output_directory)
                .map_err(AppCommandError::InvalidRequest)?;
            let request = apply_relay(request, &settings);
            transfer_manager
                .start_receive(request)
                .await
                .map_err(AppCommandError::Transfer)
        }
        AppCommand::Cancel { transfer_id } => transfer_manager
            .cancel(transfer_id)
            .await
            .map(|()| transfer_id)
            .map_err(AppCommandError::Transfer),
        AppCommand::RetryTransfer { transfer_id } => {
            transfer_manager
                .retry_with_receive_validation(transfer_id, |output_directory| async move {
                    validate_receive_directory(&output_directory)
                        .await
                        .map_err(AppCommandError::from)
                })
                .await
        }
        AppCommand::PauseTransfer { transfer_id } => transfer_manager
            .pause(transfer_id)
            .await
            .map(|()| transfer_id)
            .map_err(AppCommandError::Transfer),
        AppCommand::ResumeTransfer { transfer_id } => transfer_manager
            .resume(transfer_id)
            .await
            .map(|()| transfer_id)
            .map_err(AppCommandError::Transfer),
        AppCommand::RecoverTransfer {
            transfer_id,
            code,
            output_directory,
        } => {
            let state = resume_store
                .load_resume(transfer_id)
                .await
                .map_err(AppCommandError::from)?
                .ok_or(AppCommandError::RecoveryUnavailable)?;
            validate_recovery_inputs(&state, code.as_deref(), output_directory.as_deref()).await?;
            let new_transfer_id = transfer_manager
                .recover(state, code, output_directory)
                .await
                .map_err(AppCommandError::Transfer)?;
            Ok(new_transfer_id)
        }
        AppCommand::DiscardRecovery { transfer_id } => resume_store
            .discard_resume(transfer_id)
            .await
            .map(|()| transfer_id)
            .map_err(AppCommandError::from),
    }
}

fn apply_relay<T>(request: T, settings: &DriftSettings) -> T
where
    T: RelayRequest,
{
    match settings.relay.effective_url() {
        Some(relay) => request.with_relay(relay.to_owned()),
        None => request,
    }
}

trait RelayRequest: Sized {
    fn with_relay(self, relay: String) -> Self;
}

impl RelayRequest for SendRequest {
    fn with_relay(self, relay: String) -> Self {
        SendRequest::with_relay(self, relay)
    }
}

impl RelayRequest for ReceiveRequest {
    fn with_relay(self, relay: String) -> Self {
        ReceiveRequest::with_relay(self, relay)
    }
}

async fn validate_recovery_inputs(
    state: &ResumeState,
    receive_code: Option<&str>,
    replacement_output_directory: Option<&Path>,
) -> Result<(), AppCommandError> {
    match &state.request {
        ResumeRequest::Send { source_paths } => {
            let scan =
                scan_send_paths(source_paths.clone(), drift_storage::ScanCancellation::new())
                    .await
                    .map_err(|_| AppCommandError::RecoveryInvalid)?;
            let Some(expected) = &state.manifest else {
                return Err(AppCommandError::RecoveryInvalid);
            };
            if !manifests_match(expected, scan.manifest()) {
                return Err(AppCommandError::RecoveryInvalid);
            }
        }
        ResumeRequest::Receive { output_directory } => {
            if receive_code.is_none_or(|code| code.trim().is_empty()) {
                return Err(AppCommandError::EmptyTransferCode);
            }
            let output_directory = replacement_output_directory.unwrap_or(output_directory);
            validate_receive_directory(output_directory.to_path_buf())
                .await
                .map_err(AppCommandError::from)?;
        }
    }
    Ok(())
}

fn manifests_match(expected: &TransferManifest, actual: &TransferManifest) -> bool {
    expected.total_size == actual.total_size
        && expected.files.len() == actual.files.len()
        && expected.files.iter().all(|expected_file| {
            actual.files.iter().any(|actual_file| {
                expected_file.relative_path == actual_file.relative_path
                    && expected_file.size == actual_file.size
                    && expected_file.modified_at == actual_file.modified_at
                    && expected_file.digest == actual_file.digest
            })
        })
}

fn resume_root(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("state")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};
    use uuid::Uuid;

    #[cfg(unix)]
    fn write_script(body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!("drift-app-test-{}", Uuid::new_v4()));
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!("drift-app-state-{}", Uuid::new_v4()))
    }

    #[test]
    fn composes_services_from_override_settings() {
        let root = temp_path();
        let config_path = root.join("config").join("config.json");
        let receive_directory = root.join("received");
        fs::create_dir_all(&receive_directory).unwrap();
        let mut settings = DriftSettings::default();
        settings.transfer.default_receive_directory = receive_directory.clone();
        settings.transfer.croc_executable = PathBuf::from("custom-croc");

        SettingsLoader::with_path(&config_path)
            .save(&settings)
            .unwrap();
        let state = AppState::bootstrap_with_config_path(&config_path).unwrap();

        assert_eq!(state.settings(), &settings);
        assert_eq!(state.settings_source(), SettingsSource::File);
        assert_eq!(
            state.resume_store().root(),
            root.join("config").join("state")
        );
        assert_eq!(state.backend_name(), "croc");
        assert!(!state.custom_relay_configured());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn startup_error_message_does_not_include_configuration_details() {
        let root = temp_path();
        let config_path = root.join("config.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(&config_path, b"not-json").unwrap();
        let error = match AppState::bootstrap_with_config_path(&config_path) {
            Ok(_) => panic!("invalid configuration unexpectedly loaded"),
            Err(error) => error,
        };
        assert_eq!(
            error.user_message(),
            "Drift could not load valid settings. Check the configuration file."
        );
        assert!(!error.user_message().contains("not-json"));
        assert!(matches!(error, AppError::Settings(SettingsError::Parse(_))));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn command_debug_redacts_receive_code() {
        let command = AppCommand::Receive {
            code: "secret-transfer-code".into(),
            output_directory: Some(PathBuf::from("/tmp/received")),
        };
        let debug = format!("{command:?}");
        assert!(!debug.contains("secret-transfer-code"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn relay_settings_are_snapshotted_for_send_and_receive_requests() {
        let root = temp_path();
        let loader = SettingsLoader::with_path(root.join("config.json"));
        let store = SettingsStore::new(loader, DriftSettings::default());
        store
            .update_relay(RelaySettings {
                enabled: true,
                url: Some("https://first-relay.example.test".into()),
            })
            .unwrap();

        let first_settings = store.snapshot().unwrap();
        let send = apply_relay(
            SendRequest::new(vec![PathBuf::from("source.txt")]).unwrap(),
            &first_settings,
        );
        let receive = apply_relay(
            ReceiveRequest::new("transfer-code", &root).unwrap(),
            &first_settings,
        );

        store
            .update_relay(RelaySettings {
                enabled: true,
                url: Some("https://second-relay.example.test".into()),
            })
            .unwrap();

        assert_eq!(
            send.relay.as_deref(),
            Some("https://first-relay.example.test")
        );
        assert_eq!(
            receive.relay.as_deref(),
            Some("https://first-relay.example.test")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn disabled_relay_settings_leave_default_request_routing_untouched() {
        let settings = DriftSettings {
            relay: RelaySettings {
                enabled: false,
                url: Some("https://configured-but-disabled.example.test".into()),
            },
            ..DriftSettings::default()
        };
        let request = apply_relay(
            SendRequest::new(vec![PathBuf::from("source.txt")]).unwrap(),
            &settings,
        );
        assert_eq!(request.relay, None);
    }

    #[test]
    fn dispatch_reports_invalid_settings_as_load_failure() {
        let root = temp_path();
        fs::create_dir_all(&root).unwrap();
        let settings = DriftSettings {
            relay: RelaySettings {
                enabled: true,
                url: Some("ftp://relay.example.test".into()),
            },
            ..DriftSettings::default()
        };
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();

        let result = runtime.block_on(dispatch_command(
            TransferManager::new(CrocBackend::default()),
            JsonStore::new(root.join("state")),
            SettingsStore::new(
                SettingsLoader::with_path(root.join("config.json")),
                settings,
            ),
            root.clone(),
            AppCommand::Receive {
                code: "transfer-code".into(),
                output_directory: Some(root.clone()),
            },
        ));
        let error = result.unwrap_err();

        assert!(matches!(
            &error,
            AppCommandError::SettingsLoad(SettingsError::Validation(
                crate::settings::SettingsValidationError::InvalidRelayUrl
            ))
        ));
        assert_eq!(error.user_message(), "Settings could not be loaded.");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn pause_and_resume_commands_report_unsupported_backend_safely() {
        let root = temp_path();
        let config_path = root.join("config").join("config.json");
        let settings = DriftSettings::default();
        SettingsLoader::with_path(&config_path)
            .save(&settings)
            .unwrap();
        let state = AppState::bootstrap_with_config_path(&config_path).unwrap();
        let transfer_id = TransferId::new();

        let pause = state
            .runtime
            .block_on(
                state
                    .handle()
                    .dispatch(AppCommand::PauseTransfer { transfer_id }),
            )
            .unwrap();
        assert!(matches!(
            pause,
            Err(AppCommandError::Transfer(
                TransferError::CapabilityUnavailable(TransferCapability::Pause)
            ))
        ));
        assert_eq!(
            AppCommandError::Transfer(TransferError::CapabilityUnavailable(
                TransferCapability::Pause
            ))
            .user_message(),
            "Pause is unavailable for this backend."
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reveal_errors_have_safe_user_messages() {
        let unsupported = AppCommandError::from(RevealError::Unsupported);
        let failed = AppCommandError::from(RevealError::Failed);

        assert!(matches!(unsupported, AppCommandError::RevealFailed));
        assert!(matches!(failed, AppCommandError::RevealFailed));
        assert_eq!(
            failed.user_message(),
            "The completed receive destination could not be revealed."
        );
    }

    #[test]
    fn recovery_rejects_missing_sender_source_before_backend_start() {
        let root = temp_path();
        let store = JsonStore::new(root.join("state"));
        let transfer_id = TransferId::new();
        let file = drift_core::FileEntry::new("source.bin", 1).unwrap();
        let manifest = TransferManifest::new(transfer_id, vec![file.clone()]).unwrap();
        let state = ResumeState {
            schema_version: drift_core::RESUME_SCHEMA_VERSION,
            transfer_id,
            backend: "croc".into(),
            backend_version: Some("11.2.x".into()),
            capabilities: drift_core::ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: ResumeRequest::Send {
                source_paths: vec![root.join("missing-source.bin")],
            },
            manifest: Some(manifest),
            file_id: file.file_id,
            chunk_size: drift_core::DEFAULT_RESUME_CHUNK_SIZE,
            file_size: 1,
            completed_chunks: Vec::new(),
            file_digest: None,
            temp_file_path: None,
        };
        let manager = TransferManager::new(CrocBackend::default());
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(store.save_resume(&state)).unwrap();

        let result = runtime.block_on(dispatch_command(
            manager,
            store,
            SettingsStore::new(
                SettingsLoader::with_path(root.join("config.json")),
                DriftSettings::default(),
            ),
            root.clone(),
            AppCommand::RecoverTransfer {
                transfer_id,
                code: None,
                output_directory: None,
            },
        ));
        assert!(matches!(result, Err(AppCommandError::RecoveryInvalid)));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_preserves_metadata_when_backend_is_incompatible() {
        let root = temp_path();
        let output_directory = root.join("received");
        fs::create_dir_all(&output_directory).unwrap();
        let store = JsonStore::new(root.join("state"));
        let transfer_id = TransferId::new();
        let state = ResumeState {
            schema_version: drift_core::RESUME_SCHEMA_VERSION,
            transfer_id,
            backend: "other-backend".into(),
            backend_version: Some("1.0.0".into()),
            capabilities: drift_core::ResumeCapabilities {
                pause: false,
                resume: false,
            },
            request: ResumeRequest::Receive {
                output_directory: output_directory.clone(),
            },
            manifest: None,
            file_id: Uuid::nil(),
            chunk_size: drift_core::DEFAULT_RESUME_CHUNK_SIZE,
            file_size: 0,
            completed_chunks: Vec::new(),
            file_digest: None,
            temp_file_path: None,
        };
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(store.save_resume(&state)).unwrap();

        let result = runtime.block_on(dispatch_command(
            TransferManager::new(CrocBackend::default()),
            store.clone(),
            SettingsStore::new(
                SettingsLoader::with_path(root.join("config.json")),
                DriftSettings::default(),
            ),
            output_directory,
            AppCommand::RecoverTransfer {
                transfer_id,
                code: Some("transfer-code".into()),
                output_directory: None,
            },
        ));

        assert!(matches!(
            result,
            Err(AppCommandError::Transfer(TransferError::Backend(message)))
                if message == "resume backend is incompatible"
        ));
        assert_eq!(
            runtime.block_on(store.load_resume(transfer_id)).unwrap(),
            Some(state)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn receive_dispatch_rejects_unavailable_destination_before_backend() {
        let root = temp_path();
        let config_path = root.join("config").join("config.json");
        let receive_directory = root.join("received");
        fs::create_dir_all(&receive_directory).unwrap();
        let mut settings = DriftSettings::default();
        settings.transfer.default_receive_directory = receive_directory;
        SettingsLoader::with_path(&config_path)
            .save(&settings)
            .unwrap();
        let state = AppState::bootstrap_with_config_path(&config_path).unwrap();

        let result = state
            .runtime
            .block_on(state.handle().dispatch(AppCommand::Receive {
                code: "transfer-code".into(),
                output_directory: Some(root.join("missing").join("nested")),
            }))
            .unwrap();

        assert!(matches!(
            result,
            Err(AppCommandError::OutputDirectoryUnavailable)
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn receive_retry_revalidates_destination_before_starting_new_attempt() {
        let root = temp_path();
        let receive_directory = root.join("received");
        fs::create_dir_all(&receive_directory).unwrap();
        let script = write_script(
            "if [ \"$1\" = \"--version\" ]; then printf 'v11.2.2-build\\n'; exit 0; fi\nsleep 1",
        );
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let resume_store = JsonStore::new(root.join("state"));
        let manager = TransferManager::new(
            CrocBackend::new(&script).with_timeout(std::time::Duration::from_millis(50)),
        );
        let mut events = manager.subscribe();

        let result = runtime.block_on(async {
            let transfer_id = dispatch_command(
                manager.clone(),
                resume_store.clone(),
                SettingsStore::new(
                    SettingsLoader::with_path(root.join("config.json")),
                    DriftSettings::default(),
                ),
                receive_directory.clone(),
                AppCommand::Receive {
                    code: "transfer-code".into(),
                    output_directory: Some(receive_directory.clone()),
                },
            )
            .await
            .unwrap();
            loop {
                let notification =
                    tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                        .await
                        .unwrap()
                        .unwrap();
                if notification.transfer_id == transfer_id
                    && notification.event == drift_core::TransferEvent::Failed
                {
                    break transfer_id;
                }
            }
        });
        fs::remove_dir_all(&root).unwrap();

        let retry_result = runtime.block_on(dispatch_command(
            manager,
            resume_store,
            SettingsStore::new(
                SettingsLoader::with_path(root.join("config.json")),
                DriftSettings::default(),
            ),
            root.clone(),
            AppCommand::RetryTransfer {
                transfer_id: result,
            },
        ));
        assert!(matches!(
            retry_result,
            Err(AppCommandError::OutputDirectoryUnavailable)
        ));

        let _ = fs::remove_file(script);
    }
}
