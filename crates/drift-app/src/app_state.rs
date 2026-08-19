use crate::settings::{
    ConfigPathError, DriftSettings, SettingsError, SettingsLoader, SettingsSource,
};
use drift_core::TransferId;
use drift_protocol::{BackendError, CrocBackend, ReceiveRequest, SendRequest};
use drift_storage::{validate_receive_directory, DestinationError, JsonStore};
use drift_transfer::{TransferManager, TransferNotification};
use std::{
    fmt,
    path::{Path, PathBuf},
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
    resume_store: JsonStore,
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
        self.settings.relay.url.is_some()
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
            default_receive_directory: self.settings.transfer.default_receive_directory.clone(),
        }
    }

    fn from_loader(loader: SettingsLoader) -> Result<Self, AppError> {
        let loaded = loader.load().map_err(AppError::Settings)?;
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(AppError::Runtime)?;
        let mut backend = CrocBackend::new(&loaded.settings.transfer.croc_executable)
            .with_timeout(loaded.settings.transfer.timeout);
        if let Some(relay) = loaded.settings.relay.url.clone() {
            backend = backend.with_relay(relay);
        }
        let transfer_manager = TransferManager::with_backend_name(backend.clone(), "croc");
        let resume_store = JsonStore::new(resume_root(loader.path()));

        Ok(Self {
            settings: loaded.settings,
            settings_source: loaded.source,
            config_path: loader.path().to_path_buf(),
            backend,
            transfer_manager,
            resume_store,
            runtime,
        })
    }
}

#[derive(Clone)]
pub struct AppHandle {
    runtime: Handle,
    backend: CrocBackend,
    transfer_manager: TransferManager<CrocBackend>,
    default_receive_directory: PathBuf,
}

impl AppHandle {
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

    pub fn validate_destination(
        &self,
        path: PathBuf,
    ) -> JoinHandle<Result<(), AppCommandError>> {
        self.runtime.spawn(async move {
            validate_receive_directory(path)
                .await
                .map_err(AppCommandError::from)
        })
    }

    pub fn dispatch(&self, command: AppCommand) -> JoinHandle<Result<TransferId, AppCommandError>> {
        let transfer_manager = self.transfer_manager.clone();
        let default_receive_directory = self.default_receive_directory.clone();
        self.runtime.spawn(async move {
            dispatch_command(transfer_manager, default_receive_directory, command).await
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TransferNotification> {
        self.transfer_manager.subscribe()
    }

    pub async fn session(&self, transfer_id: TransferId) -> Option<drift_core::TransferSession> {
        self.transfer_manager.session(transfer_id).await
    }
}

pub enum AppCommand {
    Send {
        paths: Vec<PathBuf>,
    },
    Receive {
        code: String,
        output_directory: Option<PathBuf>,
    },
    Cancel {
        transfer_id: TransferId,
    },
}

impl fmt::Debug for AppCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Send { paths } => formatter
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
    #[error("transfer code must not be empty")]
    EmptyTransferCode,
    #[error("transfer command failed")]
    Transfer(#[source] drift_core::TransferError),
}

impl AppCommandError {
    pub fn user_message(&self) -> &'static str {
        match self {
            Self::Preflight(_) => "Croc is not ready.",
            Self::InvalidRequest(_) => "The transfer request is invalid.",
            Self::EmptyOutputDirectory => "The receive folder is unavailable.",
            Self::OutputDirectoryUnavailable => "The receive folder is unavailable.",
            Self::OutputDirectoryNotWritable => "The receive folder is not writable.",
            Self::EmptyTransferCode => "Enter a transfer code.",
            Self::Transfer(_) => "The transfer could not start.",
        }
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
        }
    }
}

async fn dispatch_command(
    transfer_manager: TransferManager<CrocBackend>,
    default_receive_directory: PathBuf,
    command: AppCommand,
) -> Result<TransferId, AppCommandError> {
    match command {
        AppCommand::Send { paths } => {
            let request = SendRequest::new(paths).map_err(AppCommandError::InvalidRequest)?;
            transfer_manager
                .start_send(request)
                .await
                .map_err(AppCommandError::Transfer)
        }
        AppCommand::Receive {
            code,
            output_directory,
        } => {
            let output_directory = output_directory.unwrap_or(default_receive_directory);
            if code.trim().is_empty() {
                return Err(AppCommandError::EmptyTransferCode);
            }
            validate_receive_directory(&output_directory)
                .await
                .map_err(AppCommandError::from)?;
            let request = ReceiveRequest::new(code, output_directory)
                .map_err(AppCommandError::InvalidRequest)?;
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
    }
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

        let result = state.runtime.block_on(state.handle().dispatch(AppCommand::Receive {
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
}
