use serde::{Deserialize, Serialize};
use std::{
    env, fmt, fs, io,
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DriftSettings {
    pub relay: RelaySettings,
    pub transfer: TransferSettings,
    pub ui: UiSettings,
    pub privacy: PrivacySettings,
}

impl Default for DriftSettings {
    fn default() -> Self {
        Self {
            relay: RelaySettings::default(),
            transfer: TransferSettings::default(),
            ui: UiSettings::default(),
            privacy: PrivacySettings::default(),
        }
    }
}

impl fmt::Debug for DriftSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DriftSettings")
            .field("relay_configured", &self.relay.url.is_some())
            .field("transfer", &self.transfer)
            .field("ui", &self.ui)
            .field("privacy", &self.privacy)
            .finish()
    }
}

impl DriftSettings {
    pub fn validate(&self) -> Result<(), SettingsValidationError> {
        if self.transfer.croc_executable.as_os_str().is_empty() {
            return Err(SettingsValidationError::EmptyCrocExecutable);
        }
        if let Some(url) = self.relay.url.as_deref() {
            if !is_valid_relay_url(url) {
                return Err(SettingsValidationError::InvalidRelayUrl);
            }
        }
        if self.transfer.timeout.is_zero() {
            return Err(SettingsValidationError::NonPositiveTimeout);
        }
        validate_receive_directory(&self.transfer.default_receive_directory)?;
        if !self.privacy.redact_sensitive_values {
            return Err(SettingsValidationError::PrivacyRedactionDisabled);
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RelaySettings {
    pub url: Option<String>,
}

impl fmt::Debug for RelaySettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelaySettings")
            .field("url_configured", &self.url.is_some())
            .finish()
    }
}

impl Default for RelaySettings {
    fn default() -> Self {
        Self { url: None }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TransferSettings {
    pub croc_executable: PathBuf,
    pub default_receive_directory: PathBuf,
    #[serde(with = "duration_seconds")]
    pub timeout: Duration,
}

impl Default for TransferSettings {
    fn default() -> Self {
        Self {
            croc_executable: PathBuf::from("croc"),
            default_receive_directory: default_receive_directory(),
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl fmt::Debug for TransferSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransferSettings")
            .field(
                "croc_executable_configured",
                &!self.croc_executable.as_os_str().is_empty(),
            )
            .field(
                "default_receive_directory_configured",
                &!self.default_receive_directory.as_os_str().is_empty(),
            )
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UiSettings {
    pub show_notifications: bool,
}

impl Default for UiSettings {
    fn default() -> Self {
        Self {
            show_notifications: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PrivacySettings {
    pub redact_sensitive_values: bool,
}

impl Default for PrivacySettings {
    fn default() -> Self {
        Self {
            redact_sensitive_values: true,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SettingsValidationError {
    #[error("croc executable path must not be empty")]
    EmptyCrocExecutable,
    #[error("relay URL must use http or https and include a host")]
    InvalidRelayUrl,
    #[error("transfer timeout must be positive")]
    NonPositiveTimeout,
    #[error("default receive directory must not be empty")]
    EmptyReceiveDirectory,
    #[error("default receive directory is not usable")]
    UnusableReceiveDirectory,
    #[error("privacy settings must redact sensitive values")]
    PrivacyRedactionDisabled,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConfigPathError {
    #[error("home directory is unavailable")]
    HomeDirectoryUnavailable,
    #[error("configuration directory is unavailable")]
    ConfigDirectoryUnavailable,
}

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("configuration file could not be read")]
    Read(#[source] io::Error),
    #[error("configuration file contains invalid JSON")]
    Parse(#[source] serde_json::Error),
    #[error("configuration values are invalid")]
    Validation(#[from] SettingsValidationError),
    #[error("configuration path must name a file")]
    InvalidPath,
    #[error("configuration file could not be serialized")]
    Serialization(#[source] serde_json::Error),
    #[error("configuration file could not be written")]
    Write(#[source] io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsSource {
    Defaults,
    File,
}

impl SettingsSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Defaults => "defaults",
            Self::File => "file",
        }
    }
}

#[derive(Debug)]
pub struct LoadedSettings {
    pub settings: DriftSettings,
    pub source: SettingsSource,
}

#[derive(Debug, Clone)]
pub struct SettingsLoader {
    path: PathBuf,
}

impl SettingsLoader {
    pub fn new() -> Result<Self, ConfigPathError> {
        Ok(Self::with_path(default_config_path()?))
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn test_override(path: impl Into<PathBuf>) -> Self {
        Self::with_path(path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<LoadedSettings, SettingsError> {
        let data = match fs::read(&self.path) {
            Ok(data) => data,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let settings = DriftSettings::default();
                settings.validate()?;
                return Ok(LoadedSettings {
                    settings,
                    source: SettingsSource::Defaults,
                });
            }
            Err(error) => return Err(SettingsError::Read(error)),
        };
        let settings: DriftSettings =
            serde_json::from_slice(&data).map_err(SettingsError::Parse)?;
        settings.validate()?;
        Ok(LoadedSettings {
            settings,
            source: SettingsSource::File,
        })
    }

    pub fn save(&self, settings: &DriftSettings) -> Result<(), SettingsError> {
        if self.path.file_name().is_none() {
            return Err(SettingsError::InvalidPath);
        }
        settings.validate()?;
        let data = serde_json::to_vec_pretty(settings).map_err(SettingsError::Serialization)?;
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(SettingsError::Write)?;

        let temporary_path = self.path.with_extension("json.tmp");
        fs::write(&temporary_path, data).map_err(SettingsError::Write)?;
        if let Err(error) = fs::rename(&temporary_path, &self.path) {
            let _ = fs::remove_file(&temporary_path);
            return Err(SettingsError::Write(error));
        }
        Ok(())
    }
}

pub fn default_config_path() -> Result<PathBuf, ConfigPathError> {
    #[cfg(target_os = "macos")]
    {
        return Ok(home_dir()?
            .join("Library")
            .join("Application Support")
            .join("Drift")
            .join("config.json"));
    }

    #[cfg(target_os = "windows")]
    {
        let root = env::var_os("APPDATA")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| home_dir().ok())
            .ok_or(ConfigPathError::ConfigDirectoryUnavailable)?;
        return Ok(root.join("Drift").join("config.json"));
    }

    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let root = env::var_os("XDG_CONFIG_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| home_dir().ok().map(|home| home.join(".config")))
            .ok_or(ConfigPathError::ConfigDirectoryUnavailable)?;
        Ok(root.join("drift").join("config.json"))
    }
}

fn default_receive_directory() -> PathBuf {
    home_dir()
        .map(|home| home.join("Downloads"))
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn home_dir() -> Result<PathBuf, ConfigPathError> {
    env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("USERPROFILE")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .ok_or(ConfigPathError::HomeDirectoryUnavailable)
}

fn validate_receive_directory(path: &Path) -> Result<(), SettingsValidationError> {
    if path.as_os_str().is_empty() {
        return Err(SettingsValidationError::EmptyReceiveDirectory);
    }
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(SettingsValidationError::UnusableReceiveDirectory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            match fs::metadata(parent) {
                Ok(metadata) if metadata.is_dir() => Ok(()),
                _ => Err(SettingsValidationError::UnusableReceiveDirectory),
            }
        }
        Err(_) => Err(SettingsValidationError::UnusableReceiveDirectory),
    }
}

fn is_valid_relay_url(value: &str) -> bool {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_whitespace) {
        return false;
    }
    let Some((scheme, remainder)) = value.split_once("://") else {
        return false;
    };
    if !matches!(scheme, "http" | "https") {
        return false;
    }
    let authority = remainder.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') {
        return false;
    }
    if authority.starts_with('[') {
        let Some(end) = authority.find(']') else {
            return false;
        };
        if end == 1 {
            return false;
        }
        return authority[end + 1..]
            .strip_prefix(':')
            .map_or(true, valid_port);
    }

    let mut parts = authority.split(':');
    let host = parts.next().unwrap_or_default();
    if host.is_empty() {
        return false;
    }
    match parts.next() {
        Some(port) if !valid_port(port) => false,
        Some(_) if parts.next().is_some() => false,
        _ => true,
    }
}

fn valid_port(port: &str) -> bool {
    port.parse::<u16>().is_ok_and(|port| port > 0)
}

mod duration_seconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Duration::from_secs(u64::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use uuid::Uuid;

    fn temp_directory() -> PathBuf {
        let path = env::temp_dir().join(format!("drift-app-settings-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn missing_file_uses_defaults() {
        let directory = temp_directory();
        let loaded = SettingsLoader::test_override(directory.join("config.json"))
            .load()
            .unwrap();
        assert_eq!(loaded.settings, DriftSettings::default());
        assert_eq!(loaded.source, SettingsSource::Defaults);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn valid_settings_round_trip_through_override_path() {
        let directory = temp_directory();
        let path = directory.join("nested").join("config.json");
        let mut settings = DriftSettings::default();
        settings.relay.url = Some("https://relay.example.test:443".into());
        settings.transfer.croc_executable = PathBuf::from("/opt/bin/croc");
        settings.transfer.default_receive_directory = directory.join("received");
        settings.transfer.timeout = Duration::from_secs(90);

        let loader = SettingsLoader::test_override(&path);
        loader.save(&settings).unwrap();
        let loaded = loader.load().unwrap();

        assert_eq!(loaded.settings, settings);
        assert_eq!(loaded.source, SettingsSource::File);
        assert!(!path.with_extension("json.tmp").exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_empty_executable_path() {
        let mut settings = DriftSettings::default();
        settings.transfer.croc_executable = PathBuf::new();
        assert_eq!(
            settings.validate(),
            Err(SettingsValidationError::EmptyCrocExecutable)
        );
    }

    #[test]
    fn rejects_invalid_relay_url() {
        let mut settings = DriftSettings::default();
        settings.relay.url = Some("ftp://relay.example.test".into());
        assert_eq!(
            settings.validate(),
            Err(SettingsValidationError::InvalidRelayUrl)
        );
    }

    #[test]
    fn rejects_non_positive_timeout() {
        let mut settings = DriftSettings::default();
        settings.transfer.timeout = Duration::ZERO;
        assert_eq!(
            settings.validate(),
            Err(SettingsValidationError::NonPositiveTimeout)
        );
    }

    #[test]
    fn rejects_unusable_receive_directory() {
        let mut settings = DriftSettings::default();
        let directory = temp_directory();
        settings.transfer.default_receive_directory = directory.join("missing").join("nested");
        assert_eq!(
            settings.validate(),
            Err(SettingsValidationError::UnusableReceiveDirectory)
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_disabled_sensitive_value_redaction() {
        let mut settings = DriftSettings::default();
        settings.privacy.redact_sensitive_values = false;
        assert_eq!(
            settings.validate(),
            Err(SettingsValidationError::PrivacyRedactionDisabled)
        );
    }

    #[test]
    fn debug_redacts_relay_value() {
        let mut settings = DriftSettings::default();
        settings.relay.url = Some("https://secret-relay.example.test".into());
        let debug = format!("{settings:?}");
        assert!(!debug.contains("secret-relay.example.test"));
        assert!(debug.contains("relay_configured: true"));

        let relay_debug = format!("{:?}", settings.relay);
        assert!(!relay_debug.contains("secret-relay.example.test"));
    }

    #[test]
    fn invalid_json_returns_parse_error() {
        let directory = temp_directory();
        let path = directory.join("config.json");
        fs::write(&path, b"not-json").unwrap();
        assert!(matches!(
            SettingsLoader::with_path(&path).load(),
            Err(SettingsError::Parse(_))
        ));
        fs::remove_dir_all(directory).unwrap();
    }
}
