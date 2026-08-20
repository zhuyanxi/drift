use crate::{
    BackendCapability, BackendError, BackendEvent, ReceiveRequest, SendRequest, TransferBackend,
};
use async_trait::async_trait;
use std::{
    ffi::OsString,
    fmt,
    future::Future,
    path::{Path, PathBuf},
    process::ExitStatus,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
    time,
};
use tracing::debug;

pub const SUPPORTED_CROC_VERSION_RANGE: &str = "11.2.x";

const DIAGNOSTIC_LIMIT: usize = 64 * 1024;
const LINE_LIMIT: usize = 16 * 1024;
const VERSION_OUTPUT_LIMIT: usize = 8 * 1024;
const PREFLIGHT_LIMIT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrocVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl CrocVersion {
    fn is_supported(self) -> bool {
        self.major == 11 && self.minor == 2
    }
}

impl fmt::Display for CrocVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrocParseError {
    InvalidTransferCode,
}

#[derive(Clone)]
pub struct CrocBackend {
    executable: PathBuf,
    relay: Option<String>,
    timeout: Duration,
}

impl fmt::Debug for CrocBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrocBackend")
            .field("executable", &self.executable)
            .field("relay_configured", &self.relay.is_some())
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl Default for CrocBackend {
    fn default() -> Self {
        Self::new("croc")
    }
}

impl CrocBackend {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            relay: None,
            timeout: Duration::from_secs(30 * 60),
        }
    }

    pub fn with_relay(mut self, relay: impl Into<String>) -> Self {
        self.relay = Some(relay.into());
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn executable(&self) -> &PathBuf {
        &self.executable
    }

    pub async fn preflight(&self) -> Result<CrocVersion, BackendError> {
        let mut command = Command::new(&self.executable);
        command
            .arg("--version")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| map_spawn_error(&self.executable, error))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(BackendError::MissingPipe { stream: "stdout" })?;
        let stderr = child
            .stderr
            .take()
            .ok_or(BackendError::MissingPipe { stream: "stderr" })?;
        let stdout_task = tokio::spawn(read_bounded(stdout, VERSION_OUTPUT_LIMIT));
        let stderr_task = tokio::spawn(read_bounded(stderr, VERSION_OUTPUT_LIMIT));

        let status = match time::timeout(self.preflight_timeout(), child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                terminate_child(&mut child).await;
                let _ = join_bounded(stdout_task).await;
                let _ = join_bounded(stderr_task).await;
                return Err(BackendError::Io(error));
            }
            Err(_) => {
                terminate_child(&mut child).await;
                let _ = join_bounded(stdout_task).await;
                let _ = join_bounded(stderr_task).await;
                return Err(BackendError::Timeout {
                    timeout: self.preflight_timeout(),
                });
            }
        };
        let stdout = join_bounded(stdout_task).await?;
        let stderr = join_bounded(stderr_task).await?;
        if stdout.truncated || stderr.truncated {
            return Err(BackendError::OutputLimit { stream: "version" });
        }
        if !status.success() {
            return Err(BackendError::VersionInvocation);
        }

        let mut version_output = stdout.bytes;
        version_output.extend_from_slice(&stderr.bytes);
        let version =
            parse_croc_version(&version_output).ok_or(BackendError::InvalidVersionOutput)?;
        if !version.is_supported() {
            return Err(BackendError::UnsupportedVersion {
                found: version.to_string(),
                supported: SUPPORTED_CROC_VERSION_RANGE,
            });
        }
        Ok(version)
    }

    fn send_args(&self, request: &SendRequest) -> Result<Vec<OsString>, BackendError> {
        if request.paths.is_empty() {
            return Err(BackendError::InvalidRequest(
                "send request must contain at least one path".into(),
            ));
        }
        let mut args = self.relay_args(request.relay.as_deref());
        args.push(OsString::from("--disable-clipboard"));
        args.push(OsString::from("send"));
        args.extend(request.paths.iter().map(|path| path.as_os_str().to_owned()));
        Ok(args)
    }

    fn receive_args(&self, request: &ReceiveRequest) -> Result<Vec<OsString>, BackendError> {
        if request.code.trim().is_empty() {
            return Err(BackendError::InvalidRequest(
                "receive request code must not be empty".into(),
            ));
        }
        let mut args = self.relay_args(request.relay.as_deref());
        args.push(OsString::from("--yes"));
        args.push(OsString::from("--disable-clipboard"));
        args.push(OsString::from("--out"));
        args.push(request.output_directory.as_os_str().to_owned());
        Ok(args)
    }

    fn relay_args(&self, request_relay: Option<&str>) -> Vec<OsString> {
        let relay = request_relay.or(self.relay.as_deref());
        relay
            .map(|relay| vec![OsString::from("--relay"), OsString::from(relay)])
            .unwrap_or_default()
    }

    async fn spawn(
        &self,
        args: Vec<OsString>,
        role: &'static str,
        secret: Option<&str>,
        requires_code: bool,
    ) -> Result<TransferHandle, BackendError> {
        debug!(role, argument_count = args.len(), "starting croc backend");
        let mut command = Command::new(&self.executable);
        command
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        command.env_remove("CROC_SECRET");
        if let Some(secret) = secret {
            command.env("CROC_SECRET", secret);
        }
        let mut child = command
            .spawn()
            .map_err(|error| map_spawn_error(&self.executable, error))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(BackendError::MissingPipe { stream: "stdout" })?;
        let stderr = child
            .stderr
            .take()
            .ok_or(BackendError::MissingPipe { stream: "stderr" })?;
        let (updates_sender, updates) = mpsc::channel(32);
        let _ = updates_sender.try_send(BackendEvent::CapabilityUnavailable {
            capability: BackendCapability::Progress,
        });
        Ok(TransferHandle {
            child,
            stdout_task: Some(read_output(stdout, "stdout", updates_sender.clone())),
            stderr_task: Some(read_output(stderr, "stderr", updates_sender)),
            updates: Some(updates),
            requires_code,
            redaction_secret: secret.map(str::to_owned),
            timeout: self.timeout,
        })
    }

    fn preflight_timeout(&self) -> Duration {
        PREFLIGHT_LIMIT
    }
}

pub fn parse_croc_line(line: &str) -> Result<Option<BackendEvent>, CrocParseError> {
    let normalized = strip_ansi(line);
    let line = normalized.trim_matches(['\r', '\n']).trim();
    let Some(value) = line.strip_prefix("Code is:") else {
        return Ok(None);
    };
    let code = value.trim();
    if !matches!(value.chars().next(), Some(character) if character.is_whitespace())
        || code.is_empty()
        || code.len() > 256
        || code
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(CrocParseError::InvalidTransferCode);
    }
    Ok(Some(BackendEvent::CodeGenerated {
        code: code.to_owned(),
    }))
}

pub fn parse_croc_version(output: &[u8]) -> Option<CrocVersion> {
    output.split(|byte| *byte == b'\n').find_map(|line| {
        let line = String::from_utf8_lossy(line).trim().to_owned();
        let token = line
            .strip_prefix("croc version ")
            .or_else(|| line.strip_prefix("croc "))
            .unwrap_or(&line);
        let token = token.strip_prefix('v').unwrap_or(token);
        let token = token.split('-').next()?;
        let mut parts = token.split('.');
        let version = CrocVersion {
            major: parts.next()?.parse().ok()?,
            minor: parts.next()?.parse().ok()?,
            patch: parts.next()?.parse().ok()?,
        };
        (parts.next().is_none()).then_some(version)
    })
}

fn strip_ansi(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\u{1b}' {
            output.push(character);
            continue;
        }
        if characters.next() != Some('[') {
            continue;
        }
        for character in characters.by_ref() {
            if character.is_ascii_alphabetic() {
                break;
            }
        }
    }
    output
}

fn redact_output(output: &[u8], code: Option<&str>, truncated: bool) -> Vec<u8> {
    let text = String::from_utf8_lossy(output);
    let mut redacted = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let normalized = strip_ansi(line);
        let trimmed = normalized.trim_start();
        if trimmed.starts_with("Code is:") {
            redacted.push_str("Code is: [REDACTED]");
            if line.ends_with('\n') {
                redacted.push('\n');
            }
        } else if trimmed.starts_with("CROC_SECRET=") {
            redacted.push_str("CROC_SECRET=[REDACTED]");
            if line.ends_with('\n') {
                redacted.push('\n');
            }
        } else if let Some(code) = code {
            redacted.push_str(&normalized.replace(code, "[REDACTED]"));
        } else {
            redacted.push_str(&normalized);
        }
    }
    if truncated {
        redacted.push_str("\n[output truncated]");
    }
    redacted.into_bytes()
}

async fn read_bounded<R>(mut reader: R, limit: usize) -> Result<BoundedOutput, BackendError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(limit.min(4096));
    let mut truncated = false;
    let mut buffer = [0u8; 4096];
    loop {
        let read = reader.read(&mut buffer).await.map_err(BackendError::Io)?;
        if read == 0 {
            break;
        }
        let previous_len = bytes.len();
        if previous_len < limit {
            let remaining = limit - previous_len;
            bytes.extend_from_slice(&buffer[..read.min(remaining)]);
        }
        if read > limit.saturating_sub(previous_len) {
            truncated = true;
        }
    }
    Ok(BoundedOutput { bytes, truncated })
}

async fn join_bounded(
    task: JoinHandle<Result<BoundedOutput, BackendError>>,
) -> Result<BoundedOutput, BackendError> {
    task.await.map_err(BackendError::OutputTask)?
}

fn map_spawn_error(executable: &Path, error: std::io::Error) -> BackendError {
    if error.kind() == std::io::ErrorKind::NotFound {
        BackendError::ExecutableMissing {
            executable: executable.to_owned(),
        }
    } else {
        BackendError::Spawn(error)
    }
}

async fn terminate_child(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[async_trait]
impl TransferBackend for CrocBackend {
    async fn send(&self, request: SendRequest) -> Result<TransferHandle, BackendError> {
        let args = self.send_args(&request)?;
        self.preflight().await?;
        self.spawn(args, "send", None, true).await
    }

    async fn receive(&self, request: ReceiveRequest) -> Result<TransferHandle, BackendError> {
        let args = self.receive_args(&request)?;
        self.preflight().await?;
        self.spawn(args, "receive", Some(&request.code), false)
            .await
    }
}

pub struct TransferHandle {
    child: Child,
    stdout_task: Option<JoinHandle<Result<OutputCapture, BackendError>>>,
    stderr_task: Option<JoinHandle<Result<OutputCapture, BackendError>>>,
    updates: Option<mpsc::Receiver<BackendEvent>>,
    requires_code: bool,
    redaction_secret: Option<String>,
    timeout: Duration,
}

impl TransferHandle {
    pub fn take_updates(&mut self) -> Option<mpsc::Receiver<BackendEvent>> {
        self.updates.take()
    }

    pub async fn wait(mut self) -> Result<TransferOutput, BackendError> {
        let status = self.wait_for_exit().await?;
        self.finish(status).await
    }

    pub async fn wait_with_cancel(
        self,
        cancellation: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<TransferOutput, BackendError> {
        self.wait_with_cancel_signal(async move {
            let _ = cancellation.await;
        })
        .await
    }

    pub async fn wait_with_cancel_signal<F>(
        mut self,
        cancellation: F,
    ) -> Result<TransferOutput, BackendError>
    where
        F: Future<Output = ()> + Send,
    {
        let mut cancellation = Box::pin(cancellation);
        let status = tokio::select! {
            result = time::timeout(self.timeout, self.child.wait()) => match result {
                Ok(Ok(status)) => status,
                Ok(Err(error)) => {
                    self.abort_output().await;
                    return Err(BackendError::Io(error));
                }
                Err(_) => {
                    self.abort_output().await;
                    return Err(BackendError::Timeout { timeout: self.timeout });
                }
            },
            _ = &mut cancellation => {
                self.abort_output().await;
                return Err(BackendError::Cancelled);
            }
        };
        self.finish(status).await
    }

    async fn wait_for_exit(&mut self) -> Result<ExitStatus, BackendError> {
        match time::timeout(self.timeout, self.child.wait()).await {
            Ok(Ok(status)) => Ok(status),
            Ok(Err(error)) => {
                self.abort_output().await;
                Err(BackendError::Io(error))
            }
            Err(_) => {
                self.abort_output().await;
                Err(BackendError::Timeout {
                    timeout: self.timeout,
                })
            }
        }
    }

    async fn finish(&mut self, status: ExitStatus) -> Result<TransferOutput, BackendError> {
        let output = self.collect_output().await?;
        if !status.success() {
            return Err(BackendError::ProcessFailed {
                code: status.code(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        if self.requires_code && output.code.is_none() {
            return Err(BackendError::MissingSignal {
                signal: "transfer code",
            });
        }
        Ok(TransferOutput {
            status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }

    pub async fn cancel(&mut self) -> Result<(), BackendError> {
        self.terminate().await;
        let _ = self.collect_output().await?;
        Ok(())
    }

    async fn terminate(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }

    async fn collect_output(&mut self) -> Result<CollectedOutput, BackendError> {
        let stdout = join_output(self.stdout_task.take()).await?;
        let stderr = join_output(self.stderr_task.take()).await?;
        let code = match (stdout.code.as_deref(), stderr.code.as_deref()) {
            (Some(stdout_code), Some(stderr_code)) if stdout_code != stderr_code => {
                return Err(BackendError::OutputParse {
                    stream: "combined",
                    reason: "multiple transfer code signals",
                });
            }
            (Some(code), _) | (_, Some(code)) => Some(code.to_owned()),
            (None, None) => None,
        };
        Ok(CollectedOutput {
            stdout: redact_output(
                &stdout.bytes,
                code.as_deref().or(self.redaction_secret.as_deref()),
                stdout.truncated,
            ),
            stderr: redact_output(
                &stderr.bytes,
                code.as_deref().or(self.redaction_secret.as_deref()),
                stderr.truncated,
            ),
            code,
        })
    }

    async fn abort_output(&mut self) {
        self.terminate().await;
        let _ = self.collect_output().await;
    }
}

#[derive(Debug)]
pub struct TransferOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

struct CollectedOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    code: Option<String>,
}

struct OutputCapture {
    bytes: Vec<u8>,
    code: Option<String>,
    truncated: bool,
}

struct BoundedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

fn read_output<R>(
    mut reader: R,
    stream: &'static str,
    updates: mpsc::Sender<BackendEvent>,
) -> JoinHandle<Result<OutputCapture, BackendError>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::with_capacity(DIAGNOSTIC_LIMIT.min(4096));
        let mut line = Vec::with_capacity(LINE_LIMIT.min(256));
        let mut code = None;
        let mut line_too_long = false;
        let mut truncated = false;
        let mut buffer = [0u8; 4096];

        loop {
            let read = reader.read(&mut buffer).await.map_err(BackendError::Io)?;
            if read == 0 {
                break;
            }
            let previous_len = output.len();
            if output.len() < DIAGNOSTIC_LIMIT {
                let remaining = DIAGNOSTIC_LIMIT - output.len();
                output.extend_from_slice(&buffer[..read.min(remaining)]);
            }
            if read > DIAGNOSTIC_LIMIT.saturating_sub(previous_len) {
                truncated = true;
            }

            for byte in &buffer[..read] {
                if *byte == b'\n' {
                    if line_too_long && line.starts_with(b"Code is:") {
                        return Err(BackendError::OutputParse {
                            stream,
                            reason: "invalid transfer code signal",
                        });
                    }
                    if !line_too_long {
                        observe_line(&line, stream, &mut code, &updates)?;
                    }
                    line.clear();
                    line_too_long = false;
                } else if !line_too_long {
                    if line.len() == LINE_LIMIT {
                        line_too_long = true;
                    } else {
                        line.push(*byte);
                    }
                }
            }
        }

        if !line.is_empty() && !line_too_long {
            observe_line(&line, stream, &mut code, &updates)?;
        }
        Ok(OutputCapture {
            bytes: output,
            code,
            truncated,
        })
    })
}

async fn join_output(
    task: Option<JoinHandle<Result<OutputCapture, BackendError>>>,
) -> Result<OutputCapture, BackendError> {
    let task = task.ok_or(BackendError::MissingPipe { stream: "output" })?;
    task.await.map_err(BackendError::OutputTask)?
}

fn observe_line(
    line: &[u8],
    stream: &'static str,
    code: &mut Option<String>,
    updates: &mpsc::Sender<BackendEvent>,
) -> Result<(), BackendError> {
    let Some(event) = parse_croc_line(&String::from_utf8_lossy(line)).map_err(|error| {
        BackendError::OutputParse {
            stream,
            reason: match error {
                CrocParseError::InvalidTransferCode => "invalid transfer code signal",
            },
        }
    })?
    else {
        return Ok(());
    };

    if let BackendEvent::CodeGenerated { code: value } = &event {
        if let Some(previous) = code {
            if previous != value {
                return Err(BackendError::OutputParse {
                    stream,
                    reason: "multiple transfer code signals",
                });
            }
            return Ok(());
        }
        *code = Some(value.clone());
    }
    let _ = updates.try_send(event);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[cfg(unix)]
    fn write_script(body: &str) -> PathBuf {
        use std::{fs, os::unix::fs::PermissionsExt};

        let path = std::env::temp_dir().join(format!("drift-croc-test-{}", uuid::Uuid::new_v4()));
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&path, permissions).unwrap();
        path
    }

    #[test]
    fn builds_sender_invocation_with_relay_before_subcommand() {
        let backend = CrocBackend::new("croc").with_relay("relay.example");
        let request = SendRequest::new(vec![PathBuf::from("one.txt")]).unwrap();
        assert_eq!(
            backend.send_args(&request).unwrap(),
            vec![
                "--relay",
                "relay.example",
                "--disable-clipboard",
                "send",
                "one.txt"
            ]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn builds_receiver_invocation_without_logging_code() {
        let backend = CrocBackend::new("croc");
        let request = ReceiveRequest::new("secret-code", "/tmp/out").unwrap();
        let debug = format!("{request:?}");
        assert!(!debug.contains("secret-code"));
        assert_eq!(
            backend.receive_args(&request).unwrap(),
            vec!["--yes", "--disable-clipboard", "--out", "/tmp/out"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn parses_only_the_documented_code_line() {
        assert_eq!(
            parse_croc_line("Code is: alpha-bravo").unwrap(),
            Some(BackendEvent::CodeGenerated {
                code: "alpha-bravo".into()
            })
        );
        assert_eq!(parse_croc_line("Sending file (1 MB)").unwrap(), None);
        assert!(matches!(
            parse_croc_line("Code is: alpha bravo"),
            Err(CrocParseError::InvalidTransferCode)
        ));
        assert!(matches!(
            parse_croc_line("Code is:alpha-bravo"),
            Err(CrocParseError::InvalidTransferCode)
        ));
        assert!(!format!(
            "{:?}",
            BackendEvent::CodeGenerated {
                code: "alpha-bravo".into()
            }
        )
        .contains("alpha-bravo"));
    }

    #[test]
    fn parses_supported_and_unsupported_versions() {
        assert_eq!(
            parse_croc_version(b"croc version v11.2.2-build\n"),
            Some(CrocVersion {
                major: 11,
                minor: 2,
                patch: 2,
            })
        );
        assert_eq!(
            parse_croc_version(b"v11.1.9\n"),
            Some(CrocVersion {
                major: 11,
                minor: 1,
                patch: 9,
            })
        );
        assert_eq!(parse_croc_version(b"not croc\n"), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn preflight_classifies_missing_and_unsupported_versions() {
        let missing = CrocBackend::new(
            std::env::temp_dir().join(format!("drift-missing-croc-{}", uuid::Uuid::new_v4())),
        );
        assert!(matches!(
            missing.preflight().await,
            Err(BackendError::ExecutableMissing { .. })
        ));

        let script = write_script(
            "if [ \"$1\" = \"--version\" ]; then printf 'v11.1.9\\n'; exit 0; fi; exit 0",
        );
        let backend = CrocBackend::new(&script);
        assert!(matches!(
            backend.preflight().await,
            Err(BackendError::UnsupportedVersion { found, .. }) if found == "11.1.9"
        ));
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn preflight_rejects_version_invocation_failure() {
        let script = write_script(
            "if [ \"$1\" = \"--version\" ]; then printf 'broken\\n'; exit 9; fi; exit 0",
        );
        let backend = CrocBackend::new(&script);
        assert!(matches!(
            backend.preflight().await,
            Err(BackendError::VersionInvocation)
        ));
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    fn versioned_script(body: &str) -> PathBuf {
        write_script(&format!(
            "if [ \"$1\" = \"--version\" ]; then printf 'v11.2.2-build\\n'; exit 0; fi\n{body}"
        ))
    }

    #[test]
    fn validates_empty_requests() {
        assert!(SendRequest::new(Vec::new()).is_err());
        assert!(ReceiveRequest::new("", Path::new("/tmp")).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn captures_process_output() {
        let script = versioned_script("printf 'Code is: secret-code\\n'; printf 'warning\\n' >&2");
        let backend = CrocBackend::new(&script);
        let request = SendRequest::new(vec![PathBuf::from("ignored")]).unwrap();
        let output = backend.send(request).await.unwrap().wait().await.unwrap();
        assert_eq!(output.stdout, b"Code is: [REDACTED]\n");
        assert_eq!(output.stderr, b"warning\n");
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn emits_live_code_and_capability_events() {
        let script = versioned_script("sleep 0.05; printf 'Code is: live-code\\n' >&2; sleep 0.05");
        let backend = CrocBackend::new(&script);
        let request = SendRequest::new(vec![PathBuf::from("ignored")]).unwrap();
        let mut handle = backend.send(request).await.unwrap();
        let mut updates = handle.take_updates().unwrap();
        let wait_task = tokio::spawn(async move { handle.wait().await });

        assert_eq!(
            time::timeout(Duration::from_secs(1), updates.recv())
                .await
                .unwrap()
                .unwrap(),
            BackendEvent::CapabilityUnavailable {
                capability: BackendCapability::Progress,
            }
        );
        assert_eq!(
            time::timeout(Duration::from_secs(1), updates.recv())
                .await
                .unwrap()
                .unwrap(),
            BackendEvent::CodeGenerated {
                code: "live-code".into()
            }
        );
        wait_task.await.unwrap().unwrap();
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn keeps_receiver_code_out_of_args_and_diagnostics() {
        let script = versioned_script("printf 'args=%s\\nsecret=%s\\n' \"$*\" \"$CROC_SECRET\"");
        let backend = CrocBackend::new(&script);
        let request = ReceiveRequest::new("receiver-secret", "/tmp/out").unwrap();
        let output = backend
            .receive(request)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        let output = String::from_utf8(output.stdout).unwrap();
        assert!(output.contains("--out /tmp/out"));
        assert!(output.contains("secret=[REDACTED]"));
        assert!(!output.contains("receiver-secret"));
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_malformed_or_missing_code_signals() {
        let malformed = versioned_script("printf 'Code is: malformed code\\n'");
        let backend = CrocBackend::new(&malformed);
        let error = backend
            .send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::OutputParse { .. }));
        let _ = std::fs::remove_file(malformed);

        let missing = versioned_script("printf 'Sending file (1 MB)\\n'");
        let backend = CrocBackend::new(&missing);
        let error = backend
            .send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BackendError::MissingSignal {
                signal: "transfer code"
            }
        ));
        let _ = std::fs::remove_file(missing);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounds_long_diagnostics_without_losing_code_parsing() {
        let script = versioned_script(
            "dd if=/dev/zero bs=1024 count=80 2>/dev/null | tr '\\0' x >&2; printf '\\nCode is: bounded-code\\n' >&2",
        );
        let backend = CrocBackend::new(&script);
        let output = backend
            .send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert!(output.stderr.len() <= DIAGNOSTIC_LIMIT + 32);
        assert!(String::from_utf8_lossy(&output.stderr).contains("output truncated"));
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn maps_non_zero_exit_code_and_stderr() {
        let script = versioned_script("printf 'failed\\n' >&2; exit 7");
        let backend = CrocBackend::new(&script);
        let request = SendRequest::new(vec![PathBuf::from("ignored")]).unwrap();
        let error = backend
            .send(request)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BackendError::ProcessFailed {
                code: Some(7),
                stderr
            } if stderr == "failed"
        ));
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn supports_timeout_and_cancellation() {
        let script = versioned_script("while true; do :; done");
        let backend = CrocBackend::new(&script).with_timeout(Duration::from_millis(20));
        let request = SendRequest::new(vec![PathBuf::from("ignored")]).unwrap();
        let error = backend
            .send(request)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Timeout { .. }));

        let mut handle = backend
            .send(SendRequest::new(vec![PathBuf::from("ignored")]).unwrap())
            .await
            .unwrap();
        handle.cancel().await.unwrap();
        let _ = std::fs::remove_file(script);
    }
}
