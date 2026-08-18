use crate::{BackendError, ReceiveRequest, SendRequest, TransferBackend};
use async_trait::async_trait;
use std::{ffi::OsString, fmt, path::PathBuf, process::ExitStatus, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    task::JoinHandle,
    time,
};
use tracing::debug;

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

    fn send_args(&self, request: &SendRequest) -> Result<Vec<OsString>, BackendError> {
        if request.paths.is_empty() {
            return Err(BackendError::InvalidRequest(
                "send request must contain at least one path".into(),
            ));
        }
        let mut args = self.relay_args(request.relay.as_deref());
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
        args.push(OsString::from(&request.code));
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
    ) -> Result<TransferHandle, BackendError> {
        debug!(role, argument_count = args.len(), "starting croc backend");
        let mut command = Command::new(&self.executable);
        command
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(BackendError::Spawn)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(BackendError::MissingPipe { stream: "stdout" })?;
        let stderr = child
            .stderr
            .take()
            .ok_or(BackendError::MissingPipe { stream: "stderr" })?;
        Ok(TransferHandle {
            child,
            stdout_task: Some(read_output(stdout)),
            stderr_task: Some(read_output(stderr)),
            timeout: self.timeout,
        })
    }
}

#[async_trait]
impl TransferBackend for CrocBackend {
    async fn send(&self, request: SendRequest) -> Result<TransferHandle, BackendError> {
        self.spawn(self.send_args(&request)?, "send").await
    }

    async fn receive(&self, request: ReceiveRequest) -> Result<TransferHandle, BackendError> {
        self.spawn(self.receive_args(&request)?, "receive").await
    }
}

pub struct TransferHandle {
    child: Child,
    stdout_task: Option<JoinHandle<Result<Vec<u8>, std::io::Error>>>,
    stderr_task: Option<JoinHandle<Result<Vec<u8>, std::io::Error>>>,
    timeout: Duration,
}

impl TransferHandle {
    pub async fn wait(mut self) -> Result<TransferOutput, BackendError> {
        let status = self.wait_for_exit().await?;
        self.finish(status).await
    }

    pub async fn wait_with_cancel(
        mut self,
        mut cancellation: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<TransferOutput, BackendError> {
        let status = tokio::select! {
            result = time::timeout(self.timeout, self.child.wait()) => match result {
                Ok(result) => result.map_err(BackendError::Io)?,
                Err(_) => {
                    self.terminate().await;
                    return Err(BackendError::Timeout { timeout: self.timeout });
                }
            },
            _ = &mut cancellation => {
                self.terminate().await;
                return Err(BackendError::Cancelled);
            }
        };
        self.finish(status).await
    }

    async fn wait_for_exit(&mut self) -> Result<ExitStatus, BackendError> {
        match time::timeout(self.timeout, self.child.wait()).await {
            Ok(result) => result.map_err(BackendError::Io),
            Err(_) => {
                self.terminate().await;
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
        Ok(CollectedOutput { stdout, stderr })
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
}

fn read_output<R>(mut reader: R) -> JoinHandle<Result<Vec<u8>, std::io::Error>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await?;
        Ok(output)
    })
}

async fn join_output(
    task: Option<JoinHandle<Result<Vec<u8>, std::io::Error>>>,
) -> Result<Vec<u8>, BackendError> {
    let task = task.ok_or(BackendError::MissingPipe { stream: "output" })?;
    task.await
        .map_err(BackendError::OutputTask)?
        .map_err(BackendError::Io)
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
            vec!["--relay", "relay.example", "send", "one.txt"]
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
            vec!["secret-code", "--out", "/tmp/out"]
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn validates_empty_requests() {
        assert!(SendRequest::new(Vec::new()).is_err());
        assert!(ReceiveRequest::new("", Path::new("/tmp")).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn captures_process_output() {
        let script = write_script("printf 'code\\n'; printf 'warning\\n' >&2");
        let backend = CrocBackend::new(&script);
        let request = SendRequest::new(vec![PathBuf::from("ignored")]).unwrap();
        let output = backend.send(request).await.unwrap().wait().await.unwrap();
        assert_eq!(output.stdout, b"code\n");
        assert_eq!(output.stderr, b"warning\n");
        let _ = std::fs::remove_file(script);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn maps_non_zero_exit_code_and_stderr() {
        let script = write_script("printf 'failed\\n' >&2; exit 7");
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
        let script = write_script("while true; do :; done");
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
