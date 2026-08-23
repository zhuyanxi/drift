use drift_protocol::CrocBackend;
use drift_storage::JsonStore;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

pub const TEST_CODE: &str = "integration-code";

#[derive(Clone, Copy)]
pub enum FakeCrocBehavior {
    Pair,
    ProcessFailure,
    RelayFailure,
    ReceivePartialFailure,
    Slow,
}

impl FakeCrocBehavior {
    fn name(self) -> &'static str {
        match self {
            Self::Pair => "pair",
            Self::ProcessFailure => "process-failure",
            Self::RelayFailure => "relay-failure",
            Self::ReceivePartialFailure => "receive-partial-failure",
            Self::Slow => "slow",
        }
    }
}

pub struct Harness {
    root: PathBuf,
}

impl Harness {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join(format!("drift-p1-12-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join("scripts")).unwrap();
        fs::create_dir_all(root.join("mailbox")).unwrap();
        Self { root }
    }

    pub fn path(&self, relative: impl AsRef<Path>) -> PathBuf {
        self.root.join(relative)
    }

    pub fn source(&self, relative: impl AsRef<Path>, contents: &[u8]) -> PathBuf {
        let path = self.path(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, contents).unwrap();
        path
    }

    pub fn destination(&self) -> PathBuf {
        let path = self.path("destination");
        fs::create_dir_all(&path).unwrap();
        path
    }

    pub fn resume_store(&self) -> JsonStore {
        JsonStore::new(self.path("resume"))
    }

    pub fn backend(&self, behavior: FakeCrocBehavior, timeout: Duration) -> CrocBackend {
        let script = self.write_script(behavior);
        CrocBackend::new(script).with_timeout(timeout)
    }

    fn write_script(&self, behavior: FakeCrocBehavior) -> PathBuf {
        let script_path = self
            .path("scripts")
            .join(format!("fake-croc-{}.sh", behavior.name()));
        let mailbox = shell_quote(&self.path("mailbox"));
        let script = format!(
            r#"#!/bin/sh
set -eu
mailbox={mailbox}
behavior='{behavior}'

if [ "${{1:-}}" = "--version" ]; then
    printf 'v11.2.2-build\n'
    exit 0
fi

if [ "$behavior" = "process-failure" ]; then
    printf 'private integration backend diagnostic\n' >&2
    exit 7
fi

if [ "$behavior" = "slow" ]; then
    while :; do sleep 1; done
fi

mode=""
output=""
relay_requested=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        send)
            mode="send"
            rm -rf "$mailbox/payload.tmp" "$mailbox/payload"
            mkdir -p "$mailbox/payload.tmp"
            ;;
        --out)
            shift
            output="$1"
            ;;
        --relay)
            relay_requested=1
            shift
            ;;
        --disable-clipboard|--yes)
            ;;
        *)
            if [ "$mode" = "send" ]; then
                cp -R "$1" "$mailbox/payload.tmp/"
            fi
            ;;
    esac
    shift
done

if [ "$behavior" = "relay-failure" ] && [ "$relay_requested" -eq 1 ]; then
    printf 'private relay diagnostic\n' >&2
    while :; do sleep 1; done
fi

if [ "$mode" = "send" ]; then
    mv "$mailbox/payload.tmp" "$mailbox/payload"
    printf 'Code is: {code}\n' >&2
    exit 0
fi

if [ -n "$output" ]; then
    if [ "${{CROC_SECRET:-}}" != "{code}" ]; then
        printf 'unexpected transfer code\n' >&2
        exit 8
    fi
    while [ ! -d "$mailbox/payload" ]; do sleep 0.01; done
    mkdir -p "$output"
    if [ "$behavior" = "receive-partial-failure" ]; then
        printf 'unverified partial output' > "$output/file.txt"
        exit 7
    fi
    for entry in "$mailbox/payload"/*; do
        [ -e "$entry" ] || continue
        cp -R "$entry" "$output/"
    done
    exit 0
fi

printf 'unsupported fake croc invocation\n' >&2
exit 9
"#,
            mailbox = mailbox,
            behavior = behavior.name(),
            code = TEST_CODE,
        );
        fs::write(&script_path, script).unwrap();
        let mut permissions = fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&script_path, permissions).unwrap();
        script_path
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}
