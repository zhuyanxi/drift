# P1-12 MVP Validation

## Validation Boundary

The P1-12 integration target exercises the real `TransferManager -> TransferBackend -> CrocBackend -> child process` boundary on Unix. It does not call the public Croc relay during the default test suite.

The test harness creates an isolated temporary root for every test. It provides:

- a generated executable that reports the controlled Croc-compatible version `v11.2.2-build`;
- a mailbox below that temporary root instead of a developer home directory or permanent path;
- source, destination, and JSON resume-store directories below the same root;
- deterministic process behaviors for pairing, relay timeout, process failure, partial receive failure, and cancellation.

The fake executable still receives structured Croc arguments and `CROC_SECRET` through the production adapter. This validates argument ordering, relay selection, code handling, process lifecycle, safe error mapping, staging cleanup, and recovery behavior without making CI dependent on a mutable `PATH`, a public relay, or production secrets.

This harness is not a cryptographic or public-network test. Croc remains the MVP security boundary; native protocol and relay work require a separate security review.

## Focused Commands

Run the focused integration target during iteration:

```sh
rtk cargo test -p drift-transfer --test p1_12_mvp
```

The merge validation set is:

```sh
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets --all-features -- -D warnings
rtk cargo check --workspace
rtk cargo test --workspace
rtk cargo audit
rtk cargo deny check
```

`cargo-audit` and `cargo-deny` are not part of the Rust toolchain. Install them locally with:

```sh
rtk cargo install cargo-audit --locked --version 0.21.2
rtk cargo install cargo-deny --locked --version 0.18.3
```

The checked-in `deny.toml` rejects unknown registries and Git sources, records the accepted license policy, and leaves advisory and duplicate-version findings visible to CI.

## Platform Prerequisites

### macOS

Core and transfer checks do not need the GUI toolchain. GUI smoke validation for GPUI `0.2.2` needs Xcode's Metal Toolchain:

```sh
rtk xcode-select --install
rtk xcodebuild -downloadComponent MetalToolchain
rtk xcrun --find metal
rtk cargo check -p drift-app --features gui
```

When `xcrun --find metal` fails, the GUI job is reported as blocked with the exact prerequisite. Core, transfer, and non-GUI workspace checks remain runnable.

### Linux

The GUI smoke job installs the X11, Wayland, OpenGL/EGL, font, and keyboard development packages required to compile the GPUI target:

```sh
sudo apt-get update
sudo apt-get install --yes \
  libfontconfig1-dev libfreetype6-dev libwayland-dev \
  libx11-dev libx11-xcb-dev libxcb1-dev \
  libxkbcommon-dev libxkbcommon-x11-dev libxrandr-dev \
  libxi-dev libxcursor-dev libxinerama-dev \
  libgl1-mesa-dev libegl1-mesa-dev
rtk cargo check -p drift-app --features gui
```

A headless display server is needed only for GUI runtime tests. The P1-12 GUI smoke step is a build check and does not require a display session.

## CI Matrix

`.github/workflows/p1-12-validation.yml` runs default workspace formatting, clippy, check, tests, and the focused integration target on `ubuntu-latest` and `macos-latest`. A separate GUI job runs all-features clippy and the GUI build when Linux dependencies or the macOS Metal Toolchain are available. Missing Metal is recorded as an explicit blocked prerequisite rather than treated as a passing GUI build.

The dependency-security job installs the pinned `cargo-audit` and `cargo-deny` versions and fails on their findings.

When a workspace job fails, it uploads only runner and toolchain versions from `test-output/environment.txt`. Raw Croc output, transfer codes, secrets, and permanent paths are not collected as CI artifacts.

## Coverage and Limits

The integration target checks:

- byte-identical single-file output;
- multi-file and nested directory layout;
- typed, user-safe process failure and relay timeout errors;
- retry and cancellation of a retryable relay interruption;
- removal of unverified receive output and staging directories;
- Croc pause/resume capability reporting;
- versioned recovery metadata and restart behavior;
- invalid receive destinations before backend start.

The target is Unix-only because its controlled executable uses a temporary POSIX shell script. Windows release validation remains outside P1-12. The fake relay behavior proves Drift's failure and retry boundaries, not relay availability or transport security on the public network.
