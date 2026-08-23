# drift

Rust-native desktop file transfer foundation.

## Workspace

- `drift-core`: manifest, path policy, lifecycle, progress, chunks, resume state
- `drift-protocol`: `TransferBackend` and Croc process adapter
- `drift-transfer`: serialized session manager and event stream
- `drift-storage`: atomic JSON resume persistence
- `drift-ui`: GPUI shell
- `drift-app`: desktop entry point and tracing setup
- `drift-network`, `drift-relay`, `drift-cli`: Phase 0 boundaries

## Checks

```sh
rtk cargo fmt --all -- --check
rtk cargo check --workspace --locked
rtk cargo test -p drift-core --locked
rtk cargo test -p drift-protocol --locked
rtk cargo test -p drift-transfer --locked
rtk cargo test -p drift-storage --locked
```

## Desktop App

Default workspace build keeps GPUI optional so core and backend checks work without desktop GUI prerequisites. Launch windowed app with:

```sh
rtk cargo run -p drift-app --features gui --locked
```

Send flow:

1. Click **Choose files or folders** on Home, or drop files/folders anywhere on Home or Send.
2. Drift scans selected paths, validates them, then prepares transfer.
3. Start transfer and share displayed code with receiver.

Receive flow: open **Receive**, enter transfer code, choose destination folder, then receive.

Croc must be installed and available on `PATH`, or configured through Drift settings. On macOS, GPUI `0.2.2` may require Xcode Metal Toolchain:

```sh
rtk xcodebuild -downloadComponent MetalToolchain
rtk xcrun --find metal
```
