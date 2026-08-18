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
cargo fmt --all -- --check
cargo check --workspace
cargo test -p drift-core
cargo test -p drift-protocol
cargo test -p drift-transfer
cargo test -p drift-storage
```

The default workspace build keeps GPUI optional so core and backend checks work on machines without Apple's Metal toolchain. Build the windowed app with:

```sh
cargo run -p drift-app --features gui
```

On macOS, GPUI `0.2.2` requires the Metal Toolchain component from Xcode.
