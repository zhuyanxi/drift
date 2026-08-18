---
name: "Drift Rust"
description: "Use when writing or reviewing Rust source, Cargo manifests, workspace crates, async tasks, errors, dependencies, or configuration."
applyTo: ["**/*.rs", "**/Cargo.toml", "Cargo.toml", "rust-toolchain.toml"]
---

# Rust Rules

## Language and Workspace

- Use the pinned stable toolchain from `rust-toolchain.toml`.
- Keep crate responsibilities and dependency direction from `copilot-instructions.md`.
- Keep domain APIs free of GPUI, croc command details, and concrete transport types.
- Use traits only at real replacement boundaries such as `TransferBackend`, `NetworkTransport`, or storage.
- Do not add a JavaScript runtime or WebView dependency.

## Code Style

- Prefer ownership, borrowing, `Result`, `Option`, enums, and small focused functions.
- Avoid needless cloning, premature abstractions, and one-letter variable names.
- Use `thiserror` for typed library errors. Use `anyhow` only at application boundaries when contextual errors matter more than matching.
- Do not use `unwrap()`, `expect()`, or `panic!()` in normal runtime paths. Tests and locally proven invariants are the only exceptions.
- Add public API documentation where callers need behavior or safety guarantees.
- Add comments only for security assumptions, concurrency invariants, platform behavior, protocol constraints, or non-obvious performance choices.

## Async and I/O

- Use Tokio for long-running network, process, file, hashing, and scheduling work.
- Never block the GPUI event or render loop.
- Avoid synchronous large-file I/O, blocking network calls, process waits, and arbitrary sleeps in async paths.
- Give every long-running task a cancellation and shutdown path.
- Bound queues and buffers. Memory must scale with chunk size and concurrency, not file size.

## Errors and Logging

- Map low-level errors into typed domain or application errors at crate boundaries.
- Keep user-facing messages separate from OS, process, and protocol diagnostics.
- Use `tracing` structured fields for correlation, normally with transfer IDs.
- Never log transfer codes, passwords, keys, PAKE material, capabilities, or raw payloads. See `security.instructions.md` for the complete policy.

## Configuration and Dependencies

- Load typed configuration once, then pass it to services. Do not read environment variables throughout the codebase.
- Before adding a dependency, check necessity, maintenance, security advisories, transitive cost, and cross-platform impact.
- Do not perform unrelated dependency upgrades or broad lockfile modernization.
- Pin pre-1.0 GPUI versions exactly and treat GPUI upgrades as dedicated changes.

## Validation

For local iteration, run focused checks first:

```sh
cargo fmt --all -- --check
cargo check -p <modified-crate>
cargo test -p <modified-crate>
```

Before merge or in CI, run the workspace checks required by the repository:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo check --workspace
cargo test --workspace
```

Do not claim a command passed unless it actually ran successfully. If platform tooling blocks a feature, report the exact blocker and keep platform-independent checks green.
