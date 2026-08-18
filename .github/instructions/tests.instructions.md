---
name: "Drift Tests"
description: "Use when adding or reviewing unit tests, integration tests, property tests, async tests, GPUI tests, E2E transfer tests, or Rust validation commands."
applyTo: ["tests/**/*.rs", "**/tests/**/*.rs", "**/*_test.rs"]
---

# Testing Rules

Every non-trivial feature needs focused tests. Test behavior and contracts, not private implementation details.

## Unit Tests

Cover:

- transfer state transitions and invalid transitions;
- manifest serialization and validation;
- path sanitization;
- chunk boundaries and scheduler state;
- progress calculation and aggregation;
- resume state ordering and validation;
- error mapping;
- Croc command construction and process lifecycle.

Use descriptive behavior names:

```rust
#[test]
fn resume_rejects_manifest_when_source_file_digest_changed() {}
```

Avoid names such as `test_resume` that hide the expected behavior.

## Integration Tests

Cover the real boundaries as they become available:

```text
sender -> receiver
multi-file transfer
directory transfer
interruption -> resume
invalid code
relay unavailable
disk full
integrity failure
```

Use temporary directories and ephemeral services. Compare final files byte-for-byte. Ensure children, temporary files, sockets, and relay rooms are cleaned up on success and failure.

## Property and Async Tests

Use `proptest` where it gives useful coverage for:

- path normalization;
- manifest round trips;
- chunk calculation;
- resume bitmaps;
- progress aggregation.

Use `tokio::test` for async lifecycle and cancellation. Keep timing deterministic; avoid arbitrary sleeps when a channel, barrier, or explicit timeout can express the condition.

## GPUI Tests

When GPUI test support is available, cover input, focus, actions, keyboard shortcuts, render state, loading, empty, error, disabled, and large-content behavior. Keep network and file-transfer tests outside GPUI.

## Validation Commands

Local iteration should stay narrow:

```sh
cargo fmt --all -- --check
cargo check -p <modified-crate>
cargo test -p <modified-crate>
```

Before merge or CI:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo check --workspace
cargo test --workspace
cargo audit
cargo deny check
```

Run only commands supported by the environment, and report unavailable tools or platform blockers. Do not claim full-suite success after running only a focused test.

## Test Quality

- Make tests deterministic and isolated.
- Assert externally meaningful state, events, errors, and final files.
- Include empty, exact-boundary, partial, malformed, cancelled, timed-out, and failure cases where relevant.
- Do not use production `unwrap`, `expect`, or `panic` as part of a runtime path merely to satisfy a test.
- Add a regression test for every fixed bug when practical.
