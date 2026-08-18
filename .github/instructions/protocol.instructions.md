---
name: "Drift Protocol and Network"
description: "Use when writing or reviewing TransferBackend implementations, CrocBackend process handling, network transports, relay boundaries, protocol errors, or backend selection."
applyTo: ["crates/drift-protocol/**/*.rs", "crates/drift-network/**/*.rs", "crates/drift-relay/**/*.rs"]
---

# Protocol and Network Rules

## Backend Boundary

Keep protocol choice behind a replaceable boundary:

```rust
#[async_trait]
pub trait TransferBackend: Send + Sync {
    async fn send(&self, request: SendRequest) -> Result<TransferHandle>;
    async fn receive(&self, request: ReceiveRequest) -> Result<TransferHandle>;
}
```

The exact API may evolve, but GUI and domain code must not know which backend is selected.

## CrocBackend

The first usable backend reuses croc. All croc-specific behavior stays inside `CrocBackend`:

- command construction;
- process spawning and lifecycle;
- stdin, stdout, and stderr handling;
- cancellation and timeout;
- exit status conversion;
- error mapping;
- cleanup.

Rules:

- Build commands from structured argument lists. Never interpolate untrusted values into a shell command.
- Keep transfer code, secrets, capabilities, and authentication material out of logs and debug output.
- Capture stderr for diagnostics, then map it to a typed backend error.
- Kill and reap timed-out or cancelled children.
- Do not let process details leak into UI or domain APIs.
- Test argument construction, output capture, non-zero exits, timeout, cancellation, and missing executable behavior.

## Native and Stored Backends

- Do not implement native cryptography casually. Native protocol work requires reviewed key establishment, nonce management, authenticated chunks, context binding, and a security review.
- Do not use a transfer code directly as an encryption key.
- Stored transfer is a future capability. Keep encrypted object storage and live relay transfer as separate backend lifecycles.

## Network Layer

`drift-network` owns connections, transports, timeout, reconnect, and network metrics. Keep it independent from UI and file presentation.

- Use Tokio for network tasks.
- Make timeouts and reconnect behavior explicit and cancellation-aware.
- Do not introduce QUIC or another transport without throughput, latency, loss, CPU, memory, and reconnect measurements.
- Map socket and transport failures into stable typed network errors before they reach application code.

## Relay Layer

A relay may provide:

```text
rendezvous
pairing
connection forwarding
connection lifecycle
resource limits
optional relay authentication
metrics
```

A relay must not:

```text
decrypt file data
inspect plaintext filenames
persist plaintext transfer content
log transfer secrets
```

Keep relay configuration, room expiry, connection limits, and rate limits explicit and typed.

## Protocol Validation

Prefer temporary directories, fake executables, and ephemeral relay harnesses. Integration tests must compare final file contents byte-for-byte and cover sender/receiver, multi-file, directory, interrupted transfer, invalid code, relay failure, disk-full, and integrity failure cases as the implementation reaches those phases.
