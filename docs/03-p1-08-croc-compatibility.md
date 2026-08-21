# P1-08 Croc Compatibility Decision

## Decision

Drift does not expose byte-level pause/resume for Croc 11.2.x. Drift exposes safe cancel-and-restart recovery only.

## Evidence

- Installed validation binary reports `croc version 11.2.2`.
- Croc 11.2.2 `send --help` exposes transfer, relay, storage, and proxy options, but no pause or resume command/flag.
- Croc documentation states that interrupted transfers can resume, but does not define a pause command, signal contract, metadata API, or confirmation event that Drift can safely control.
- Drift's Croc adapter currently owns structured process arguments, cancellation, child termination, output capture, and reaping. It has no verified pause handshake.

## Consequences

- `BackendCapability::Pause` and `BackendCapability::Resume` remain false for `CrocBackend`.
- TransferManager returns `CapabilityUnavailable` for pause/resume instead of changing lifecycle state or killing a process.
- Drift never labels a Croc transfer `Paused` or `Resuming` based on guessed signals.
- Recoverable interruption persists versioned, secret-free metadata. Recovery requires explicit user action and starts a fresh Croc attempt after source/destination validation.
- Receive recovery metadata records only the relative name of Drift-owned hidden staging output. Discard revalidates the destination and removes that owned staging tree; it does not delete unrelated destination entries.
- Receiver recovery may use a newly selected, validated destination; persisted destination remains fallback when no replacement is supplied.
- Native byte-level resume remains outside this story until Croc behavior is verified on macOS and Linux with an executable peer transfer.
