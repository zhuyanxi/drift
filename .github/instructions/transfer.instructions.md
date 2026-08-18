---
name: "Drift Transfer Core"
description: "Use when writing or reviewing transfer sessions, TransferManager, manifests, progress, chunk scheduling, pause/resume, retry, verification, or resume persistence."
applyTo: ["crates/drift-core/**/*.rs", "crates/drift-transfer/**/*.rs", "crates/drift-storage/**/*.rs"]
---

# Transfer Rules

## Ownership

- `drift-core` owns domain types, commands, events, lifecycle states, errors, manifests, progress, and resume models.
- `drift-transfer` owns orchestration, scheduling, backend calls, cancellation, pause/resume, retry, progress aggregation, and verification flow.
- `drift-storage` owns file streaming, temporary and partial files, manifest persistence, resume metadata, and atomic rename.
- None of these crates may depend on GPUI.

## Lifecycle

Represent transfer lifecycle with explicit states:

```text
Created
Connecting
Authenticating
Negotiating
Transferring
Paused
Resuming
Verifying
Completed
Failed
Cancelled
```

- Validate every transition.
- Serialize state transitions through `TransferManager` or the owning supervisor.
- Do not replace the state machine with independent flags such as `is_connected`, `is_paused`, or `is_finished`.
- Terminal states cannot transition again.
- Cancellation must stop active tasks and leave an observable terminal state.

## Events and Commands

Use domain commands for user intent, such as:

```text
SendFiles
ReceiveByCode
PauseTransfer
ResumeTransfer
CancelTransfer
RetryTransfer
ChooseOutputDirectory
CopyTransferCode
```

Publish domain events for UI and application consumers:

```text
Created
Connecting
Connected
Authenticating
MetadataReady
Progress
Paused
Resumed
Verifying
Completed
Failed
Cancelled
```

Do not make UI code poll internal task state.

## Manifest and Paths

`TransferManifest` must identify the transfer, every file, relative path, size, modification metadata where needed, digest where available, and total size.

- Reject empty manifests when a transfer requires files.
- Reject `..`, absolute paths, Windows drive paths, UNC paths, empty components, invalid names, and symlink escapes.
- Treat received paths as untrusted input.
- Validate manifest totals and metadata before scheduling data.
- Never allow a received path to overwrite an unintended destination.
- Keep path policy in a reusable domain or storage boundary, not inside a view.

## Streaming and Chunks

- Stream large files. Never load a GB-scale file into memory.
- Use bounded buffers and an explicit chunk size. Initial target is approximately 4 MiB with 4-8 in-flight chunks, subject to measurement.
- Track chunk identity, offset, length, state, and integrity metadata where the backend supports it.
- Make chunk boundaries deterministic and test zero-size, exact-boundary, and final-partial cases.
- Progress must never exceed total bytes. Define behavior for zero-byte transfers.

## Resume

Resume state must identify:

```text
transfer_id
file_id
file_size
chunk_size
completed_chunks
digest
temp_file_path
```

Before resuming:

1. Validate transfer and manifest metadata.
2. Validate source or destination file identity.
3. Validate completed chunks where required.
4. Reconnect and request missing chunks.
5. Verify the complete file.
6. Atomically rename only after verification.

Never treat `partial_file_size == completed_bytes` as proof of correct content.

## Persistence and Errors

- Write resume metadata through a temporary file followed by atomic rename.
- Keep storage metadata separate from the file data channel.
- Map backend, network, filesystem, security, cancellation, and internal errors into typed transfer errors.
- Preserve machine-matchable causes while exposing concise user-facing messages at the application boundary.
- Do not persist secrets or log sensitive paths unnecessarily.

## Transfer Validation

Test state transitions, invalid transitions, manifest validation, path policy, chunk boundaries, progress aggregation, resume ordering, cancellation, retry, verification, and persistence round trips. Use fake backends for orchestration tests so tests do not require a live relay or a croc executable.
