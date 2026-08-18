# Drift — GitHub Copilot Instructions

## Scope

These instructions define project-wide architectural rules. Focused rules live in `.github/instructions/` and load when their file patterns or task descriptions match.

User requests remain authoritative. Within project guidance, prefer the more specific instruction when it does not violate security or architecture. When guidance conflicts, stop and surface the conflict before making a risky change.

## Mission

Drift is a Rust-native desktop application for secure peer-to-peer and relay-assisted file transfer.

Product goals:

- short-lived transfer codes for pairing;
- end-to-end encrypted transfer;
- direct connection with relay fallback;
- multi-file and directory transfer;
- resumable transfers;
- minimal user interaction;
- no mandatory account or cloud storage.

The long-term product is an independent transfer platform, not a GUI wrapper around croc:

```text
                 Drift
                   |
       ┌───────────┼───────────┐
       |           |           |
      GUI         CLI        Relay
       |           |           |
       └───────────┼───────────┘
                   |
            Transfer Core
                   |
       ┌───────────┼───────────┐
       |           |           |
    Direct       Relay       Stored
       |           |           |
       └───────────┼───────────┘
                   |
             Encrypted Data
```

Current strategy:

1. Build the desktop application with GPUI.
2. Keep Transfer Core independent of UI and protocol details.
3. Use croc through `CrocBackend` for the first usable backend.
4. Keep backend replacement possible.
5. Delay native protocol work until the architecture is stable and security-reviewed.

## Product Constraints

- Rust remains the primary language.
- GUI remains Rust-native with GPUI.
- Use Tokio for long-running asynchronous work.
- Do not add JavaScript, WebView, React, Electron, or Tauri unless explicitly requested.
- Do not invent cryptographic or NAT-traversal protocols.
- Relay handles rendezvous and encrypted traffic forwarding; it must not inspect plaintext file contents.
- UI must stay simple: `Drop -> Code -> Send` and `Code -> Receive`.

## Workspace Architecture

Preferred layout:

```text
drift/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── crates/
│   ├── drift-app/
│   ├── drift-ui/
│   ├── drift-core/
│   ├── drift-transfer/
│   ├── drift-protocol/
│   ├── drift-network/
│   ├── drift-storage/
│   ├── drift-relay/
│   └── drift-cli/
├── tests/
├── docs/
└── .github/
    ├── copilot-instructions.md
    └── instructions/
```

Crate ownership:

| Crate | Owns | Must not own |
| --- | --- | --- |
| `drift-app` | startup, composition, config loading, logging, window creation | low-level transfer or networking logic |
| `drift-ui` | GPUI views, actions, presentation state, input, drag/drop | protocol implementation |
| `drift-core` | domain types, state machine, commands, events, errors | GPUI or concrete network code |
| `drift-transfer` | orchestration, scheduling, progress, cancellation, retry, verification | GPUI |
| `drift-protocol` | backend traits and protocol adapters | UI concerns |
| `drift-network` | connections, transports, timeout, reconnect, metrics | UI concerns |
| `drift-storage` | streaming files, partial files, manifests, resume metadata, atomic rename | UI concerns |
| `drift-relay` | pairing, forwarding, limits, lifecycle, relay metrics | plaintext file access |
| `drift-cli` | CLI parsing and presentation using shared core | duplicate transfer logic |

## Dependency Direction

Control and event flow:

```text
UI -> App -> Transfer/Core -> Protocol -> Network
                    └-----> Storage
```

Rust crate dependencies:

```text
drift-app -> drift-ui
drift-app -> drift-core
drift-app -> drift-transfer
drift-ui -> drift-core
drift-transfer -> drift-core
drift-transfer -> drift-protocol
drift-transfer -> drift-storage
drift-protocol -> drift-network
```

Forbidden dependencies:

```text
drift-core → drift-ui
drift-transfer → drift-ui
drift-protocol → drift-ui
drift-storage → drift-ui
drift-network → drift-ui
```

No circular dependencies. Do not leak GPUI, croc command details, or a specific transport into domain APIs.

## Architectural Invariants

- UI sends domain commands; it does not start processes or perform transfer scheduling.
- Transfer state is separate from UI state.
- Transfer lifecycle uses explicit states, not unrelated boolean flags.
- Backend choice stays behind `TransferBackend`.
- Large files use streaming and bounded buffers.
- Received data goes to temporary or partial files, is verified, then is atomically renamed.
- Secrets never enter logs, user-facing errors, persisted metadata, or debug output unnecessarily.
- The relay never needs plaintext file access.

## Instruction Map

| File | Scope |
| --- | --- |
| `rust.instructions.md` | Rust style, workspace code, Cargo, async, errors, dependencies |
| `gpui.instructions.md` | GPUI views, application composition, UI state, responsiveness |
| `transfer.instructions.md` | domain lifecycle, manifests, chunks, resume, orchestration |
| `protocol.instructions.md` | backend abstraction, Croc process, network and relay boundaries |
| `security.instructions.md` | cryptography, secrets, path safety, security review |
| `tests.instructions.md` | unit, integration, property, E2E, validation and test naming |

Read the relevant focused file before changing its concern. Security-sensitive work must follow `security.instructions.md` even when another file also applies.

## Delivery Rules

Before editing:

1. Identify the owning crate and direct behavior.
2. Read nearby implementation, callers, and existing tests.
3. State one local hypothesis and one check that can disprove it.
4. Make the smallest change that tests the hypothesis.

After editing:

1. Run the narrowest relevant test, check, or lint command first.
2. Repair failures in the same slice before widening scope.
3. Run formatting and the relevant workspace checks.
4. Report commands actually run and blockers honestly.

Keep changes focused. Do not perform unrelated dependency upgrades, broad refactors, commits, or branch operations unless explicitly requested. Update documentation when public behavior or architecture changes.

## Current Priorities

### Phase 0 — Foundation

```text
Rust workspace
GPUI application shell
Transfer Core
CrocBackend
logging
focused tests
```

### Phase 1 — MVP

```text
Send / Receive flows
drag and drop
progress
cancel
pause / resume
multi-file and directory transfer
custom relay
macOS / Linux validation
```

### Phase 2 — Native Engine

```text
native protocol abstraction
native transport
independent relay
resume implementation
security review
```

Do not implement Phase 2 cryptography before Phase 0 and Phase 1 architecture is stable and reviewed.

# Workspace Rules

- After completing any task in this workspace, always output a summary and description in English of the changes made (like a commit message: concise summary line + detailed description), placed inside a bash fenced code block so symbols and formatting are easy to copy.
- Do NOT automatically run `git commit`. Only output the summary and description in your final response.
- Always prefix CLI commands with `rtk` (see RTK.md: `rtk <command>`). Do not run raw commands.

## Final Rule

> Keep UI replaceable, transfer engine independent, protocol modular, and cryptography conservative.
