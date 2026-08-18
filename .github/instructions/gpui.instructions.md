---
name: "Drift GPUI"
description: "Use when writing or reviewing GPUI views, desktop application composition, actions, drag and drop, keyboard input, rendering, or UI state in drift-app and drift-ui."
applyTo: ["crates/drift-ui/**/*.rs", "crates/drift-app/**/*.rs"]
---

# GPUI Rules

## Ownership

- Keep GPUI-specific code inside `drift-ui` and `drift-app`.
- `drift-ui` owns views, components, actions, presentation state, dialogs, drag and drop, progress rendering, and keyboard interaction.
- `drift-app` owns startup, application composition, configuration loading, logging, window creation, and service wiring.
- Do not implement transfer protocols, file streaming, encryption, relay logic, or scheduling inside a view.
- Do not leak GPUI types into `drift-core`, `drift-transfer`, `drift-protocol`, `drift-network`, or `drift-storage`.

Preferred flow:

```text
User action -> domain command -> TransferManager -> TransferBackend
Transfer task -> TransferEvent -> application layer -> GPUI entity update -> render
```

## GPUI Usage

- Use GPUI idioms: `Application`, `App`, `Entity`, `Context`, `Window`, `View`, `Element`, and `Action`.
- Follow the exact GPUI version pinned in `Cargo.toml`. Do not invent APIs from another GPUI release.
- Keep the default workspace check usable without optional platform GUI tooling when the project feature layout requires it.
- Keep render methods deterministic and cheap. Move async work and expensive file operations outside the render path.
- Use event-driven updates. Do not aggressively poll transfer state from the UI.

## State Boundaries

Keep UI state separate from transfer state.

UI state may include:

```text
CurrentView
SelectedFiles
InputCode
DialogState
SettingsViewState
```

Transfer state belongs to domain and transfer crates:

```text
TransferSession
TransferProgress
TransferError
```

Adapt domain values into view models instead of moving GPUI entities into domain APIs.

## Interaction and Responsiveness

- Keep Send and Receive flows explicit, with loading, empty, error, disabled, and completed states.
- File pickers and drag/drop handlers produce paths or domain commands; they do not invoke protocol code directly.
- Keep keyboard actions and focus behavior predictable.
- Keep the UI responsive during hashing, filesystem scanning, connection, transfer, retry, pause, and resume.
- Use cancellation-aware async tasks and update entities from task results or domain events.
- Avoid large text, progress labels, or dynamic content that changes stable control dimensions.

## Platform Boundaries

Use platform abstractions for file picking, clipboard, notifications, opening paths, revealing files, and tray behavior. Keep `cfg` branches inside dedicated platform modules rather than scattering them through views.

## UI Validation

Add focused tests for input, focus, actions, render state, and keyboard behavior when supported by GPUI. Validate large-content, error, loading, and disabled states. If the local machine lacks the Metal Toolchain, run default workspace checks and report GUI-feature validation as blocked rather than hiding the failure.
