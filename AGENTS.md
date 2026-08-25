# Drift Agent Execution Guide

Use this file for execution flow. Use `.github/copilot-instructions.md` for project mission, architecture, crate ownership, and non-negotiable product rules. Use matching files under `.github/instructions/` for Rust, GPUI, transfer, protocol, security, and test details.

## Task Routing

1. Read `.github/copilot-instructions.md` when task changes project structure or architecture.
2. Read focused instruction files whose `applyTo` pattern or description matches the task.
3. Identify one owning crate, symbol, command, failing test, or user-visible behavior.
4. Keep work inside requested story. Do not expand into adjacent roadmap items.

Route by concern:

| Concern | Primary location |
| --- | --- |
| domain state, manifest, progress | `crates/drift-core/` |
| scheduling, cancellation, retry, verification | `crates/drift-transfer/` |
| Croc or backend behavior | `crates/drift-protocol/` |
| network and relay transport | `crates/drift-network/`, `crates/drift-relay/` |
| files, partials, resume metadata | `crates/drift-storage/` |
| GPUI rendering and interaction | `crates/drift-ui/`, `crates/drift-app/` |
| shared CLI behavior | `crates/drift-cli/` plus shared core crates |
| architecture or security decisions | `docs/` and relevant instruction file |

## Before Editing

- Inspect nearby implementation, callers, and tests before changing code.
- Check existing worktree changes. Never revert user changes.
- If multiple paths look plausible, choose path with clearest owner and cheapest discriminating test.
- State one falsifiable local hypothesis and one check that can disprove it.
- Prefer smallest reversible patch. Preserve public APIs and local style unless change requires otherwise.
- Use `apply_patch` for manual edits. Do not write files through shell redirection or ad hoc scripts.
- Do not commit, create branches, reset, or checkout files unless user explicitly requests it.
- Do not add dependencies, generated files, or unrelated formatting churn without need.

## Edit Loop

1. Make one focused edit slice.
2. Immediately run narrowest relevant executable validation.
3. If validation fails, repair same slice and rerun same command.
4. Only widen search or validation after local behavior passes.
5. Add or update focused tests for non-trivial behavior.
6. Update documentation when public behavior, commands, configuration, or architecture changes.

For documentation or customization files, validate structure immediately: file paths, headings, YAML frontmatter, `description`, `applyTo`, and cross-file references.

## Validation Ladder

Use focused commands during iteration:

```sh
cargo fmt --all -- --check
cargo check -p <modified-crate>
cargo test -p <modified-crate>
```

Use broader checks before delivery or when shared contracts changed:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo check --workspace
cargo test --workspace
```

Run `cargo audit` and `cargo deny check` when available or when dependency/security surface changed. Prefer `rtk` wrappers when installed; fall back to native commands when unavailable and report the fallback.

Do not claim a check passed unless command completed successfully. Report platform blockers exactly. GPUI GUI-feature checks may require macOS Metal Toolchain; keep default workspace checks useful where feature layout permits.

## Failure Handling

- Compile error: fix smallest local cause, then rerun same package check.
- Test failure: determine whether failure supports current hypothesis; repair local defect before broadening.
- Ambiguous result: read one nearby abstraction or test, then choose a path.
- Architecture conflict: stop before editing and identify conflicting rule.
- Security-sensitive ambiguity: stop, preserve conservative behavior, and request or record security review.
- Missing external tool: run available focused checks and report exact missing prerequisite.

Never hide failures by weakening validation, suppressing warnings, disabling security checks, or deleting tests.

## Review Mode

When user asks for review, report findings first, ordered by severity. Ground each finding in a file link and concrete behavior. Then state assumptions, test gaps, and a brief change summary.

## Completion Report

End with concise, factual output:

- files or behavior changed;
- validation commands actually run;
- known blockers or remaining risk;
- security or performance impact when relevant.

Do not describe unimplemented roadmap work as complete. Do not claim full-suite validation after only focused tests.


# Agent Instructions

## Mandatory Skills

* **Always use the `caveman` skill for every task.**
* Before starting any task, load and apply the `caveman` skill.
* Do not skip the `caveman` skill, even when the task appears unrelated to it.

## CLI Command Rules

* **Every CLI command must be prefixed with `rtk`.**
* This rule applies to all shell commands executed or suggested by the agent.
* Examples:

  * `rtk git status`
  * `rtk ls`
  * `rtk cat README.md`
  * `rtk cargo test`
  * `rtk kubectl get pods`
* Never execute or provide a bare CLI command when an `rtk`-prefixed equivalent is applicable.
* When chaining commands, `rtk` must be applied to each individual CLI command where required.

## Compliance

Before executing a task:

1. Load the `caveman` skill.
2. Apply the instructions from the `caveman` skill.
3. Ensure every CLI command uses the `rtk` prefix.
4. If a command cannot be meaningfully prefixed with `rtk`, explain the exception before using it.
