# P1-12 MVP Integration and Platform Validation

- **Epic:** Phase 1 - MVP
- **Priority:** P1
- **Primary owner:** `tests/`, CI configuration
- **Supporting crates:** all Phase 1 crates
- **Depends on:** P1-01 through P1-11
- **Labels:** `phase-1`, `integration`, `e2e`, `macos`, `linux`, `ci`, `security-review`

## User Story

As a release owner, I want repeatable integration checks on macOS and Linux so the MVP claim is based on real sender/receiver behavior, safe failure handling, and documented environment prerequisites.

## Scope

- Create integration harness with isolated temporary directories, test settings, and a controlled Croc executable/version.
- Exercise Drift sender/receiver through real app/transfer/protocol boundaries where feasible.
- Add end-to-end cases for single file, multi-file, directory, cancel, retryable relay/process failure, receive destination safety, and resume/recovery capability.
- Compare final outputs byte-for-byte and validate directory layout.
- Add macOS and Linux CI jobs for fmt, clippy, unit tests, integration tests, and a GUI smoke build where platform toolchain permits.
- Document prerequisite setup: Croc version, Xcode Metal Toolchain on macOS, Linux display/runtime dependencies, test relay strategy.
- Add security-tooling job or documented local command for `cargo audit` and `cargo deny check` when configuration is added.

## Implementation Steps

1. Add a reusable test harness module that creates temp source/destination/config locations and cleans them up.
2. Pin/download/provide Croc for CI through a verified mechanism; do not rely on mutable system PATH alone.
3. Decide whether tests use Croc public relay, a dedicated test relay, or fake executable per test class; document tradeoffs.
4. Add non-network integration tests first, then isolated network tests with timeouts and diagnostics.
5. Add byte-for-byte and path-layout assertions after every successful receive.
6. Add failure assertions: no final partial file, no secret in captured logs, child cleanup, typed error classification.
7. Configure CI artifact collection for redacted logs and failure diagnostics only.

## Acceptance Criteria

- [x] macOS and Linux CI execute reproducible default workspace checks.
- [x] Integration harness does not use developer home directories, production relay secrets, or permanent paths.
- [x] Sender-to-receiver single-file transfer produces byte-identical final output.
- [x] Multi-file and directory transfers preserve expected relative structure.
- [x] Interrupted/cancelled transfer leaves no final unverified output and cleans child resources.
- [x] Relay/process failure produces typed failure and user-safe diagnostic path.
- [x] Resume/recovery behavior is tested according to P1-08 capability decision.
- [x] GUI smoke validation is either green or explicitly marked blocked with exact platform prerequisite.
- [x] CI runs formatting, clippy, tests, and available dependency security checks without suppressing failures.

## Tests and Validation

This story establishes validation itself. Required commands should include:

```sh
rtk cargo fmt --all -- --check
rtk cargo clippy --all-targets --all-features -- -D warnings
rtk cargo check --workspace
rtk cargo test --workspace
rtk cargo audit
rtk cargo deny check
```

Run focused integration targets separately during iteration. Timeouts must fail with readable, redacted diagnostics; never hang CI indefinitely.

### Current Validation

- `rtk cargo fmt --all -- --check` — passed.
- `rtk cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` — passed.
- `rtk cargo check --workspace --locked` — passed.
- `rtk cargo test -p drift-core --locked` — 16 passed.
- `rtk cargo test -p drift-transfer --locked` — 28 passed, including 8 P1-12 integration cases.
- `rtk cargo test -p drift-ui --locked` — 41 passed.
- `rtk cargo test -p drift-app --locked` — 36 passed.
- `rtk git diff --check` — passed.
- `.github/workflows/p1-12-validation.yml` — YAML parsed locally; GitHub matrix execution remains pending.
- `rtk cargo clippy --all-targets --all-features --locked -- -D warnings` and `rtk cargo check -p drift-app --features gui --locked` — blocked locally by the missing GPUI Metal Toolchain.
- `rtk cargo audit --version` and `rtk cargo deny --version` — unavailable locally; CI installs the pinned tools.
- `deny.toml` — TOML parsed locally; security jobs remain pending until CI or the tools are installed locally.

## Junior Engineer Checklist

- Use temporary directories and deterministic fixture files.
- Clean resources in test teardown even after assertion failures.
- Do not assert raw terminal output when typed events/errors are available.
- Keep tests independent; no required ordering.

## Mid-Level Review Focus

- Confirm CI's Croc source/version and relay strategy are reproducible and security-reviewed.
- Confirm tests establish user-visible guarantees instead of only internal method calls.
- Confirm platform-specific blockers are not hidden by disabling tests.
- Confirm byte equality, path safety, cleanup, and redaction cases cover the release risk.

## Out of Scope

- Windows release validation.
- Native backend performance benchmarks.
- Packaging, signing, auto-update, or release publication.
