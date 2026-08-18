---
name: "Drift Security"
description: "Use when changing cryptography, authentication, transfer codes, key management, capability tokens, file paths, symlinks, relay authorization, subprocess arguments, secret storage, logging, or security-sensitive error handling."
---

# Security Rules

Security-sensitive changes require additional review. Passing tests is necessary but does not prove cryptographic security.

## Cryptography

Never:

- invent an encryption algorithm, PAKE, key exchange, or nonce scheme;
- use a short transfer code directly as an encryption key;
- weaken authentication to simplify implementation;
- disable certificate or security verification without explicit written justification;
- port or rewrite croc cryptography for the MVP;
- log or persist keys, passwords, PAKE material, capabilities, or authentication secrets.

Always:

- use established, reviewed primitives and implementations;
- use cryptographically secure randomness;
- separate authentication/key establishment from data encryption;
- authenticate encrypted chunks and bind transfer, file, and chunk context;
- use explicit nonce management;
- verify received content before publishing it;
- perform a security review before native protocol or key-management work.

For MVP transfer security, reuse croc through `CrocBackend`. Native protocol candidates such as PAKE, HKDF, and AEAD require a separate design and review; do not freeze them by convenience.

## Untrusted Paths

Every received path is attacker-controlled. Reject:

- `..` traversal;
- absolute paths;
- Windows drive prefixes;
- UNC paths;
- empty or invalid components;
- symlink escapes;
- unintended overwrites.

Write received content to a temporary or partial file. Verify size and digest, then atomically rename into the selected destination. Do not expose a partial file as the final file.

## Secrets and Logs

Never log:

```text
transfer codes
passwords
keys
PAKE material
capability tokens
raw encrypted payloads
unnecessarily complete sensitive paths
```

Use transfer IDs for correlation. Redact secrets in `Debug`, errors, process arguments, metrics, persisted state, and user-visible diagnostics. Prefer structured `tracing` fields over string interpolation.

## Process and Network Boundaries

- Pass subprocess arguments as structured values; never build shell commands from untrusted input.
- Keep relay traffic opaque and encrypted end-to-end.
- Do not accept disabled certificate verification or plaintext fallback as a convenience.
- Keep low-level errors separate from user-facing messages so OS details do not leak sensitive state.

## Review Checklist

Before merging a security-sensitive change, verify:

1. Threat model and trust boundaries are documented.
2. Secrets are absent from logs, debug output, and unnecessary persistence.
3. Input validation covers malformed, hostile, and platform-specific values.
4. Temporary-file and atomic-rename behavior is preserved.
5. Integrity and authentication failures reject final output.
6. Dependencies and versions have been checked for advisories.
7. Tests cover failure behavior, but no test result is presented as proof of cryptographic security.
8. Additional security review is recorded for protocol, auth, key, capability, path, relay, or secret-storage changes.
