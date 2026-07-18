# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Enforced one strict Runtime V2 final-decision schema, including typed evidence requests, and bounded contract repair to one retry.
- Added terminal no-progress detection and bounded provider retry/backoff with stable error classes and dead-letter observability.
- Closed the Runtime V2 safety-validation loop for read-only intent, `no_change`, immutable resource scope, TTL, evidence, snapshots, grants, and confirmations.
- Prevented autonomous Agent token-group transitions from bypassing administrator confirmation while retaining deterministic emergency failover.
- Corrected severe-event resource scopes and emergency goal deduplication without inventing account zero.
- Filtered invalid Telemetry targeted-reconcile accounts and stopped repeated automatic `applied_noop` mutation growth.
- Preserved Runtime V2 Goal/Step and Packet/hash provenance without writing legacy Agent runs.

### Changed

- Added additive SQLite columns for bounded goal attempts, provider error history, and Runtime V2 mutation, transition, policy, outcome, and audit-event provenance.
- Agent goal status now distinguishes clean completion, completion after retries, terminal failure, and dead-lettered failure.

## [1.0.0] - 2026-07-16

### Added

- Independent Go scheduler with an embedded Vue 3 operations console and SQLite state storage.
- Fifty-second account and channel-monitor reconciliation without modifying Sub2API source or schema.
- Two-minute ingestion of monitor history and real request outcomes, with dedicated retention windows.
- Evidence-aware health classification, adaptive load control, staged recovery, manual protection, and flap protection.
- Explicit bindings, automatic upstream matching, exclusion rules, conflict detection, and scheduler-owned pause tracking.
- NewAPI and Sub2 password-login adapters for balances, tokens, groups, and effective multipliers.
- Composable balance and channel locks with threshold hysteresis and account ownership checks.
- Confirmed main, backup, and emergency token-group policies with deterministic disaster recovery, cooldowns, rate limits, readback, and rollback handling.
- Persistent operations agent with immutable analysis packets, goals, steps, events, checkpoints, scheduled commands, memory, policy versions, action outcomes, and daily reports.
- Registered capability boundary for agent tools, precise administrator-command grants, idempotent external writes, and reconciliation of ambiguous results.
- Administrator-controlled agent freeze and all-automation freeze barriers.
- Thirty-minute deterministic observation guidance and a non-bypassable 24-hour, 40-analysis autonomy gate for the agent.
- Session authentication with strict cookies, CSRF and same-origin protection, login and mutation rate limiting, and secret redaction.
- AES-256-GCM encryption for upstream credentials and model-provider API keys using separate master keys.
- Docker multi-stage build, non-root read-only runtime image, loopback-only Compose publication, Caddy path proxy example, and health/readiness endpoints.

### Security

- Upstream management connections require HTTPS by default and reject credential-bearing cross-host redirects.
- The agent has no shell, filesystem, arbitrary SQL, arbitrary HTTP, or secret-management capability.
- Failed, stale, incomplete, or conflicting evidence freezes automated writes rather than reusing old state.

[Unreleased]: https://github.com/hua226529-ctrl/sub2api-account-scheduler/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/hua226529-ctrl/sub2api-account-scheduler/releases/tag/v1.0.0
