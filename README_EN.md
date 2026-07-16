# Sub2API Account Scheduler

[![CI](https://github.com/hua226529-ctrl/sub2api-account-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/hua226529-ctrl/sub2api-account-scheduler/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An independent account scheduler and AI-assisted operations console for Sub2API, NewAPI, and Sub2 upstreams.

[完整中文文档](README.md)

> [!WARNING]
> This service can change Sub2API account scheduling, load factors, and confirmed upstream token groups. Keep all automation in observation mode after the first deployment and review every proposed action before enabling control.

## What it does

- Polls Sub2API accounts and channel monitors every 50 seconds.
- Imports monitor history and real request outcomes every 2 minutes without sending active model probes.
- Classifies credential, infrastructure, capacity, model, semantic, and client failures before deciding whether to pause or reduce load.
- Queries NewAPI and Sub2 balances, tokens, groups, and multipliers every 10 minutes using encrypted account credentials.
- Coordinates confirmed main, backup, and emergency token groups only when an entire model pool is unavailable.
- Maintains independent channel, balance, cost, and manual locks so an account is restored only when every lock has cleared.
- Provides a persistent, tool-constrained operations agent with goals, events, checkpoints, scheduled commands, policy versions, outcome evaluation, and daily reports.

The scheduler does not modify the Sub2API source code or database schema. It communicates through Sub2API administration APIs and stores its own state in SQLite.

## Quick start

Requirements:

- A running Sub2API instance and a global administrator API key.
- Docker Engine and Docker Compose v2.
- An external Docker network shared with Sub2API.
- An HTTPS reverse proxy for production access.

```bash
git clone https://github.com/hua226529-ctrl/sub2api-account-scheduler.git
cd sub2api-account-scheduler

export SUB2API_DOCKER_NETWORK=sub2api_sub2api-network
cp .env.example .env

# Generate two different 32-byte keys.
openssl rand -base64 32
openssl rand -base64 32

# Edit .env, then protect it.
chmod 600 .env
mkdir -p data

SUB2API_DOCKER_NETWORK="$SUB2API_DOCKER_NETWORK" docker compose up -d --build
```

The process listens on `:8323` inside the container. Compose publishes it only as `127.0.0.1:8323` on the host. The included Caddy snippet serves it below `/scheduler/`; use a domain such as `https://api.example.com/scheduler/`.

Verify the service before opening the console:

```bash
curl --fail http://127.0.0.1:8323/healthz
curl --fail http://127.0.0.1:8323/readyz
```

Use `SUB2API_ADMIN_API_KEY` to sign in.

## Required secrets

- `SUB2API_ADMIN_API_KEY` is always required.
- `UPSTREAM_CREDENTIAL_KEY` encrypts NewAPI and Sub2 account credentials.
- `AGENT_CREDENTIAL_KEY` independently encrypts model provider API keys.

The two encryption keys must be different and must decode to exactly 32 bytes from Base64 or hexadecimal. Keep them unchanged and back them up with the SQLite database. Existing encrypted records cannot be recovered after a key is lost or replaced.

See the [Chinese documentation](README.md#环境变量) for the complete configuration reference.

## Safety model

- The administrator API key is read from the environment and is never stored in SQLite.
- Browser sessions use an `HttpOnly`, `Secure`, `SameSite=Strict` cookie; SQLite stores only the session token hash and CSRF state.
- Upstream and model credentials are encrypted with AES-256-GCM.
- Upstream connections require HTTPS by default and reject credential-bearing cross-host redirects.
- The agent cannot run shell commands, access the filesystem, issue arbitrary SQL or HTTP, or read secrets.
- Every agent action must use a registered capability with validation, idempotency, state readback, and audit records.
- Administrator-only switches can freeze the agent or all automation; the agent cannot clear or bypass them.

## Observation gates

Deterministic automation should remain in `DRY_RUN=true` for at least 30 minutes after first deployment.

The operations agent enters observation mode whenever it is first enabled, re-enabled, or moved to another model. Full autonomy is enabled only after all of the following are true:

- 24 continuous observation hours.
- At least 40 successful scheduled analyses.
- At least 95% simulated-action executability.
- Zero privilege violations.
- Zero structured-output errors.

The web interface cannot bypass this gate, and service restarts do not reset its progress.

`DRY_RUN` and the initial failure, recovery, manual-hold, and flap settings seed missing SQLite values only. After the database has been created, update these policies in the console; changing `.env` alone does not overwrite persisted settings.

## Development

Go 1.26.3 and Node.js 24 are required.

```bash
cd frontend
npm ci
npm test
npm run build
cd ..

go test -buildvcs=false -count=1 ./...
go vet ./...
go build -buildvcs=false ./cmd/scheduler
```

For contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
