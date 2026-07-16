# Contributing

Thank you for improving Sub2API Account Scheduler. Contributions should preserve its central rule: uncertain data freezes automatic writes instead of guessing.

## Before opening an issue

- Search existing Issues and the changelog.
- Remove production domains, account names, IDs, request bodies, and credentials from examples.
- Use GitHub private vulnerability reporting for security defects; see [SECURITY.md](SECURITY.md).
- For behavior changes, describe the evidence, expected decision, and expected audit event.

## Development environment

- Go 1.26.3.
- Node.js 24 and npm.
- Docker Engine and Docker Compose v2 for container tests.

Fork the repository and create a focused branch from `main`:

```bash
git clone https://github.com/your-account/sub2api-account-scheduler.git
cd sub2api-account-scheduler
git checkout -b fix/short-description
```

Do not use real production data in local fixtures. The mock Sub2API under `cmd/mocksub2api` is the preferred integration target.

## Build and test

Build the frontend before compiling the final Go binary because the generated assets are embedded into the server:

```bash
cd frontend
npm ci
npm test
npm run build
cd ..

test -z "$(gofmt -l cmd internal)"
go test -buildvcs=false -count=1 ./...
go vet ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -buildvcs=false -trimpath -o scheduler-linux-amd64 ./cmd/scheduler
```

On Windows PowerShell, inspect `gofmt -l cmd internal` and ensure it produces no file names. Run `gofmt -w` only on files you intentionally changed.

For container changes:

```bash
docker build -t sub2api-account-scheduler:test .
```

## Change requirements

- Keep business decisions deterministic and covered by table-driven tests where possible.
- Count only distinct upstream observations; tests must cover duplicate reads.
- Any external write must define validation, idempotency, state readback, audit behavior, and ambiguous-result handling.
- New agent tools must be registered capabilities with a narrow parameter schema and explicit resource scope.
- Never expose password, key, token, cookie, raw request content, or unredacted upstream errors to the model or public API.
- Preserve the distinction between monitor evidence and real request evidence.
- Database migrations must be forward-compatible, transactional where applicable, and tested against an older schema.
- UI changes must include loading, empty, error, disabled, confirmation, and narrow-screen states where relevant.
- Keep generated `internal/webui/dist`, `frontend/node_modules`, binaries, databases, logs, `.env`, and release archives out of commits.

## Pull requests

A pull request should contain one coherent change and include:

- A concise problem statement and implementation summary.
- Tests that fail before the change and pass afterward.
- Migration or compatibility notes when storage or upstream APIs change.
- Screenshots for meaningful interface changes, using synthetic data.
- Documentation and changelog updates for user-visible behavior.

Before submitting, confirm:

- Frontend tests and build pass.
- `gofmt` reports no changes.
- Go tests and `go vet` pass.
- No runtime artifacts or secrets are tracked.
- New automatic actions remain blocked by dry-run, observation, freeze, and permission boundaries as designed.

## Commit messages

Use short imperative messages. Conventional prefixes are encouraged:

```text
feat: add pool-level policy override
fix: freeze writes when telemetry is stale
docs: clarify encryption key backup
test: cover duplicate monitor timestamps
```

## License

By submitting a contribution, you agree that it is licensed under the repository's [Apache License 2.0](LICENSE).
