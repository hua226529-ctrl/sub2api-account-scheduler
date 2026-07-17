# Upgrade to Core C

## Before upgrading

1. Stop the scheduler or otherwise stop writes.
2. Back up `scheduler.db` together with its WAL/SHM files, or copy the complete stopped `data` directory.
3. Back up `.env` separately with restrictive permissions.
4. Record the current image or binary version.

## New configuration

Generate an independent console secret and do not reuse the Sub2API key:

```bash
openssl rand -base64 32
```

Set:

```dotenv
SCHEDULER_ADMIN_SECRET=replace-with-independent-console-secret
ALLOW_LEGACY_ADMIN_KEY_LOGIN=false
TRUSTED_PROXY_CIDRS=127.0.0.1/32
```

`SUB2API_ADMIN_API_KEY` remains required but is used only by the server for Sub2API requests. `TRUSTED_PROXY_CIDRS` must contain the direct reverse-proxy socket peers. Leave it empty when accessing the scheduler directly; forwarded headers will then be ignored.

For one compatibility release only, installations that cannot rotate the browser credential immediately may set `ALLOW_LEGACY_ADMIN_KEY_LOGIN=true` and omit `SCHEDULER_ADMIN_SECRET`. This is explicit, logs a deprecation warning, and should be removed after users switch credentials.

## Database migration

Migrations extend existing tables in place. They preserve policies, goals, conversations, steps, checkpoints, scheduled commands, group transitions, overrides and mutation journals. Legacy `agent_runs` and `agent_tool_calls` remain historical read-only tables and are not dropped.

The first startup may take longer while SQLite adds columns and indexes. A failed startup can be retried with the same binary. Do not manually delete migration columns or old tables.

## Operating modes after upgrade

Review and explicitly set:

- Scheduler: start with `observe`, then move to `control` after reviewing account decisions.
- Optimizer: `disabled`, `observe`, `propose`, or low-risk `auto`.
- Operator: `disabled`, confirmation-required `confirm`, or exact-grant `direct`.
- Failover: start with `observe`; use `control` only after all three-tier policies are confirmed.

Failover defaults to one group mutation per cycle. Existing active deterministic policies continue without a model.

## Verification

```bash
docker compose up -d --build
docker compose logs scheduler
curl --fail http://127.0.0.1:8323/healthz
curl --fail http://127.0.0.1:8323/readyz
```

Verify console login with `SCHEDULER_ADMIN_SECRET`, confirm the four modes, inspect policy proposals, and run one read-only chat query. Verify that arbitrary token group selection is absent and that only confirmed tier transitions are available.

## Rollback

Stop the new binary before rollback. Restore both the previous binary/image and the pre-upgrade SQLite backup. Do not run an older binary against a database after new writes have been accepted unless that older release is documented as schema-compatible.
