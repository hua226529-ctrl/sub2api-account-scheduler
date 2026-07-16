# Security Policy

## Supported versions

Security fixes are provided for the latest released version on the `main` branch.

| Version | Supported |
| --- | --- |
| 1.0.x | Yes |
| Earlier versions | No |

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for this repository:

<https://github.com/hua226529-ctrl/sub2api-account-scheduler/security/advisories/new>

Do not open a public Issue or Discussion for an unpatched vulnerability. Do not include real administrator keys, upstream passwords, model API keys, session cookies, database files, request payloads, or production addresses in any public report.

A useful report includes:

- The affected version or commit.
- The impacted component and deployment shape.
- Reproduction steps using synthetic credentials and data.
- The expected and observed security boundary.
- The likely impact and any known workaround.

Reports are reviewed on a best-effort basis. Please allow time for validation and coordinated remediation before public disclosure.

## If a secret was exposed

Treat an exposed value as compromised even if it was quickly removed from Git history.

1. Revoke and regenerate the Sub2API administrator key.
2. Revoke exposed upstream or model credentials at their providers.
3. Replace leaked session cookies by restarting the service or removing active sessions.
4. If an encryption master key was exposed, rotate all credentials encrypted by that key and recreate the affected configuration with a new master key.
5. Review scheduler audit events and upstream access logs for unexpected actions.

Rewriting Git history is not a substitute for credential rotation.

## Security boundaries

The project is designed around the following boundaries:

- The scheduler modifies Sub2API only through documented administration APIs.
- The Sub2API administrator key is read from the environment and is not stored in SQLite.
- Upstream passwords and model API keys are encrypted independently with AES-256-GCM.
- Browser sessions use strict cookies, hashed server-side tokens, CSRF validation, same-origin checks, and request rate limits.
- Upstream connections require HTTPS by default and reject credential-bearing cross-host redirects.
- The operations agent cannot execute shell commands, access the filesystem, issue arbitrary SQL or HTTP requests, or retrieve secrets.
- Agent actions are limited to registered capabilities with validation, idempotency, readback, and audit records.
- Agent and all-automation freeze switches are administrator-controlled platform boundaries.

These controls do not protect a host that is already compromised or an attacker who can read the process environment, SQLite database, and encryption keys together.

## Deployment hardening

- Keep the service bound to loopback and expose it only through an authenticated HTTPS deployment path.
- Keep `COOKIE_SECURE=true` in production.
- Keep `ALLOW_INSECURE_UPSTREAMS=false` in production.
- Store `.env` with mode `600` and restrict access to backups.
- Use different values for `UPSTREAM_CREDENTIAL_KEY` and `AGENT_CREDENTIAL_KEY`.
- Start deterministic automation in dry-run mode and allow the full agent observation gate to complete.
- Regularly review audit events, failed login attempts, automated group transitions, and policy versions.
- Update the scheduler, Sub2API, NewAPI/Sub2 upstreams, container runtime, and reverse proxy promptly.

## Out of scope

The project does not claim to secure malicious or compromised upstream services, Sub2API installations, model providers, reverse proxies, hosts, or administrator browsers. Availability and pricing data returned by upstream APIs are treated as external evidence and may be incomplete or incorrect.
