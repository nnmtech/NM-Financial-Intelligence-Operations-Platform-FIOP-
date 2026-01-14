# Scripts (Usage)

This folder contains reviewed scaffolds for autonomous tasks. Each script is
safe by default (dry-run or logging-only) and designed to be wired into CI,
cron, containers, or systemd. Use the `scripts/config.toml.example` as a
reference for environment variables and options.

Quick run examples
- Run the ingest worker locally (poll every 60s):

```bash
FOIP_POLL_INTERVAL=60 python scripts/ingest_worker.py
```

- Create a backup (dry-run; no encryption):

```bash
python scripts/backup_rotate.py
```

- Create an encrypted backup (passphrase via env):

```bash
export FOIP_DO_ENCRYPT=yes
export FOIP_BACKUP_PASSPHRASE="$(your-secret-retrieval)"
python scripts/backup_rotate.py
```

- Run the health & gate checks (prints JSON summary):

```bash
python scripts/health_gate.py
```

- Build the staging artifact and compute SHA256 (no upload):

```bash
python scripts/staging_publish.py
cat dist/foip-staging.tar.gz.sha256
```

- Run registry reconciliation (dry-run):

```bash
python scripts/registry_reconciler.py
```

- Run deterministic feature extraction (dry-run):

```bash
python scripts/feature_extract.py --out out/features
```

- Run auto-remediation check (suggests fixes; dry-run):

```bash
python scripts/auto_remediate.py
```

- Start the webhook listener (dev only):

```bash
FOIP_WEBHOOK_SECRET=sekrit python scripts/webhook_listener.py
```

Guidelines & best practices
- Secrets: do not store passphrases or tokens in the repo. Use a secret
  manager or CI secret storage and inject via environment variables.
- CI: run `scripts/health_gate.py` as a gating job in CI to fail merges on
  type/test regressions. Keep auto-remediation disabled in CI unless you
  trust the bot account and workflow.
- Backups: encrypt client-side before remote upload; verify checksums after
  upload. Rotation is supported by `scripts/backup_rotate.py`.
- Observability: wire structured logs and metrics (Prometheus, Sentry) for
  production deployment of any long-running script.

Tests
- Each scaffold has a minimal test under `tests/` (import/API checks). Run
  the full test suite with:

```bash
PYTHONPATH=src pytest -q --ignore=backup_v1
```

If you want, I can generate a consolidated `README.md` at the repo root
describing package layout, CI integration, and deployment examples.
