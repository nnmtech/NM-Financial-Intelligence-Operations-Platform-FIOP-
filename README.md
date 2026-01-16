# FOIP — Package & Automation Summary

This repository contains the FOIP Python package (authoritative code under `src/foip`) and a set of small, reviewed scaffolds for autonomous tasks used during development, staging, and operations.

Key components
- Package: `src/foip` — typed package with `py.typed`, domain models, ingestion, registry, and storage abstractions.
- Tests & typing: `pytest` tests and `mypy` configuration; package is kept green by running typechecks and tests.
- Scripts: a `scripts/` folder with safe, dry-run-first scaffolds for common automation tasks.

Provided automation scaffolds (dry-run / safe by default)
- `scripts/ingest_worker.py` — periodic async SEC ingestor (polls EDGAR, calls package ingestor).  
- `scripts/backup_rotate.py` — create workspace tar, optional OpenSSL encryption, rotate encrypted archives.  
- `scripts/health_gate.py` — run `mypy`, `pytest`, smoke import; prints JSON summary for CI gates.  
- `scripts/staging_publish.py` — build `dist/foip-staging.tar.gz`, compute SHA256, optional upload (S3/file).  
- `scripts/registry_reconciler.py` — compare external vendor IDs vs local registry and report mismatches.  
- `scripts/feature_extract.py` — deterministic ML feature extractor for filings (writes JSON feature records).  
- `scripts/auto_remediate.py` — detect failing tests and suggest (or optionally attempt) small autofixes (format/lint).  
- `scripts/webhook_listener.py` — aiohttp webhook receiver that schedules background handlers (ingest/event triggers).

How to run checks locally
```bash
PYTHONPATH=src python -m mypy src/foip
PYTHONPATH=src pytest -q --ignore=backup_v1
```

Script usage notes
- All scripts are non-destructive by default; enable behavior via environment variables (see `scripts/config.toml.example`).  
- Secrets (passphrases, tokens) must be provided via a secret manager or CI secret injection — never commit secrets to the repo.  

Release & backups
- A verified, encrypted backup `FOIP_backup_v1.tar.gz.enc` and its checksum are present in the working tree; encrypted backups are verified against the recorded checksum before plaintext artifacts are removed.

VCS policy
- A cleanup branch and annotated tag for the cleaned package state exist; no further commits or pushes are performed without explicit approval.

If you want, I can:
- display any scaffold file inline for review, or
- create a repository-level CI workflow (GitHub Actions) that runs `health_gate` and stages artifacts.
FOIP recommended stack and best-practices

Local Docker build & run (with file-backed secrets)
-----------------------------------------------

Build the image locally and run `docker-compose` using file-backed secrets:

1. Build the image:

```bash
docker build -t foip:latest -f Dockerfile .
```

2. Create a temporary `secrets/` directory with your secret files (mode 600):

```bash
mkdir -p secrets
printf "%s" "<POSTGRES_PASSWORD>" > secrets/postgres_password
printf "%s" "<SEC_API_KEY>" > secrets/sec_api_key
chmod 600 secrets/*
```

3. Start services with docker-compose (the compose file uses file-backed secrets):

```bash
docker compose up --build -d
```

4. When finished, teardown and remove secrets:

```bash
docker compose down
rm -rf secrets
```

Notes:
- For CI / production, write these secrets from your secret manager into temporary files (the GitHub Actions workflow `deploy-with-secrets.yml` demonstrates this pattern).
- Do NOT commit the `secrets/` directory or `.env` into the repository.


This workspace follows the project's documented best-practices:

- Use `attrs` for declarative data models (lightweight, explicit).
- Use `msgspec` for fast, predictable serialization when needed.
- Use `tenacity` for robust retry handling with exponential backoff.
- Use `diskcache` for local disk-backed caching; avoid overusing remote caches for local dev.
- Use `anyio` to write portable async code without locking to asyncio/trio.
- Use `watchdog` for file-system event handling where appropriate.
- Use `structlog` for structured JSON-first logging.
- Use `schedule` for simple process-local scheduling when cron is overkill.
- Use `pluggy` for plugin/hook based extensibility.

Files added/updated:
- `shared_metadata_schema.py.py`: refactored to `attrs` models and central enums.
- `foip_infra.py`: helpers for logging, serialization adapters, retry/cache examples.
- `requirements.txt`: pinned dependency list for the recommended stack.

Developer notes:
- Prefer enums over freeform status strings.
- Normalize datetimes to UTC and keep them timezone-aware.
- Add adapters where interoperating with `pydantic`/`msgspec` is necessary.
- When in doubt, prefer explicit conversions and small, well-tested adapters.

Agent Resource Lifecycle (Orchestrator)
--------------------------------------

- Convention: Agent implementations may expose resource objects (HTTP clients, DB clients,
	connectors, ingestors) as attributes on the `Agent` or `Engine` instance. If a resource
	exposes `initialize()` and `close()` coroutines, the `MAKEROrchestrator` will attempt to
	initialize these resources before executing the agent and close them after execution.

- Heuristics used by the orchestrator:
	- Look for instance attributes whose value exposes a callable `initialize()` method.
	- Initialize the resource when it appears uninitialized (e.g., has `initialized`/`is_initialized` false, or a `session` attribute that is `None`).
	- Track initialized resources and call `close()` on them in `finally` blocks to ensure cleanup.

- Best practice (recommended):
	- Implement explicit lifecycle methods on resources:

		```python
		class MyConnector:
				def __init__(self):
						self.session = None
						self.initialized = False

				async def initialize(self):
						self.session = aiohttp.ClientSession()
						self.initialized = True

				async def close(self):
						if self.session:
								await self.session.close()
						self.initialized = False
		```

	- Optionally expose a short `agent.resources` list if you need explicit, ordered control
		over which resources are initialized and when (the orchestrator will call `initialize()`/`close()` on each resource it finds).

- Notes:
	- The orchestrator's heuristics aim to be convenient for common cases; if you require
		deterministic ordering or complex startup/shutdown sequencing, implement an explicit
		`initialize()`/`close()` on the agent/engine itself and let the orchestrator call it.

Linting
-------

- A lightweight lint rule is provided to enforce timezone-aware datetimes: avoid `datetime.utcnow()`.
	A pre-commit hook runs `scripts/check_no_datetime_utcnow.py` and will fail commits if occurrences
	are found. Prefer `datetime.now(timezone.utc)` or the project's `_ensure_utc()` helper when
	normalizing timestamps.

- Another checker enforces serialization best-practices for registry writes: when calling
	`entity_registry.register_entity(entity_id, payload)`, prefer `attr.asdict(payload)` so
	that attrs-based models are reliably serialized. The hook runs `scripts/check_register_entity_attr_asdict.py`.
