FOIP recommended stack and best-practices

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
