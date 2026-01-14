"""
FIOP Platform - Main Entry Point
Phase 1: Foundation Implementation
"""

import asyncio
import signal
from pathlib import Path
from typing import Optional, Any
from datetime import datetime, timezone

import structlog
import os
from aiohttp import web
import redis.asyncio as aioredis
from typing import Tuple
from prometheus_client import Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from tenacity import retry, stop_after_attempt, wait_random_exponential
import time
from uuid import uuid4

# OpenTelemetry tracing
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from contextlib import asynccontextmanager
from typing import Any

def format_trace_id(trace_id_int: int) -> str:
    try:
        return format(trace_id_int, '032x')
    except Exception:
        return ""
from contextlib import asynccontextmanager

try:
    from fastapi import FastAPI
    import uvicorn
except Exception:
    FastAPI = None
    uvicorn = None

# Prefer local absolute imports; provide minimal fallbacks for tests/top-level runs
try:
    from foip.finscan_maker_core import MAKEROrchestrator
except Exception:
    MAKEROrchestrator: Any = object

try:
    from foip.finscan_sec_ingestor import SECIngestionAgent, SECIngestor
except Exception:
    SECIngestor: Any = type("SECIngestor", (), {})
    SECIngestionAgent: Any = type("SECIngestionAgent", (), {})

try:
    from foip.operations_reconciliation_core import (
        BankReconciliationAgent,
        BankReconciliationEngine,
    )
except Exception:
    BankReconciliationAgent: Any = object
    BankReconciliationEngine: Any = object

try:
    from foip.operations_reconciliation_matchers import FuzzyTransactionMatcher
except Exception:
    FuzzyTransactionMatcher: Any = type("FuzzyTransactionMatcher", (), {})

try:
    from foip.shared_cost_calculator import CostCalculator
except Exception:
    CostCalculator: Any = type("CostCalculator", (), {})

try:
    from foip.shared_metadata_registry import EntityRegistry
except Exception:
    EntityRegistry: Any = object

try:
    from foip.storage.postgres import DatabaseConnection
except Exception:
    DatabaseConnection: Any = object

try:
    from foip.shared_storage_worm import WORMStorage
except Exception:
    WORMStorage: Any = object

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()


class FIOPPlatform:
    """Main FIOP Platform Application"""

    def __init__(self, config: dict):
        self.config = config
        self.running = False

        # Initialize shared components
        self.db = DatabaseConnection(config["database_url"])
        self.entity_registry = EntityRegistry(self.db)
        self.worm_storage = WORMStorage(Path(config["worm_storage_path"]))
        self.cost_calculator = CostCalculator(config.get("cost_limits", {}))

        # Initialize MAKER orchestrator
        self.maker = MAKEROrchestrator(
            worm_storage=self.worm_storage,
            cost_calculator=self.cost_calculator,
            invariant_checker=None,  # Would be implemented
        )

        # Initialize agents
        self._initialize_agents()

        # Readiness cache
        self._ready_cache: dict = {"ok": False, "db": False, "redis": False}
        self._ready_cache_at: float | None = None
        self._ready_cache_ttl = int(os.environ.get("READINESS_CACHE_TTL", "15"))
        self._ready_lock = asyncio.Lock()

        # Redis client (created at startup)
        self._redis: aioredis.Redis | None = None

        # Prometheus metrics with service/instance labels
        self._service = os.environ.get("SERVICE_NAME", "foip")
        self._instance = os.environ.get("INSTANCE_ID") or __import__("socket").gethostname()

        self._readiness_hist = Histogram(
            "readiness_probe_duration_seconds",
            "Duration of readiness probe in seconds",
            labelnames=["service", "instance"],
        )
        self._readiness_up = Gauge(
            "readiness_up", "Readiness up (1) or down (0)", labelnames=["service", "instance"]
        )
        self._db_latency_hist = Histogram(
            "db_latency_seconds", "Database query latency in seconds", labelnames=["service", "instance"]
        )
        self._redis_latency_hist = Histogram(
            "redis_latency_seconds", "Redis ping latency in seconds", labelnames=["service", "instance"]
        )
        from prometheus_client import Counter

        self._db_errors = Counter("db_errors_total", "Total DB errors", labelnames=["service", "instance"])
        self._redis_errors = Counter("redis_errors_total", "Total Redis errors", labelnames=["service", "instance"])

        # Setup OpenTelemetry tracer provider
        try:
            resource = Resource.create({"service.name": self._service})
            provider = TracerProvider(resource=resource)
            # Prefer OTLP exporter if endpoint present
            otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
            if otlp_endpoint:
                exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
            else:
                exporter = ConsoleSpanExporter()

            provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(provider)
            self._tracer = trace.get_tracer(__name__)
        except Exception:
            self._tracer = trace.get_tracer(__name__)

        log.info("FIOP Platform initialized")

    def _initialize_agents(self):
        """Initialize all agents for Phase 1"""
        # SEC Ingestion Agent
        sec_ingestor = SECIngestor(
            api_key=self.config["sec_api_key"], entity_registry=self.entity_registry
        )
        sec_agent = SECIngestionAgent(sec_ingestor)
        self.maker.register_agent(sec_agent)

        # Bank Reconciliation Agent
        bank_connector = MockBankConnector()  # Mock for MVP
        transaction_matcher = FuzzyTransactionMatcher()
        reconciliation_engine = BankReconciliationEngine(
            entity_registry=self.entity_registry,
            bank_connector=bank_connector,
            transaction_matcher=transaction_matcher,
        )
        reconciliation_agent = BankReconciliationAgent(reconciliation_engine)
        self.maker.register_agent(reconciliation_agent)

        log.info("Agents initialized", count=len(self.maker.agents))

    async def start(self):
        """Start the FIOP platform"""
        if self.running:
            log.warning("Platform already running")
            return

        self.running = True

        try:
            # Connect / initialize database pool
            try:
                if hasattr(self.db, "init_pool"):
                    await self.db.init_pool()
                    log.info("Database pool initialized via init_pool()")
                elif hasattr(self.db, "create_pool"):
                    await self.db.create_pool()
                    log.info("Database pool created via create_pool()")
                elif hasattr(self.db, "connect_pool"):
                    await self.db.connect_pool()
                    log.info("Database pool created via connect_pool()")
                else:
                    # fallback to generic connect
                    await self.db.connect()
                    log.info("Database connected via connect()")
            except Exception:
                log.exception("Database pool initialization failed; attempting generic connect()")
                try:
                    await self.db.connect()
                except Exception:
                    log.exception("Database generic connect() also failed")
                    raise

            # Run initial setup
            await self._run_initial_setup()

            # Start scheduled runs
            asyncio.create_task(self._run_scheduled_tasks())

            # Start API server (if configured)
            if self.config.get("enable_api", False):
                asyncio.create_task(self._start_api_server())

            log.info("FIOP Platform started successfully")

            # Keep running until stopped
            while self.running:
                await asyncio.sleep(1)

        except Exception as e:
            log.error("Platform startup failed", error=str(e))
            raise

    async def stop(self):
        """Stop the FIOP platform gracefully"""
        self.running = False

        # Close database connection
        await self.db.close()

        # Stop API server if running (aiohttp runner)
        if getattr(self, "_runner", None):
            try:
                await self._runner.cleanup()
            except Exception:
                log.exception("Error while cleaning up API runner")

        # Stop uvicorn server if running
        if getattr(self, "_uvicorn_server", None):
            try:
                # signal server to stop
                self._uvicorn_server.should_exit = True
                if getattr(self, "_api_task", None):
                    await self._api_task
            except Exception:
                log.exception("Error while stopping uvicorn server")

        # Close redis client if created
        if getattr(self, "_redis", None):
            try:
                await self._redis.close()
            except Exception:
                pass
            try:
                await self._redis.wait_closed()
            except Exception:
                pass

        # Close SEC ingestor session
        # (Would need to track and close all resources)

        log.info("FIOP Platform stopped")

    async def _run_initial_setup(self):
        """Run initial setup tasks"""
        log.info("Running initial setup")

        # Create database schema
        await self._create_database_schema()

        # Run initial data ingestion for configured companies
        if self.config.get("initial_companies"):
            await self._run_initial_ingestion()

        log.info("Initial setup completed")

    async def _create_database_schema(self):
        """Create database tables for Phase 1"""
        async with self.db.transaction() as conn:
            # Entities table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entities (
                    entity_type VARCHAR(50) NOT NULL,
                    identifier VARCHAR(255) NOT NULL,
                    source VARCHAR(50) NOT NULL,
                    first_seen TIMESTAMP NOT NULL,
                    last_seen TIMESTAMP NOT NULL,
                    metadata JSONB,
                    PRIMARY KEY (entity_type, identifier, source)
                )
            """
            )

            # Entity relationships table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS entity_relationships (
                    relationship_id UUID PRIMARY KEY,
                    source_entity_type VARCHAR(50) NOT NULL,
                    source_identifier VARCHAR(255) NOT NULL,
                    source_source VARCHAR(50) NOT NULL,
                    target_entity_type VARCHAR(50) NOT NULL,
                    target_identifier VARCHAR(255) NOT NULL,
                    target_source VARCHAR(50) NOT NULL,
                    relationship_type VARCHAR(100) NOT NULL,
                    confidence FLOAT DEFAULT 1.0,
                    established_by_run UUID NOT NULL,
                    established_at TIMESTAMP NOT NULL,
                    metadata JSONB,
                    FOREIGN KEY (source_entity_type, source_identifier, source_source) 
                        REFERENCES entities(entity_type, identifier, source),
                    FOREIGN KEY (target_entity_type, target_identifier, target_source)
                        REFERENCES entities(entity_type, identifier, source)
                )
            """
            )

            # Cost tracking table
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_costs (
                    run_id UUID PRIMARY KEY,
                    run_type VARCHAR(100) NOT NULL,
                    estimated_cost DECIMAL(10, 4) NOT NULL,
                    actual_cost DECIMAL(10, 4),
                    cost_breakdown JSONB,
                    recorded_at TIMESTAMP NOT NULL
                )
            """
            )

            log.info("Database schema created")

    async def _run_initial_ingestion(self):
        """Run initial SEC ingestion for configured companies"""
        log.info("Running initial SEC ingestion")

        run_id = await self.maker.start_run(
            run_type="sec_ingestion",
            trigger="initial_setup",
            parameters={
                "companies": self.config["initial_companies"],
                "forms": ["10-K", "10-Q", "8-K"],
            },
        )

        # Execute the run
        context = await self.maker.execute_run(run_id)

        log.info(
            "Initial ingestion completed", run_id=run_id, errors=len(context.errors)
        )

    async def _run_scheduled_tasks(self):
        """Run scheduled tasks based on configuration"""
        while self.running:
            # Check for scheduled runs
            current_hour = datetime.now(timezone.utc).hour

            # Daily SEC ingestion at 2 AM UTC
            if current_hour == 2:
                await self._run_daily_sec_ingestion()

            # Hourly bank reconciliation
            if current_hour % 1 == 0:  # Every hour
                await self._run_hourly_reconciliation()

            # Wait for next check
            await asyncio.sleep(60)  # Check every minute

    async def _run_daily_sec_ingestion(self):
        """Run daily SEC ingestion"""
        log.info("Starting daily SEC ingestion")

        run_id = await self.maker.start_run(
            run_type="sec_ingestion",
            trigger="scheduled_daily",
            parameters={
                "companies": self.config.get("monitored_companies", []),
                "forms": ["8-K"],  # Just check for current reports
            },
        )

        await self.maker.execute_run(run_id)

    async def _run_hourly_reconciliation(self):
        """Run hourly bank reconciliation"""
        log.info("Starting hourly bank reconciliation")

        run_id = await self.maker.start_run(
            run_type="bank_reconciliation",
            trigger="scheduled_hourly",
            parameters={"accounts": self.config.get("bank_accounts", [])},
        )

        await self.maker.execute_run(run_id)

    async def _start_api_server(self):
        """Start REST API server"""
        # Prefer FastAPI + Uvicorn when available (better integration with ASGI tooling),
        # otherwise fall back to the lightweight aiohttp server implemented earlier.
        port = int(os.environ.get("PORT", os.environ.get("HTTP_PORT", 8000)))
        host = os.environ.get("HOST", "0.0.0.0")

        async def _check_readiness(self) -> Tuple[dict, int]:
            """Cached readiness check that verifies DB and Redis quickly.

            Returns (payload_dict, status_code).
            """

            now = asyncio.get_event_loop().time()
            if self._ready_cache_at and (now - self._ready_cache_at) < self._ready_cache_ttl:
                return (self._ready_cache, 200 if self._ready_cache["ok"] else 503)

            async with self._ready_lock:
                # check again inside lock
                now = asyncio.get_event_loop().time()
                if self._ready_cache_at and (now - self._ready_cache_at) < self._ready_cache_ttl:
                    return (self._ready_cache, 200 if self._ready_cache["ok"] else 503)

                db_ok = False
                redis_ok = False

                # start timing the checks
                start_time = time.perf_counter()

                # DB check: prefer using a pool or lightweight fetch when available
                db_start = time.perf_counter()
                try:
                    # try common async DB client interfaces in order of preference
                    if hasattr(self.db, "fetchval"):
                        # libraries like databases or asyncpg provide fetchval
                        await asyncio.wait_for(self.db.fetchval("SELECT 1"), timeout=3)
                    elif hasattr(self.db, "execute") and hasattr(self.db, "fetchone"):
                        # generic execute/fetchone
                        await asyncio.wait_for(self.db.fetchone("SELECT 1"), timeout=3)
                    elif hasattr(self.db, "acquire"):
                        async with self.db.acquire() as conn:
                            if hasattr(conn, "fetchval"):
                                await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=3)
                            else:
                                await asyncio.wait_for(conn.execute("SELECT 1"), timeout=3)
                    else:
                        # fallback to transaction() pattern used earlier
                        async def _db_check():
                            async with self.db.transaction() as conn:
                                await conn.execute("SELECT 1")

                        await asyncio.wait_for(_db_check(), timeout=3)

                    db_ok = True
                except Exception:
                    db_ok = False
                finally:
                    try:
                        db_duration = time.perf_counter() - db_start
                        try:
                            self._db_latency_hist.labels(self._service, self._instance).observe(db_duration)
                        except Exception:
                            pass
                    except Exception:
                        pass

                # Redis check using redis.asyncio (supports AUTH)
                try:
                    # Prefer explicit REDIS_URL if provided, otherwise host/port/password
                    redis_url = os.environ.get("REDIS_URL")
                    if not redis_url:
                        redis_host = os.environ.get("REDIS_HOST", "redis")
                        redis_port = int(os.environ.get("REDIS_PORT", "6379"))
                        redis_password = None
                        # Support password file pattern
                        pwd_file = os.environ.get("REDIS_PASSWORD_FILE")
                        if pwd_file and os.path.exists(pwd_file):
                            try:
                                redis_password = Path(pwd_file).read_text().strip()
                            except Exception:
                                redis_password = None

                        if redis_password:
                            redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/0"
                        else:
                            redis_url = f"redis://{redis_host}:{redis_port}/0"

                    # reuse a single client instance for the app
                    if not self._redis:
                        self._redis = aioredis.from_url(redis_url, socket_connect_timeout=2)

                    # use tenacity-backed ping with exponential backoff and jitter
                    @retry(reraise=True, stop=stop_after_attempt(5), wait=wait_random_exponential(multiplier=0.5, max=5))
                    async def _ping():
                        return await asyncio.wait_for(self._redis.ping(), timeout=2)

                    try:
                        pong = await _ping()
                        redis_ok = bool(pong)
                    except Exception:
                        # final failure after retries
                        redis_ok = False
                except Exception:
                    redis_ok = False


                ok = db_ok and redis_ok

                payload = {"status": "ready" if ok else "not_ready", "db": db_ok, "redis": redis_ok, "ok": ok}
                # metrics: observe duration and set up/down gauge
                try:
                    duration = time.perf_counter() - start_time
                    self._readiness_hist.observe(duration)
                    self._readiness_up.set(1 if ok else 0)
                except Exception:
                    pass

                self._ready_cache = payload
                self._ready_cache_at = asyncio.get_event_loop().time()
                return (payload, 200 if ok else 503)

        # initialize (or attempt to) long-lived redis client with light reconnect/backoff
        async def _init_redis_client():
            if self._redis:
                return
            try:
                redis_url = os.environ.get("REDIS_URL")
                if not redis_url:
                    redis_host = os.environ.get("REDIS_HOST", "redis")
                    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
                    redis_password = None
                    pwd_file = os.environ.get("REDIS_PASSWORD_FILE")
                    if pwd_file and os.path.exists(pwd_file):
                        try:
                            redis_password = Path(pwd_file).read_text().strip()
                        except Exception:
                            redis_password = None

                    if redis_password:
                        redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/0"
                    else:
                        redis_url = f"redis://{redis_host}:{redis_port}/0"

                # create client
                self._redis = aioredis.from_url(redis_url, socket_connect_timeout=2)

                # try ping with exponential backoff (non-blocking short attempts)
                attempts = 0
                delay = 0.5
                while attempts < 4:
                    try:
                        pong = await asyncio.wait_for(self._redis.ping(), timeout=2)
                        if pong:
                            log.info("Redis connected during startup")
                            return
                    except Exception:
                        try:
                            await self._redis.close()
                        except Exception:
                            pass
                        self._redis = aioredis.from_url(redis_url, socket_connect_timeout=2)
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 5.0)
                        attempts += 1
                log.warning("Redis did not respond to pings during startup; readiness will report accordingly")
            except Exception:
                log.exception("Failed to initialize Redis client")

        # attempt to initialize redis client (don't block startup indefinitely)
        asyncio.create_task(_init_redis_client())

        if FastAPI is not None and uvicorn is not None:
            log.info("Starting FastAPI + uvicorn API server", host=host, port=port)

            @asynccontextmanager
            async def _lifespan(app):
                # Startup: initialize DB pool and Redis client
                try:
                    if hasattr(self.db, "init_pool"):
                        await self.db.init_pool()
                        log.info("Database pool initialized via init_pool() (lifespan)")
                    elif hasattr(self.db, "create_pool"):
                        await self.db.create_pool()
                        log.info("Database pool created via create_pool() (lifespan)")
                    elif hasattr(self.db, "connect_pool"):
                        await self.db.connect_pool()
                        log.info("Database pool created via connect_pool() (lifespan)")
                    else:
                        await self.db.connect()
                        log.info("Database connected via connect() (lifespan)")
                except Exception:
                    log.exception("Database pool initialization failed during lifespan startup")

                try:
                    await _init_redis_client()
                except Exception:
                    log.exception("Redis initialization failed during lifespan startup")

                # mark started time for metrics/uptime
                self._started_at = datetime.now(timezone.utc)

                try:
                    yield
                finally:
                    # Shutdown: close redis and DB pool
                    if getattr(self, "_redis", None):
                        try:
                            await self._redis.close()
                        except Exception:
                            pass
                        try:
                            await self._redis.wait_closed()
                        except Exception:
                            pass

                    try:
                        if hasattr(self.db, "close_pool"):
                            await self.db.close_pool()
                        elif hasattr(self.db, "disconnect"):
                            await self.db.disconnect()
                        else:
                            await self.db.close()
                    except Exception:
                        log.exception("Error closing DB during lifespan shutdown")

            app = FastAPI(lifespan=_lifespan)

            @app.get("/health")
            async def health():
                uptime = None
                try:
                    started = getattr(self, "_started_at", None)
                    if started:
                        uptime = (datetime.now(timezone.utc) - started).total_seconds()
                except Exception:
                    uptime = None

                payload = {"status": "ok"}
                if uptime is not None:
                    payload["uptime_seconds"] = round(uptime, 2)

                return payload

            @app.get("/ready")
            async def ready():
                payload, status = await _check_readiness(self)
                return payload, status

            # Request middleware: bind request_id and trace_id to structlog context and create request span
            from fastapi import Request

            @app.middleware("http")
            async def _request_middleware(request: Request, call_next):
                request_id = request.headers.get("X-Request-ID") or str(uuid4())
                structlog.contextvars.clear_contextvars()
                structlog.contextvars.bind_contextvars(request_id=request_id)

                with self._tracer.start_as_current_span("http.request") as span:
                    span.set_attribute("http.method", request.method)
                    span.set_attribute("http.path", str(request.url.path))
                    trace_id = format_trace_id(span.get_span_context().trace_id)
                    structlog.contextvars.bind_contextvars(trace_id=trace_id)
                    try:
                        response = await call_next(request)
                        span.set_attribute("http.status_code", response.status_code)
                        return response
                    except Exception as e:
                        span.record_exception(e)
                        raise

            from fastapi import Request, Response as FastAPIResponse

            def _is_allowed_metrics_request(addr: str, headers) -> bool:
                # Allowlist check
                allowlist = os.environ.get("METRICS_ALLOWLIST", "127.0.0.1,::1").split(",")
                allowlist = [a.strip() for a in allowlist if a.strip()]
                if addr and addr in allowlist:
                    return True
                # Bearer token check
                token = os.environ.get("METRICS_BEARER_TOKEN")
                if token:
                    auth = headers.get("authorization") or headers.get("Authorization")
                    if auth and auth.lower().startswith("bearer "):
                        if auth.split(None, 1)[1].strip() == token:
                            return True
                return False

            @app.get("/metrics")
            async def metrics(request: Request):
                # perform allowlist and bearer token auth
                peer = request.client.host if getattr(request, "client", None) else None
                if not _is_allowed_metrics_request(peer, request.headers):
                    return FastAPIResponse(status_code=403, content=b"Forbidden")

                try:
                    data = generate_latest()
                    return FastAPIResponse(content=data, media_type=CONTENT_TYPE_LATEST)
                except Exception:
                    return FastAPIResponse(status_code=503, content=b"metrics_unavailable")

            # Configure and start uvicorn programmatically in background
            config = uvicorn.Config(app, host=host, port=port, log_level="info")
            server = uvicorn.Server(config)

            # store references for graceful shutdown
            self._uvicorn_server = server
            self._api_task = asyncio.create_task(server.serve())
            self._started_at = datetime.now(timezone.utc)

            # start Prometheus metrics HTTP server (non-blocking)
            try:
                start_http_server(self._metrics_port)
                log.info("Prometheus metrics server started", port=self._metrics_port)
            except Exception:
                log.exception("Failed to start Prometheus metrics server")

            log.info("FastAPI server started", host=host, port=port)
            return

        # Fallback to aiohttp if FastAPI/uvicorn unavailable
        log.info("Starting internal aiohttp API server (fallback)")

        app = web.Application()

        async def health(request: web.Request) -> web.Response:
            uptime = None
            try:
                started = getattr(self, "_started_at", None)
                if started:
                    uptime = (datetime.now(timezone.utc) - started).total_seconds()
            except Exception:
                uptime = None

            payload = {"status": "ok"}
            if uptime is not None:
                payload["uptime_seconds"] = round(uptime, 2)

            return web.json_response(payload)

        app.add_routes([web.get("/health", health)])
        async def ready_handler(request: web.Request) -> web.Response:
            payload, status = await _check_readiness(self)
            return web.json_response(payload, status=status)

        def _is_allowed_metrics_request_aio(peer: str, headers) -> bool:
            allowlist = os.environ.get("METRICS_ALLOWLIST", "127.0.0.1,::1").split(",")
            allowlist = [a.strip() for a in allowlist if a.strip()]
            if peer and peer in allowlist:
                return True
            token = os.environ.get("METRICS_BEARER_TOKEN")
            if token:
                auth = headers.get("authorization") or headers.get("Authorization")
                if auth and auth.lower().startswith("bearer "):
                    if auth.split(None, 1)[1].strip() == token:
                        return True
            return False

        async def metrics_handler(request: web.Request) -> web.Response:
            peer = request.remote
            if not _is_allowed_metrics_request_aio(peer, request.headers):
                return web.Response(status=403, text="Forbidden")

            try:
                data = generate_latest()
                return web.Response(body=data, content_type=CONTENT_TYPE_LATEST)
            except Exception:
                return web.json_response({"error": "metrics_unavailable"}, status=503)

        app.add_routes([web.get("/ready", ready_handler), web.get("/metrics", metrics_handler)])

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=host, port=port)
        await site.start()

        # record runner for cleanup
        self._runner = runner
        self._site = site
        self._started_at = datetime.now(timezone.utc)
        # start Prometheus metrics HTTP server (non-blocking)
        try:
            start_http_server(self._metrics_port)
            log.info("Prometheus metrics server started", port=self._metrics_port)
        except Exception:
            log.exception("Failed to start Prometheus metrics server")

        log.info("aiohttp API server started", host=host, port=port)


async def main():
    """Main entry point"""
    # Load configuration
    config = {
        "database_url": "postgresql://user:password@localhost/fiop",
        "worm_storage_path": "./data/worm",
        "sec_api_key": "your_sec_api_key_here",
        "cost_limits": {"max_per_run": 0.50, "daily_limit": 5.00},
        "initial_companies": [
            {"ticker": "AAPL", "name": "Apple Inc."},
            {"ticker": "MSFT", "name": "Microsoft Corporation"},
        ],
        "monitored_companies": [
            {"ticker": "AAPL"},
            {"ticker": "MSFT"},
            {"ticker": "GOOGL"},
        ],
        "bank_accounts": [
            {"id": "checking_001", "name": "Primary Checking"},
            {"id": "savings_001", "name": "Business Savings"},
        ],
        "enable_api": True,
    }

    # Create platform instance
    platform = FIOPPlatform(config)

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(platform, s))
        )

    try:
        # Start platform
        await platform.start()
    except KeyboardInterrupt:
        log.info("Shutdown requested via keyboard")
    finally:
        await platform.stop()


async def shutdown(platform: FIOPPlatform, signal):
    """Graceful shutdown handler"""
    log.info(f"Received signal {signal.name}, shutting down...")
    await platform.stop()


if __name__ == "__main__":
    asyncio.run(main())
