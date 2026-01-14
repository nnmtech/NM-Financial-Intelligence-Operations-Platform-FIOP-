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
            # Connect to database
            await self.db.connect()

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
        # This would start FastAPI or similar
        log.info("API server would start here")
        # Implementation omitted for brevity


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
