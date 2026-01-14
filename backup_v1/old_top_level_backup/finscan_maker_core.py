"""
MAKER - Deterministic Execution Engine
Coordinates both FinScan and Operations runs
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import attr
import structlog
from enum import Enum

# Import canonical schema types; allow top-level fallback for test runs
try:
    from shared_metadata_schema import EntityID, EntityType, RunMetadata
    from shared_metadata_schema import RunStatus as SharedRunStatus
except Exception:
    # Minimal local fallback
    from enum import Enum as _Enum

    class SharedRunStatus(_Enum):
        PENDING = "pending"
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"
        CANCELED = "canceled"
        BLOCKED = "blocked"


try:
    from shared_storage_worm import WORMStorage
except Exception:
    WORMStorage = object

try:
    from shared_cost_calculator import CostCalculator
except Exception:
    CostCalculator = object

try:
    from .invariants import InvariantChecker
except Exception:
    InvariantChecker = object

# Use central structured logger if available
try:
    from foip_infra import log as _foip_log

    log = _foip_log.bind(module="finscan_maker_core")
except Exception:
    log = structlog.get_logger(__name__)


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    BLOCKED = "blocked"  # By invariant violation


@dataclass
class RunContext:
    """Context for a single run execution"""

    run_id: uuid.UUID
    trigger: str
    run_type: str
    parameters: Dict[str, Any]
    artifacts: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Cross-domain references
    input_entity_ids: List[EntityID] = field(default_factory=list)
    output_entity_ids: List[EntityID] = field(default_factory=list)

    # Cost tracking
    cost_estimate: float = 0.0
    actual_cost: float = 0.0
    cost_breakdown: Dict[str, float] = field(default_factory=dict)

    # Invariants
    invariant_violations: List[str] = field(default_factory=list)
    blocked_by_invariant: bool = False


class Agent:
    """Base agent class for both FinScan and Operations"""

    def __init__(self, name: str, agent_type: str):
        self.name = name
        self.agent_type = agent_type  # "finscan", "operations"
        self.dependencies: List[str] = []

    async def execute(self, context: RunContext) -> Dict[str, Any]:
        """Execute agent logic - to be implemented by subclasses"""
        raise NotImplementedError

    async def validate(self, context: RunContext) -> List[str]:
        """Validate inputs before execution"""
        return []

    def estimate_cost(self, context: RunContext) -> float:
        """Estimate cost of execution"""
        return 0.0


class MAKEROrchestrator:
    """
    Main orchestrator that coordinates parallel and serial execution
    of FinScan and Operations agents
    """

    def __init__(
        self,
        worm_storage: WORMStorage,
        cost_calculator: CostCalculator,
        invariant_checker: InvariantChecker,
    ):
        self.worm = worm_storage
        self.cost_calculator = cost_calculator
        self.invariant_checker = invariant_checker

        # Agent registry
        self.agents: Dict[str, Agent] = {}
        self.run_history: Dict[uuid.UUID, RunMetadata] = {}

        # Execution queues
        self.pending_runs: asyncio.Queue = asyncio.Queue()
        self.active_runs: Dict[uuid.UUID, RunContext] = {}

        # Concurrency protection for shared state
        self._active_runs_lock = asyncio.Lock()
        self._run_history_lock = asyncio.Lock()

        log.info("MAKER Orchestrator initialized")

    def register_agent(self, agent: Agent) -> None:
        """Register an agent with the orchestrator"""
        self.agents[agent.name] = agent
        log.info(f"Agent registered", agent=agent.name, type=agent.agent_type)

    async def start_run(
        self,
        run_type: str,
        trigger: str = "manual",
        parameters: Optional[Dict[str, Any]] = None,
    ) -> uuid.UUID:
        """Start a new run with proper validation"""
        run_id = uuid.uuid4()

        # Create run context
        context = RunContext(
            run_id=run_id,
            trigger=trigger,
            run_type=run_type,
            parameters=parameters or {},
        )

        # Check invariants before starting
        invariant_errors = await self.invariant_checker.check_run_invariants(
            run_type, parameters
        )

        if invariant_errors:
            context.invariant_violations = invariant_errors
            context.blocked_by_invariant = True

            # Create blocked run record
            run_metadata = RunMetadata(
                run_id=run_id,
                run_type=run_type,
                trigger=trigger,
                status=SharedRunStatus.BLOCKED.value,
                invariants_check={},
                invariant_violations=invariant_errors,
            )

            # Use attr.asdict to serialize attrs-based RunMetadata
            try:
                payload = attr.asdict(run_metadata)
            except Exception:
                # Fallback to __dict__ if not an attrs instance
                payload = getattr(run_metadata, "__dict__", {})

            await self.worm.store_artifact(
                artifact_type="run_metadata",
                content=payload,
                run_id=run_id,
            )

            log.warning(
                "Run blocked by invariants", run_id=run_id, errors=invariant_errors
            )
            return run_id

        # Estimate cost
        context.cost_estimate = await self._estimate_run_cost(context)

        # Check cost invariants
        cost_ok = await self.cost_calculator.validate_run_cost(
            run_type, context.cost_estimate
        )

        if not cost_ok:
            log.error("Run exceeds cost limits", run_id=run_id)
            return run_id

        # Add to pending queue
        async with self._active_runs_lock:
            self.active_runs[run_id] = context
        await self.pending_runs.put((run_id, context))

        log.info("Run started", run_id=run_id, type=run_type, trigger=trigger)

        return run_id

    async def execute_run(self, run_id: uuid.UUID) -> RunContext:
        """Execute a run with proper coordination"""
        if run_id not in self.active_runs:
            raise ValueError(f"Run {run_id} not found")

        context = self.active_runs[run_id]

        try:
            # Update status
            await self._update_run_status(run_id, SharedRunStatus.RUNNING)

            # Determine execution plan based on run type
            execution_plan = await self._create_execution_plan(context)

            # Execute plan
            results = await self._execute_plan(execution_plan, context)

            # Seal artifacts in WORM storage
            await self._seal_artifacts(context, results)

            # Update run metadata
            await self._complete_run(context)

            log.info("Run completed successfully", run_id=run_id)

        except Exception as e:
            log.exception("Run failed", run_id=run_id, error=str(e))
            await self._fail_run(context, str(e))

        finally:
            self.active_runs.pop(run_id, None)

        return context

    async def _create_execution_plan(self, context: RunContext) -> Dict:
        """Create deterministic execution plan based on run type"""
        if context.run_type == "sec_ingestion":
            # Parallel fetching of different forms
            return {
                "parallel": ["sec_10k_agent", "sec_10q_agent", "sec_8k_agent"],
                "serial": ["sec_validator", "risk_extractor", "artifact_builder"],
            }
        elif context.run_type == "bank_reconciliation":
            # Serial with potential parallel matching
            return {
                "serial": [
                    "bank_fetcher",
                    "ledger_fetcher",
                    "matcher_agent",  # Can be parallelized internally
                    "exception_handler",
                    "reconciliation_reporter",
                ]
            }
        elif context.run_type == "cross_domain":
            # Combined FinScan + Operations run
            return {
                "parallel": ["sec_ingestion", "bank_reconciliation"],
                "serial": [
                    "correlation_analyzer",
                    "risk_assessor",
                    "compliance_validator",
                ],
            }
        else:
            return {"serial": [context.run_type]}

    async def _execute_plan(self, plan: Dict, context: RunContext) -> Dict[str, Any]:
        """Execute agents according to plan"""
        results = {}

        # Execute parallel agents
        if "parallel" in plan:
            parallel_tasks = []
            for agent_name in plan["parallel"]:
                if agent_name in self.agents:
                    task = self._execute_agent_safe(self.agents[agent_name], context)
                    parallel_tasks.append(task)

            if parallel_tasks:
                parallel_results = await asyncio.gather(*parallel_tasks)
                for agent_name, result in zip(plan["parallel"], parallel_results):
                    results[agent_name] = result

        # Execute serial agents
        if "serial" in plan:
            for agent_name in plan["serial"]:
                if agent_name in self.agents:
                    result = await self._execute_agent_safe(
                        self.agents[agent_name], context
                    )
                    results[agent_name] = result

                    # Check for blocking conditions
                    if context.blocked_by_invariant:
                        break

        return results

    async def _execute_agent_safe(self, agent: Agent, context: RunContext) -> Any:
        """Execute agent with error handling and cost tracking"""
        try:
            # Initialize any resource-like attributes on the agent that expose
            # an `initialize()` method and appear uninitialized. Track those
            # resources so we can `close()` them after execution.
            initialized_resources = []
            for attr_name, attr_val in vars(agent).items():
                if attr_val is None:
                    continue
                init = getattr(attr_val, "initialize", None)
                if not callable(init):
                    continue

                # Heuristics to decide whether to initialize:
                # - explicit boolean flags: `is_initialized` or `initialized`
                # - presence of a `session` attribute that is currently None
                should_init = False
                if hasattr(attr_val, "is_initialized"):
                    try:
                        if not getattr(attr_val, "is_initialized"):
                            should_init = True
                    except Exception:
                        pass
                elif hasattr(attr_val, "initialized"):
                    try:
                        if not getattr(attr_val, "initialized"):
                            should_init = True
                    except Exception:
                        pass
                elif hasattr(attr_val, "session"):
                    try:
                        if getattr(attr_val, "session") is None:
                            should_init = True
                    except Exception:
                        pass

                if should_init:
                    try:
                        await init()
                        initialized_resources.append(attr_val)
                    except Exception:
                        log.exception(
                            "Failed to initialize agent resource",
                            agent=agent.name,
                            resource=attr_name,
                        )

            # Pre-execution validation
            validation_errors = await agent.validate(context)
            if validation_errors:
                context.errors.extend(validation_errors)
                # Close any resources we initialized above
                for res in initialized_resources:
                    try:
                        close = getattr(res, "close", None)
                        if callable(close):
                            await close()
                    except Exception:
                        log.exception(
                            "Failed to close initialized resource during validation error",
                            agent=agent.name,
                        )
                return None

            # Track cost
            agent_cost = agent.estimate_cost(context)
            context.actual_cost += agent_cost

            # Execute
            start_time = datetime.now(timezone.utc)
            result = await agent.execute(context)
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

            # Update cost breakdown
            context.cost_breakdown[agent.name] = agent_cost

            log.info(
                "Agent execution completed",
                agent=agent.name,
                time=execution_time,
                cost=agent_cost,
            )

            return result

        except Exception as e:
            log.error("Agent execution failed", agent=agent.name, error=str(e))
            context.errors.append(f"{agent.name}: {str(e)}")
            return None
        finally:
            # Close any resources we initialized above
            for res in initialized_resources:
                try:
                    close = getattr(res, "close", None)
                    if callable(close):
                        await close()
                except Exception:
                    log.exception(
                        "Failed to close initialized resource", agent=agent.name
                    )

    async def _seal_artifacts(self, context: RunContext, results: Dict[str, Any]):
        """Store all artifacts in WORM storage"""
        for agent_name, result in results.items():
            if result:
                artifact_id = await self.worm.store_artifact(
                    artifact_type=f"{agent_name}_result",
                    content=result,
                    run_id=context.run_id,
                    metadata={
                        "agent": agent_name,
                        "run_type": context.run_type,
                        "cost": context.cost_breakdown.get(agent_name, 0),
                    },
                )
                context.output_entity_ids.append(artifact_id)

    async def _complete_run(self, context: RunContext):
        """Complete run and store final metadata"""
        run_metadata = RunMetadata(
            run_id=context.run_id,
            run_type=context.run_type,
            trigger=context.trigger,
            started_at=datetime.now(timezone.utc),  # Should be from start
            completed_at=datetime.now(timezone.utc),
            status=(
                SharedRunStatus.COMPLETED.value
                if not context.errors
                else SharedRunStatus.FAILED.value
            ),
            estimated_cost=context.cost_estimate,
            actual_cost=context.actual_cost,
            cost_breakdown=context.cost_breakdown,
            invariants_check={},  # Populated by invariant checker
            invariant_violations=context.invariant_violations,
            input_entities=context.input_entity_ids,
            output_artifacts=context.output_entity_ids,
        )
        # Serialize using attr.asdict when available
        try:
            payload = attr.asdict(run_metadata)
        except Exception:
            payload = getattr(run_metadata, "__dict__", {})

        await self.worm.store_artifact(
            artifact_type="run_metadata",
            content=payload,
            run_id=context.run_id,
        )

        async with self._run_history_lock:
            self.run_history[context.run_id] = run_metadata

        # Log cost metrics
        await self.cost_calculator.record_run_cost(context.run_id, context.actual_cost)

    async def _fail_run(self, context: RunContext, error: str):
        """Handle failed run"""
        run_metadata = RunMetadata(
            run_id=context.run_id,
            run_type=context.run_type,
            trigger=context.trigger,
            started_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
            status=SharedRunStatus.FAILED.value,
            estimated_cost=context.cost_estimate,
            actual_cost=context.actual_cost,
            cost_breakdown=context.cost_breakdown,
            invariant_violations=[error] + context.invariant_violations,
            input_entities=context.input_entity_ids,
            output_artifacts=context.output_entity_ids,
        )
        try:
            payload = attr.asdict(run_metadata)
        except Exception:
            payload = getattr(run_metadata, "__dict__", {})

        await self.worm.store_artifact(
            artifact_type="run_metadata",
            content=payload,
            run_id=context.run_id,
        )

    async def _update_run_status(self, run_id: uuid.UUID, status: RunStatus):
        """Update run status in real-time (for monitoring)"""
        # This would update a monitoring system or database
        log.debug("Run status updated", run_id=run_id, status=status.value)

    async def _estimate_run_cost(self, context: RunContext) -> float:
        """Estimate total cost for a run"""
        total = 0.0

        # Base cost by run type
        base_costs = {
            "sec_ingestion": 0.10,
            "bank_reconciliation": 0.05,
            "invoice_processing": 0.03,
            "compliance_report": 0.15,
            "cross_domain": 0.25,
        }

        total += base_costs.get(context.run_type, 0.10)

        # Add cost for each agent that will run
        execution_plan = await self._create_execution_plan(context)

        all_agents = []
        if "parallel" in execution_plan:
            all_agents.extend(execution_plan["parallel"])
        if "serial" in execution_plan:
            all_agents.extend(execution_plan["serial"])

        for agent_name in all_agents:
            if agent_name in self.agents:
                agent_cost = self.agents[agent_name].estimate_cost(context)
                total += agent_cost

        return total

    async def replay_run(self, run_id: uuid.UUID) -> RunContext:
        """Replay a run from WORM storage"""
        # Retrieve run metadata
        run_metadata = await self.worm.retrieve_artifact(
            run_id=run_id, artifact_type="run_metadata"
        )

        if not run_metadata:
            raise ValueError(f"Run {run_id} not found in WORM storage")

        # Retrieve all artifacts
        artifacts = await self.worm.retrieve_run_artifacts(run_id)

        # Create replay context
        replay_context = RunContext(
            run_id=uuid.uuid4(),  # New run ID for replay
            trigger="replay",
            run_type=f"replay_{run_metadata['run_type']}",
            parameters={"original_run_id": str(run_id)},
        )

            # Seal replay artifacts (read-only copy)
        await self.worm.store_artifact(
            artifact_type="replay_metadata",
            content={
                "original_run": run_metadata,
                "replayed_at": datetime.now(timezone.utc).isoformat(),
                "artifacts_retrieved": len(artifacts),
            },
            run_id=replay_context.run_id,
        )

        log.info("Run replayed", original_run=run_id, replay_run=replay_context.run_id)

        return replay_context
