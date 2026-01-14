"""MAKER orchestrator inside foip package."""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import attr
import structlog

if TYPE_CHECKING:
    from foip.shared_metadata_schema import EntityID, EntityType, RunMetadata, RunStatus as SharedRunStatus
    from foip.shared_storage_worm import WORMStorage
    from foip.shared_cost_calculator import CostCalculator
    from foip.invariants import InvariantChecker
else:
    EntityID = Any
    EntityType = Any
    RunMetadata = Any
    SharedRunStatus = Any
    WORMStorage = Any
    CostCalculator = Any
    InvariantChecker = Any

try:
    from foip.foip_infra import log as _foip_log
    log = _foip_log.bind(module="finscan_maker_core")
except Exception:
    log = structlog.get_logger(__name__)


@dataclass
class RunContext:
    run_id: uuid.UUID
    trigger: str
    run_type: str
    parameters: Dict[str, Any]
    artifacts: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    input_entity_ids: List[EntityID] = field(default_factory=list)
    output_entity_ids: List[EntityID] = field(default_factory=list)


class MAKEROrchestrator:
    def __init__(self, worm_storage: "WORMStorage", cost_calculator: "CostCalculator", invariant_checker: "InvariantChecker"):
        self.worm = worm_storage
        self.cost_calculator = cost_calculator
        self.invariant_checker = invariant_checker
        self.agents: Dict[str, Any] = {}
        self.run_history: Dict[uuid.UUID, Any] = {}
        self.pending_runs: asyncio.Queue = asyncio.Queue()
        self.active_runs: Dict[uuid.UUID, RunContext] = {}
        self._active_runs_lock = asyncio.Lock()
        self._run_history_lock = asyncio.Lock()
        log.info("MAKER Orchestrator initialized")

    def register_agent(self, agent: Any) -> None:
        self.agents[agent.name] = agent
        log.info(f"Agent registered", agent=agent.name, type=getattr(agent, "agent_type", None))
