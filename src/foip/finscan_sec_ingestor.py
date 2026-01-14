"""SEC ingestor inside foip package."""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import aiohttp
import attr

if TYPE_CHECKING:
    from foip.shared_metadata_schema import (
        Company,
        EntityID,
        EntityRelationship,
        EntityType,
        FormType,
        SECFiling,
        TimePoint,
    )
    from foip.shared_metadata_registry import EntityRegistry
else:
    Company = Any
    EntityID = Any
    EntityRelationship = Any
    EntityType = Any
    FormType = Any
    SECFiling = Any
    TimePoint = Any
    EntityRegistry = Any

try:
    from foip.foip_infra import log as _foip_log
    log = _foip_log.bind(module="finscan_sec_ingestor")
except Exception:
    import structlog
    log = structlog.get_logger(__name__)


class SECIngestor:
    def __init__(self, api_key: str, entity_registry: "EntityRegistry", base_url: str = "https://api.sec-api.io"):
        self.api_key = api_key
        self.entity_registry = entity_registry
        self.base_url = base_url
        self.parser = None
        self.diff_engine = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_request: Optional[datetime] = None
        self.min_interval = timedelta(seconds=0.1)
        self._rate_lock = asyncio.Lock()
        log.info("SEC Ingestor initialized")

    async def initialize(self):
        self.session = aiohttp.ClientSession(headers={"Authorization": self.api_key}, timeout=aiohttp.ClientTimeout(total=30))

    async def close(self):
        if self.session:
            await self.session.close()
