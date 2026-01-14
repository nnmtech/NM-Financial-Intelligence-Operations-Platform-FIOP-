"""Lightweight Entity Registry for foip package (simplified)."""

import asyncio
from typing import Any, Dict, List, Optional, Tuple

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from foip.shared_metadata_schema import (
        EntityID,
        EntityRelationship,
        EntityType,
    )
else:
    EntityID = Any
    EntityRelationship = Any
    EntityType = Any


class EntityRegistry:
    def __init__(self, db: Any):
        self.db = db
        self._cache: Dict[str, Tuple[Any, dict]] = {}
        self._relationships_by_entity: Dict[Tuple[str, Optional[str]], List[Any]] = {}
        self._cache_lock = asyncio.Lock()
        self._relationships_lock = asyncio.Lock()

    async def register_entity(self, entity_id: "EntityID", metadata: dict) -> bool:
        async with self._cache_lock:
            self._cache[str(entity_id)] = (entity_id, metadata)
        return True

    async def get_entity(self, entity_id: "EntityID") -> Optional[tuple]:
        async with self._cache_lock:
            return self._cache.get(str(entity_id))

    async def add_relationship(self, relationship: "EntityRelationship") -> bool:
        async with self._relationships_lock:
            key = (str(relationship.source_entity_id), getattr(relationship.relationship_type, "value", None))
            self._relationships_by_entity.setdefault(key, []).append(relationship)
        return True

    async def find_related_entities(self, entity_id: "EntityID", relationship_type: Optional[str] = None) -> List["EntityRelationship"]:
        key = (str(entity_id), getattr(relationship_type, "value", relationship_type))
        async with self._relationships_lock:
            return list(self._relationships_by_entity.get(key, []))
