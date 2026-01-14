"""Lightweight storage helper with safe EntityType coercion and WORMStorage protocol."""
from __future__ import annotations

from typing import Any, Optional, Protocol

from foip.shared_metadata_schema import EntityID, EntityType


class WORMStorage(Protocol):
    async def store(self, key: str, value: Any) -> None: ...
    async def retrieve(self, key: str) -> Optional[Any]: ...


def make_artifact_entity_id(source: str, identifier: str, entity_type: Optional[Any]) -> EntityID:
    """Construct EntityID for an artifact, coercing entity_type to EntityType safely."""
    # ensure et is an EntityType for typing/mypy and runtime correctness
    if isinstance(entity_type, EntityType):
        et: EntityType = entity_type
    else:
        try:
            et = EntityType(entity_type)
        except Exception:
            # fallback to ARTIFACT if coercion fails
            et = EntityType.ARTIFACT

    return EntityID(entity_type=et, identifier=identifier, source=source)
