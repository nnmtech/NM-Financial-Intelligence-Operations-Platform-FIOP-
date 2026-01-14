"""Lightweight storage helper with a concrete WORMStorage implementation.

This module provides a small file-backed WORMStorage used for local
smoke tests and development. It intentionally avoids external
dependencies and is not intended for production use.
"""
from __future__ import annotations

from typing import Any, Optional
from pathlib import Path

from foip.shared_metadata_schema import EntityID, EntityType


class WORMStorage:
    """A simple file-backed WORM storage for smoke/local runs.

    Implements async `store` and `retrieve` methods. Not optimized for
    production; intended to allow the application to start during smoke tests.
    """

    def __init__(self, base_path: str | Path):
        self.base = Path(base_path)
        try:
            self.base.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    async def store(self, key: str, value: Any) -> None:
        p = self.base / key
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(str(value))
        except Exception:
            pass

    async def retrieve(self, key: str) -> Optional[Any]:
        p = self.base / key
        try:
            if p.exists():
                return p.read_text()
        except Exception:
            return None
        return None


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
