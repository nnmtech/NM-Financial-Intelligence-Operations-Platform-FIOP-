"""
Entity Registry - Central source of truth for all platform entities
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from cachetools import TTLCache

try:
    from shared_metadata_schema import EntityID, EntityRelationship, EntityType
except Exception:  # fallback for test runner / top-level module import
    EntityID = object
    EntityRelationship = object
    EntityType = object

try:
    from storage.postgres import DatabaseConnection
except Exception:  # tests / environments without package layout
    DatabaseConnection = object


class EntityRegistry:
    """Central registry for all entities across FinScan and Operations"""

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self._cache: Dict[str, Tuple[EntityID, dict]] = (
            {}
        )  # In-memory cache for hot entities
        # Relationship caches: one for full-tuple lookup, one for per-entity lookup
        self._relationships_by_tuple: Dict[Tuple[str, str, str], EntityRelationship] = (
            {}
        )
        self._relationships_by_entity: Dict[
            Tuple[str, Optional[str]], List[EntityRelationship]
        ] = {}

        # TTL/LRU cache for entities: limit size and TTL (e.g., 1000 entries, 600s)
        self._cache = TTLCache(maxsize=1000, ttl=600)

        # Locks to protect concurrent async access
        self._cache_lock = asyncio.Lock()
        self._relationships_lock = asyncio.Lock()

        # Structured logger (use central infra logger when available)
        try:
            from foip_infra import log as _foip_log

            self._logger = _foip_log.bind(module="shared_metadata_registry")
        except Exception:
            import structlog

            self._logger = structlog.get_logger(__name__)

    async def register_entity(self, entity_id: EntityID, metadata: dict) -> bool:
        """Register a new entity in the platform"""
        async with self.db.transaction() as conn:
            # Check for duplicates
            existing = await conn.fetchval(
                """
                SELECT 1 FROM entities 
                WHERE entity_type = $1 AND identifier = $2 AND source = $3
            """,
                entity_id.entity_type.value,
                entity_id.identifier,
                entity_id.source,
            )

            if existing:
                # Update timestamp
                await conn.execute(
                    """
                    UPDATE entities SET last_seen = $1, metadata = $2
                    WHERE entity_type = $3 AND identifier = $4 AND source = $5
                """,
                    datetime.now(timezone.utc),
                    metadata,
                    entity_id.entity_type.value,
                    entity_id.identifier,
                    entity_id.source,
                )
            else:
                # Insert new entity
                await conn.execute(
                    """
                    INSERT INTO entities 
                    (entity_type, identifier, source, first_seen, last_seen, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    entity_id.entity_type.value,
                    entity_id.identifier,
                    entity_id.source,
                    self._ensure_utc(entity_id.timestamp),
                    datetime.now(timezone.utc),
                    metadata,
                )

            # Cache the entity (protect with lock)
            cache_key = str(entity_id)
            async with self._cache_lock:
                self._cache[cache_key] = (entity_id, metadata)
                self._logger.info(
                    "entity.cached",
                    key=cache_key,
                    entity_type=entity_id.entity_type.value,
                )

            return True

    async def get_entity(self, entity_id: EntityID) -> Optional[tuple]:
        """Retrieve entity by ID"""
        cache_key = str(entity_id)
        async with self._cache_lock:
            if cache_key in self._cache:
                self._logger.debug("entity.cache_hit", key=cache_key)
                return self._cache[cache_key]

        async with self.db.transaction() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM entities 
                WHERE entity_type = $1 AND identifier = $2 AND source = $3
            """,
                entity_id.entity_type.value,
                entity_id.identifier,
                entity_id.source,
            )

            if row:
                entity = EntityID(
                    entity_type=EntityType(row["entity_type"]),
                    identifier=row["identifier"],
                    source=row["source"],
                    timestamp=(
                        self._ensure_utc(row["last_seen"])
                        if row.get("last_seen") is not None
                        else None
                    ),
                )
                metadata = dict(row.get("metadata") or {})
                async with self._cache_lock:
                    self._cache[cache_key] = (entity, metadata)
                    self._logger.info("entity.loaded", key=cache_key)
                    return self._cache[cache_key]

        return None

    async def add_relationship(self, relationship: EntityRelationship) -> bool:
        """Add a relationship between entities"""
        async with self.db.transaction() as conn:
            # Check if relationship already exists
            rel_type_value = getattr(
                relationship.relationship_type, "value", relationship.relationship_type
            )
            existing = await conn.fetchval(
                """
                SELECT 1 FROM entity_relationships
                WHERE source_entity_type = $1 AND source_identifier = $2 AND source_source = $3
                AND target_entity_type = $4 AND target_identifier = $5 AND target_source = $6
                AND relationship_type = $7
            """,
                relationship.source_entity_id.entity_type.value,
                relationship.source_entity_id.identifier,
                relationship.source_entity_id.source,
                relationship.target_entity_id.entity_type.value,
                relationship.target_entity_id.identifier,
                relationship.target_entity_id.source,
                rel_type_value,
            )

            if not existing:
                await conn.execute(
                    """
                    INSERT INTO entity_relationships
                    (relationship_id, source_entity_type, source_identifier, source_source,
                     target_entity_type, target_identifier, target_source, relationship_type,
                     confidence, established_by_run, established_at, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                    relationship.relationship_id,
                    relationship.source_entity_id.entity_type.value,
                    relationship.source_entity_id.identifier,
                    relationship.source_entity_id.source,
                    relationship.target_entity_id.entity_type.value,
                    relationship.target_entity_id.identifier,
                    relationship.target_entity_id.source,
                    rel_type_value,
                    relationship.confidence,
                    relationship.established_by_run,
                    self._ensure_utc(relationship.established_at),
                    relationship.metadata,
                )

                # Cache the relationship (index both ways)
                tuple_key = (
                    str(relationship.source_entity_id),
                    str(relationship.target_entity_id),
                    rel_type_value,
                )
                src_entity_key = (str(relationship.source_entity_id), rel_type_value)
                tgt_entity_key = (str(relationship.target_entity_id), rel_type_value)

                async with self._relationships_lock:
                    self._relationships_by_tuple[tuple_key] = relationship
                    self._relationships_by_entity.setdefault(src_entity_key, []).append(
                        relationship
                    )
                    self._relationships_by_entity.setdefault(tgt_entity_key, []).append(
                        relationship
                    )
                    # Also index under a wildcard (None) key for queries that don't filter by type
                    self._relationships_by_entity.setdefault(
                        (str(relationship.source_entity_id), None), []
                    ).append(relationship)
                    self._relationships_by_entity.setdefault(
                        (str(relationship.target_entity_id), None), []
                    ).append(relationship)
                    self._logger.info(
                        "relationship.added",
                        rel_id=str(relationship.relationship_id),
                        src=str(relationship.source_entity_id),
                        tgt=str(relationship.target_entity_id),
                    )

                return True

            return False

    async def find_related_entities(
        self, entity_id: EntityID, relationship_type: Optional[str] = None
    ) -> List[EntityRelationship]:
        """Find all relationships for an entity"""
        cache_key = (
            str(entity_id),
            getattr(relationship_type, "value", relationship_type),
        )
        async with self._relationships_lock:
            if cache_key in self._relationships_by_entity:
                return list(self._relationships_by_entity[cache_key])

        async with self.db.transaction() as conn:
            query = """
                SELECT * FROM entity_relationships
                WHERE ((source_entity_type = $1 AND source_identifier = $2 AND source_source = $3)
                   OR (target_entity_type = $1 AND target_identifier = $2 AND target_source = $3))
            """
            params = [
                entity_id.entity_type.value,
                entity_id.identifier,
                entity_id.source,
            ]

            rel_type_value = getattr(relationship_type, "value", relationship_type)
            if rel_type_value:
                query += " AND relationship_type = $4"
                params.append(rel_type_value)

            rows = await conn.fetch(query, *params)

            relationships = []
            for row in rows:
                rel = EntityRelationship(
                    relationship_id=row["relationship_id"],
                    source_entity_id=EntityID(
                        entity_type=EntityType(row["source_entity_type"]),
                        identifier=row["source_identifier"],
                        source=row["source_source"],
                    ),
                    target_entity_id=EntityID(
                        entity_type=EntityType(row["target_entity_type"]),
                        identifier=row["target_identifier"],
                        source=row["target_source"],
                    ),
                    relationship_type=row["relationship_type"],
                    confidence=row.get("confidence"),
                    established_by_run=row.get("established_by_run"),
                    established_at=(
                        self._ensure_utc(row.get("established_at"))
                        if row.get("established_at") is not None
                        else None
                    ),
                    metadata=dict(row.get("metadata") or {}),
                )
                relationships.append(rel)

            async with self._relationships_lock:
                self._relationships_by_entity[cache_key] = relationships
            return relationships

    def _ensure_utc(self, dt: Optional[datetime]) -> Optional[datetime]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
