import sys
import types
import uuid
from datetime import datetime, timezone

import pytest

# Provide a lightweight `cachetools` shim during local test runs when the
# dependency is not installed. This keeps tests runnable locally while CI
# installs the real `cachetools` package.
try:
    import cachetools  # noqa: F401
except Exception:
    cachetools = types.ModuleType("cachetools")

    class _TTLCache(dict):
        def __init__(self, maxsize=1024, ttl=600):
            super().__init__()
            self._maxsize = maxsize
            self._ttl = ttl

        def __setitem__(self, k, v):
            if len(self) >= self._maxsize:
                self.pop(next(iter(self)))
            super().__setitem__(k, v)

        def __contains__(self, k):
            return dict.__contains__(self, k)

    cachetools.TTLCache = _TTLCache
    sys.modules["cachetools"] = cachetools

from foip.shared_metadata_registry import EntityRegistry
from foip.shared_metadata_schema import EntityID, EntityRelationship, EntityType


class FakeConn:
    async def fetchval(self, *args, **kwargs):
        return None

    async def execute(self, *args, **kwargs):
        return None

    async def fetchrow(self, *args, **kwargs):
        return None

    async def fetch(self, *args, **kwargs):
        return []


class FakeDB:
    def __init__(self):
        self.conn = FakeConn()

    def transaction(self):
        class TX:
            def __init__(self, conn):
                self.conn = conn

            async def __aenter__(self):
                return self.conn

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return TX(self.conn)


@pytest.mark.asyncio
async def test_register_and_get_entity_cache_hit():
    db = FakeDB()
    registry = EntityRegistry(db)

    eid = EntityID(
        entity_type=EntityType.FILING,
        identifier="ABC123",
        source="ext",
        timestamp=datetime.now(timezone.utc),
    )
    metadata = {"foo": "bar"}

    await registry.register_entity(eid, metadata)
    ent = await registry.get_entity(eid)

    assert ent is not None
    assert ent[0].identifier == "ABC123"
    assert ent[1]["foo"] == "bar"


@pytest.mark.asyncio
async def test_add_and_find_relationship_cached():
    db = FakeDB()
    registry = EntityRegistry(db)

    src = EntityID(
        entity_type=EntityType.FILING,
        identifier="SRC",
        source="s",
        timestamp=datetime.now(timezone.utc),
    )
    tgt = EntityID(
        entity_type=EntityType.INVOICE,
        identifier="TGT",
        source="t",
        timestamp=datetime.now(timezone.utc),
    )
    rel = EntityRelationship(
        source_entity_id=src,
        target_entity_id=tgt,
        relationship_type="owns",
        confidence=0.9,
        established_by_run=uuid.uuid4(),
        established_at=datetime.now(timezone.utc),
        metadata={"k": "v"},
    )

    added = await registry.add_relationship(rel)
    related = await registry.find_related_entities(src)

    assert added is True
    assert len(related) >= 1
    assert isinstance(related[0].relationship_id, uuid.UUID)


import asyncio
from datetime import datetime, timezone

from foip.shared_metadata_registry import EntityRegistry
from foip.shared_metadata_schema import EntityID, EntityRelationship, EntityType


class FakeConn:
    def __init__(self):
        self._vals = {}

    async def fetchval(self, *args, **kwargs):
        return None

    async def execute(self, *args, **kwargs):
        return None

    async def fetchrow(self, *args, **kwargs):
        return None

    async def fetch(self, *args, **kwargs):
        return []


class FakeDB:
    def __init__(self):
        self.conn = FakeConn()

    def transaction(self):
        class TX:
            def __init__(self, conn):
                self.conn = conn

            async def __aenter__(self):
                return self.conn

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return TX(self.conn)


def test_register_and_get_entity_cache_hit():
    db = FakeDB()
    registry = EntityRegistry(db)

    eid = EntityID(
        entity_type=EntityType.FILING,
        identifier="ABC123",
        source="ext",
        timestamp=datetime.now(timezone.utc),
    )
    metadata = {"foo": "bar"}

    # Run register_entity and then get_entity (should hit cache)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(registry.register_entity(eid, metadata))
        ent = loop.run_until_complete(registry.get_entity(eid))
    finally:
        loop.close()

    assert ent is not None
    assert ent[0].identifier == "ABC123"
    assert ent[1]["foo"] == "bar"


def test_add_and_find_relationship_cached():
    db = FakeDB()
    registry = EntityRegistry(db)

    src = EntityID(
        entity_type=EntityType.FILING,
        identifier="SRC",
        source="s",
        timestamp=datetime.now(timezone.utc),
    )
    tgt = EntityID(
        entity_type=EntityType.INVOICE,
        identifier="TGT",
        source="t",
        timestamp=datetime.now(timezone.utc),
    )
    import uuid

    rel = EntityRelationship(
        source_entity_id=src,
        target_entity_id=tgt,
        relationship_type="owns",
        confidence=0.9,
        established_by_run=uuid.uuid4(),
        established_at=datetime.now(timezone.utc),
        metadata={"k": "v"},
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        added = loop.run_until_complete(registry.add_relationship(rel))
        related = loop.run_until_complete(registry.find_related_entities(src))
    finally:
        loop.close()

    assert added is True
    assert len(related) >= 1
    import uuid as _uuid

    assert isinstance(related[0].relationship_id, _uuid.UUID)
