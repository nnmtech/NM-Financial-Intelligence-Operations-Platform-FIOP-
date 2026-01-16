"""A minimal no-op DatabaseConnection used for smoke tests and local runs.

This provides an async-friendly stub that satisfies the interfaces used
by `main.py` without requiring a real database. It is intentionally
lightweight and should NOT be used in production.
"""
from contextlib import asynccontextmanager
from typing import Any


class DummyConn:
    async def execute(self, *_args, **_kwargs):
        return None

    async def fetchone(self, *_args, **_kwargs):
        return None

    async def fetchval(self, *_args, **_kwargs):
        return 1


class DatabaseConnection:
    """Simple stub that mimics the minimal async DB interface used by main.py."""

    def __init__(self, database_url: str | None = None):
        self.database_url = database_url

    async def init_pool(self) -> None:
        return None

    async def create_pool(self) -> None:
        return None

    async def connect_pool(self) -> None:
        return None

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def close_pool(self) -> None:
        return None

    @asynccontextmanager
    async def transaction(self):
        conn = DummyConn()
        try:
            yield conn
        finally:
            return

    # convenience methods used by readiness checks
    async def fetchval(self, *args, **kwargs) -> Any:
        return await DummyConn().fetchval(*args, **kwargs)

    async def fetchone(self, *args, **kwargs) -> Any:
        return await DummyConn().fetchone(*args, **kwargs)
