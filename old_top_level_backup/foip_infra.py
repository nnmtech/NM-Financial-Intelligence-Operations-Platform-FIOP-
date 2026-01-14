"""Infrastructure helpers to align with recommended stack.

Provides:
- `structlog` JSON logger configuration
- simple `msgspec` encode/decode helpers that work with `attrs` models
- `tenacity` retry example helper
- `diskcache` memoize wrapper example
- `anyio` run helper example

These are minimal helpers intended for local use and to be extended where needed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Type

import anyio
import attr
import msgspec
import structlog
from diskcache import Cache
from tenacity import retry, stop_after_attempt, wait_exponential

from foip_msgspec_adapter import decode_attrs, encode_attrs

# ---- Logger (structlog) ----


def configure_logger(service_name: str = "foip") -> structlog.BoundLogger:
    processors = [
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
    structlog.configure(processors=processors)
    log = structlog.get_logger(service=service_name)
    return log


log = configure_logger()

# ---- msgspec helpers for attrs ----


def to_msgspec_json(obj: Any) -> bytes:
    """Encode an `attrs`-decorated object to JSON bytes using msgspec adapters."""
    return encode_attrs(obj)


def from_msgspec_json(cls: Type, data: bytes) -> Any:
    """Decode JSON bytes to an instance of `cls` using msgspec adapters."""
    return decode_attrs(cls, data)


# ---- tenacity example ----


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.5, max=10))
def retryable_call(fn: Callable[..., Any], *args, **kwargs) -> Any:
    """Run a callable with retry policy configured by tenacity."""
    return fn(*args, **kwargs)


# ---- diskcache example ----

_cache = Cache("./.foip_cache")


def memoize_disk(expire: int = 3600):
    def _decorator(fn: Callable):
        def _wrapped(*args, **kwargs):
            key = (fn.__name__, args, tuple(sorted(kwargs.items())))
            cached = _cache.get(key)
            if cached is not None:
                return cached
            val = fn(*args, **kwargs)
            _cache.set(key, val, expire=expire)
            return val

        return _wrapped

    return _decorator


# ---- anyio example ----


async def _example_worker(delay: float = 1.0):
    await anyio.sleep(delay)


def run_async_worker(delay: float = 1.0):
    anyio.run(_example_worker, delay)


# ---- small utilities ----


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
