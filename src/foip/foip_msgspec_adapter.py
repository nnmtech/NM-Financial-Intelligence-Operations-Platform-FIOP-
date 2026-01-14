"""Messagepack/JSON adapter helpers under foip package."""

import msgspec
from typing import Any


def encode(obj: Any) -> bytes:
    try:
        return msgspec.json.encode(obj)
    except Exception:
        return str(obj).encode("utf-8")


def decode(b: bytes) -> Any:
    try:
        return msgspec.json.decode(b)
    except Exception:
        return b.decode("utf-8")
