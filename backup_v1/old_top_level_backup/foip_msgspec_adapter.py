"""Adapters between `attrs` models and `msgspec` JSON.

Provides:
- `attrs_to_primitive(obj)` — recursively convert attrs objects to JSON-serializable primitives.
- `primitive_to_attrs(cls, data)` — reconstruct attrs objects from primitive dicts/lists.
- `encode_attrs(obj)` / `decode_attrs(cls, data)` helpers using `msgspec`.

This handles common typing patterns: `Optional[T]`, `List[T]`, `Dict[str, T]`, nested attrs classes, `Enum`, `datetime`, and `uuid.UUID`.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import (Any, Dict, List, Optional, Tuple, Type, get_args,
                    get_origin, get_type_hints)

import attr
import msgspec


def _is_attrs_class(cls: Type) -> bool:
    return attr.has(cls)


def attrs_to_primitive(obj: Any) -> Any:
    """Recursively convert `attrs` instances to JSON-serializable primitives.

    Conversions:
    - attrs instances -> dict
    - Enum -> value
    - datetime -> ISO string
    - uuid.UUID -> str
    - lists/tuples/sets/dicts are handled recursively
    """
    if obj is None:
        return None

    if isinstance(obj, Enum):
        return obj.value

    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, uuid.UUID):
        return str(obj)

    if attr.has(type(obj)):
        out = {}
        for field in attr.fields(type(obj)):
            val = getattr(obj, field.name)
            out[field.name] = attrs_to_primitive(val)
        return out

    if isinstance(obj, (list, tuple, set)):
        return [attrs_to_primitive(x) for x in obj]

    if isinstance(obj, dict):
        return {k: attrs_to_primitive(v) for k, v in obj.items()}

    return obj


def _convert_value(target_type: Any, value: Any) -> Any:
    if value is None:
        return None

    origin = get_origin(target_type)
    args = get_args(target_type)

    # Optional / Union[..., None]
    if origin is None and getattr(target_type, "__origin__", None) is None:
        # direct type
        if _is_attrs_class(target_type):
            return primitive_to_attrs(target_type, value)
        if isinstance(target_type, type) and issubclass(target_type, Enum):
            return target_type(value)
        if target_type is datetime:
            return datetime.fromisoformat(value)
        if target_type is uuid.UUID:
            return uuid.UUID(value)
        return value

    if origin is list or origin is List:
        (elem_type,) = args
        return [_convert_value(elem_type, v) for v in value]

    if origin is dict or origin is Dict:
        key_t, val_t = args
        return {k: _convert_value(val_t, v) for k, v in value.items()}

    # Optional[T] maps to Union[T, NoneType]
    if origin is tuple and len(args) == 2 and type(None) in args:
        non_none = args[0] if args[1] is type(None) else args[1]
        return _convert_value(non_none, value)

    # Fallback
    return value


def primitive_to_attrs(cls: Type, data: Any) -> Any:
    """Reconstruct an attrs class `cls` from primitive data (dict/list).

    This will attempt to map nested structures to nested attrs classes using
    type annotations on `cls`.
    """
    if data is None:
        return None

    if _is_attrs_class(cls):
        if not isinstance(data, dict):
            raise TypeError(
                f"Expected dict to construct {cls.__name__}, got {type(data)}"
            )
        kwargs = {}
        # Use attr metadata to discover all fields including inherited ones
        type_hints = get_type_hints(cls)
        for field in attr.fields(cls):
            field_name = field.name
            if field_name not in data:
                continue
            raw = data[field_name]
            field_type = type_hints.get(field_name, None)
            try:
                if field_type is not None:
                    kwargs[field_name] = _convert_value(field_type, raw)
                else:
                    kwargs[field_name] = raw
            except Exception:
                kwargs[field_name] = raw
        return cls(**kwargs)

    # If target is Enum
    if isinstance(cls, type) and issubclass(cls, Enum):
        return cls(data)

    # Built-in converters
    if cls is datetime:
        return datetime.fromisoformat(data)
    if cls is uuid.UUID:
        return uuid.UUID(data)

    # Nothing to do
    return data


def encode_attrs(obj: Any) -> bytes:
    """Encode an attrs object (possibly nested) to JSON bytes via msgspec."""
    prim = attrs_to_primitive(obj)
    return msgspec.json.encode(prim)


def decode_attrs(cls: Type, data: bytes) -> Any:
    """Decode JSON bytes to an attrs object of type `cls` using msgspec."""
    decoded = msgspec.json.decode(data)
    return primitive_to_attrs(cls, decoded)


__all__ = [
    "attrs_to_primitive",
    "primitive_to_attrs",
    "encode_attrs",
    "decode_attrs",
]
