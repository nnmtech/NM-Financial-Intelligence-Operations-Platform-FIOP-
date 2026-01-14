"""Minimal foip infra (structlog binding) inside package."""

import structlog
from typing import Any


def get_logger(name: str = __name__) -> Any:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )
    return structlog.get_logger(name)


log = get_logger()
