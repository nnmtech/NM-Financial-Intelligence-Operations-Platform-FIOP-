"""Cleaned metadata schema for foip package (extended types)."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

import attr


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class EntityType(str, Enum):
    COMPANY = "company"
    FILING = "filing"
    TRANSACTION = "transaction"
    INVOICE = "invoice"
    ARTIFACT = "artifact"


@attr.define
class EntityID:
    entity_type: EntityType = attr.ib(validator=attr.validators.instance_of(EntityType))
    identifier: str = attr.ib(validator=attr.validators.instance_of(str))
    source: str = attr.ib(validator=attr.validators.instance_of(str))
    timestamp: datetime = attr.ib(factory=lambda: datetime.now(timezone.utc), converter=_ensure_utc)

    def __str__(self) -> str:
        return f"{self.entity_type.value}:{self.source}:{self.identifier}"


class FormType(str, Enum):
    TEN_K = "10-K"
    TEN_Q = "10-Q"


class RunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"


# TimePoint is a simple alias for datetime used across the codebase
TimePoint = datetime


@attr.define(kw_only=True)
class Company:
    entity_id: EntityID
    name: str


@attr.define(kw_only=True)
class FinancialEntity:
    entity_id: EntityID
    timepoint: datetime


@attr.define
class BankTransaction:
    transaction_id: str
    amount: Decimal
    date: datetime
    counterparty: Optional[str] = None
    metadata: Dict[str, Any] = attr.field(factory=dict)


@attr.define
class SECFiling:
    filing_id: str
    company: Company
    form_type: FormType
    filed_at: datetime
    raw_text: Optional[str] = None
    metadata: Dict[str, Any] = attr.field(factory=dict)


@attr.define
class EntityRelationship:
    # required (non-default) attributes must come before any attributes with defaults
    source_entity_id: EntityID = attr.ib()
    target_entity_id: EntityID = attr.ib()
    relationship_type: str = attr.ib()

    # defaults
    relationship_id: uuid.UUID = attr.ib(factory=uuid.uuid4)
    confidence: float = attr.ib(default=1.0)
    established_by_run: Optional[uuid.UUID] = attr.ib(default=None)
    established_at: Optional[datetime] = attr.ib(default=None, converter=lambda v: _ensure_utc(v) if v is not None else None)
    metadata: Dict[str, Any] = attr.field(factory=dict)


@attr.define
class Artifact:
    artifact_id: str
    artifact_type: str


@attr.define(kw_only=True)
class RunMetadata:
    run_id: uuid.UUID
    run_type: str
    trigger: str
    status: RunStatus
    invariants_check: Dict[str, Any] = attr.field(factory=dict)
    invariant_violations: List[str] = attr.field(factory=list)
