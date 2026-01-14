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


@attr.define(kw_only=True)
class TimePoint:
    """Represents a time point used across the codebase.

    Provide both `effective_date` (used by tests) and optional
    `source_timestamp`/`recorded_date` to be compatible with different call sites.
    """
    effective_date: Optional[datetime] = None
    source_timestamp: Optional[datetime] = None
    recorded_date: Optional[datetime] = None


@attr.define(kw_only=True)
class Company:
    entity_id: EntityID
    name: str
    ticker: Optional[str] = None
    cik: Optional[str] = None


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


class SECFiling:
    """Flexible SECFiling container accepting extra keyword fields.

    This class intentionally accepts arbitrary keywords from different
    data sources and places unknown keys into the `metadata` dict so
    callers that construct SECFiling with extra fields (e.g. from
    parsers) don't raise errors.
    """

    def __init__(self, *args, **kwargs):
        # Known required fields - pop them, leaving extras for metadata
        self.entity_id: EntityID = kwargs.pop("entity_id")
        self.timepoint: TimePoint = kwargs.pop("timepoint")
        self.form_type: FormType = kwargs.pop("form_type")
        self.filing_date: datetime = kwargs.pop("filing_date")
        self.period_end_date: datetime = kwargs.pop("period_end_date")
        self.accession_number: str = kwargs.pop("accession_number")

        # Optional known fields
        self.amount: Optional[float] = kwargs.pop("amount", None)
        self.currency: Optional[str] = kwargs.pop("currency", None)
        self.description: Optional[str] = kwargs.pop("description", None)
        self.file_number: Optional[str] = kwargs.pop("file_number", None)
        self.sections: Optional[Any] = kwargs.pop("sections", None)

        # Remaining fields are stored in metadata for later inspection
        self.metadata: Dict[str, Any] = kwargs.pop("metadata", {})
        # include any truly unexpected extras into metadata as well
        if kwargs:
            self.metadata.update(kwargs)

    def __repr__(self) -> str:  # pragma: no cover - small helper
        return f"SECFiling({self.accession_number!r})"


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
