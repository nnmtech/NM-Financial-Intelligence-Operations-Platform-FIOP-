"""Unified Metadata Schema for FIOP Platform
Refactored to use `attrs` for declarative, lightweight models.

Notes:
- Uses `attrs` for models and validation/converters.
- Datetimes are normalized to UTC and stored as timezone-aware values.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import attr

# ========== HELPERS & NORMALIZATION ==========


def _ensure_utc(dt: datetime) -> datetime:
    if dt is None:
        return dt
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ========== CORE ENTITY IDENTIFIERS ==========


class EntityType(str, Enum):
    COMPANY = "company"
    FILING = "filing"
    TRANSACTION = "transaction"
    INVOICE = "invoice"
    VENDOR = "vendor"
    CUSTOMER = "customer"
    ACCOUNT = "account"
    REGULATION = "regulation"
    RISK_CLASS = "risk_class"
    RUN = "run"
    ARTIFACT = "artifact"


@attr.define
class EntityID:
    """Standardized entity identifier."""

    entity_type: EntityType = attr.ib(validator=attr.validators.instance_of(EntityType))
    identifier: str = attr.ib(validator=attr.validators.instance_of(str))
    source: str = attr.ib(
        validator=attr.validators.instance_of(str)
    )  # "sec", "bank", "erp", "internal"
    timestamp: datetime = attr.ib(
        factory=lambda: datetime.now(timezone.utc), converter=_ensure_utc
    )

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.entity_type.value}:{self.source}:{self.identifier}"


# ========== COMMON ENUMS ==========


class FormType(str, Enum):
    TEN_K = "10-K"
    TEN_Q = "10-Q"
    EIGHT_K = "8-K"
    TWENTY_F = "20-F"
    SIX_K = "6-K"
    S1 = "S-1"
    S3 = "S-3"
    S4 = "S-4"
    S8 = "S-8"
    DEF_14A = "DEF 14A"


class TransactionStatus(str, Enum):
    PENDING = "pending"
    MATCHED = "matched"
    RECONCILED = "reconciled"
    EXCEPTION = "exception"
    FLAGGED = "flagged"


class InvoiceStatus(str, Enum):
    RECEIVED = "received"
    VALIDATED = "validated"
    APPROVED = "approved"
    PAID = "paid"


class RunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


# ========== TIME-BASED VERSIONING ==========


@attr.define
class TimePoint:
    """Immutable point-in-time info for data records."""

    effective_date: datetime = attr.ib(converter=_ensure_utc)
    recorded_date: datetime = attr.ib(
        factory=lambda: datetime.now(timezone.utc), converter=_ensure_utc
    )
    source_timestamp: Optional[datetime] = attr.ib(
        default=None, converter=lambda v: _ensure_utc(v) if v is not None else None
    )
    run_id: Optional[uuid.UUID] = attr.ib(default=None)


# ========== CORE ENTITY MODELS ==========


@attr.define(kw_only=True)
class Company:
    """Unified company representation across SEC and operations."""

    entity_id: EntityID
    name: str
    ticker: Optional[str] = None
    cik: Optional[str] = None
    ein: Optional[str] = None
    duns: Optional[str] = None
    legal_name: Optional[str] = None
    jurisdiction: Optional[str] = None
    industry_naics: Optional[str] = None

    # Financial operations metadata
    vendor_id: Optional[str] = None
    customer_id: Optional[str] = None
    bank_accounts: List[str] = attr.field(factory=list)


@attr.define(kw_only=True)
class FinancialEntity:
    """Base class for financial entities."""

    entity_id: EntityID
    timepoint: TimePoint
    amount: Optional[float] = None
    currency: str = "USD"
    description: Optional[str] = None
    metadata: Dict[str, Any] = attr.field(factory=dict)

    # Cross-references
    company_entity_id: Optional[EntityID] = None
    related_entity_ids: List[EntityID] = attr.field(factory=list)


# ========== SEC-SPECIFIC MODELS ==========


@attr.define(kw_only=True)
class SECFiling(FinancialEntity):
    """SEC filing with operations cross-references."""

    form_type: FormType
    filing_date: datetime = attr.ib(converter=_ensure_utc)
    period_end_date: datetime = attr.ib(converter=_ensure_utc)
    accession_number: str
    file_number: Optional[str] = None

    sections: Dict[str, str] = attr.field(factory=dict)
    risk_clauses: List[str] = attr.field(factory=list)

    related_transactions: List[EntityID] = attr.field(factory=list)
    compliance_reports: List[EntityID] = attr.field(factory=list)


# ========== OPERATIONS MODELS ==========


@attr.define(kw_only=True)
class BankTransaction(FinancialEntity):
    """Bank transaction with SEC intelligence links."""

    transaction_id: str
    bank_account: str
    counterparty: str
    transaction_type: str  # simple freeform or extend to Enum if desired

    status: TransactionStatus = TransactionStatus.PENDING
    match_confidence: Optional[float] = None
    matched_ledger_id: Optional[EntityID] = None

    sec_risk_score: Optional[float] = None
    recent_filing_links: List[EntityID] = attr.field(factory=list)


@attr.define(kw_only=True)
class Invoice(FinancialEntity):
    """Invoice with vendor risk assessment."""

    invoice_number: str
    vendor_entity_id: EntityID
    po_number: Optional[str] = None
    due_date: datetime = attr.ib(converter=_ensure_utc)
    paid_date: Optional[datetime] = attr.ib(
        default=None, converter=lambda v: _ensure_utc(v) if v is not None else None
    )

    line_items: List[Dict[str, Any]] = attr.field(factory=list)
    tax_amount: float = 0.0
    total_amount: float = attr.ib()

    status: InvoiceStatus = InvoiceStatus.RECEIVED
    approval_workflow: Optional[str] = None

    vendor_risk_assessment: Optional[Dict[str, Any]] = None
    sec_disclosure_alerts: List[str] = attr.field(factory=list)


# ========== RUN & ARTIFACT MODELS ==========


@attr.define(kw_only=True, frozen=True)
class RunMetadata:
    """Immutable run record for both FinScan and Operations."""

    run_id: uuid.UUID = attr.ib(factory=uuid.uuid4)
    run_type: str
    trigger: str
    started_at: datetime = attr.ib(
        factory=lambda: datetime.now(timezone.utc), converter=_ensure_utc
    )
    completed_at: Optional[datetime] = attr.ib(
        default=None, converter=lambda v: _ensure_utc(v) if v is not None else None
    )
    status: RunStatus = RunStatus.RUNNING

    estimated_cost: float = 0.0
    actual_cost: Optional[float] = None
    cost_breakdown: Dict[str, float] = attr.field(factory=dict)

    invariants_check: Dict[str, bool] = attr.field(factory=dict)
    invariant_violations: List[str] = attr.field(factory=list)

    input_entities: List[EntityID] = attr.field(factory=list)
    output_artifacts: List[EntityID] = attr.field(factory=list)


@attr.define(kw_only=True, frozen=True)
class Artifact:
    """Immutable artifact in WORM storage."""

    artifact_id: EntityID
    run_id: uuid.UUID
    artifact_type: str
    content_hash: str
    storage_path: str
    created_at: datetime = attr.ib(
        factory=lambda: datetime.now(timezone.utc), converter=_ensure_utc
    )

    source_entity_ids: List[EntityID] = attr.field(factory=list)
    derived_entity_ids: List[EntityID] = attr.field(factory=list)


# ========== CROSS-DOMAIN RELATIONSHIPS ==========


@attr.define(kw_only=True, frozen=True)
class EntityRelationship:
    """Relationships between entities across domains."""

    relationship_id: uuid.UUID = attr.ib(factory=uuid.uuid4)
    source_entity_id: EntityID
    target_entity_id: EntityID
    relationship_type: str
    confidence: float = 1.0
    established_by_run: uuid.UUID
    established_at: datetime = attr.ib(
        factory=lambda: datetime.now(timezone.utc), converter=_ensure_utc
    )
    metadata: Dict[str, Any] = attr.field(factory=dict)


# ========== SCHEMA VALIDATION ==========


def validate_cross_references(entity: FinancialEntity) -> List[str]:
    """Validate cross-domain references in an entity."""
    violations: List[str] = []
    for ref in entity.related_entity_ids:
        if (
            entity.entity_id.entity_type == EntityType.FILING
            and ref.entity_type == EntityType.TRANSACTION
        ):
            violations.append(f"Filing directly references transaction: {ref}")
    return violations


def create_entity_id(
    entity_type: EntityType,
    identifier: str,
    source: str,
    timestamp: Optional[datetime] = None,
) -> EntityID:
    """Factory for creating standardized `EntityID` values."""
    return EntityID(
        entity_type=entity_type,
        identifier=identifier,
        source=source,
        timestamp=timestamp or datetime.now(timezone.utc),
    )
