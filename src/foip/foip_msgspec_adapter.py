"""Messagepack/JSON adapter helpers under foip package."""

import msgspec
from typing import Any, Type
from datetime import datetime

from foip.shared_metadata_schema import (
    SECFiling,
    EntityID,
    TimePoint,
    EntityType,
    FormType,
)


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


def encode_attrs(obj: Any) -> bytes:
    """Compatibility wrapper used by tests."""
    # Special-case SECFiling and other simple containers that msgspec
    # can't serialize directly. Build a plain dict and encode to JSON bytes.
    if isinstance(obj, SECFiling):
        # local temporaries to satisfy mypy about Optional[datetime]
        eid_ts = getattr(obj.entity_id, "timestamp", None)
        tp_eff = getattr(obj.timepoint, "effective_date", None)
        tp_src = getattr(obj.timepoint, "source_timestamp", None)
        tp_rec = getattr(obj.timepoint, "recorded_date", None)

        data = {
            "entity_id": {
                "entity_type": obj.entity_id.entity_type.value,
                "identifier": obj.entity_id.identifier,
                "source": obj.entity_id.source,
                "timestamp": eid_ts.isoformat() if eid_ts is not None else None,
            },
            "timepoint": {
                "effective_date": tp_eff.isoformat() if tp_eff is not None else None,
                "source_timestamp": tp_src.isoformat() if tp_src is not None else None,
                "recorded_date": tp_rec.isoformat() if tp_rec is not None else None,
            },
            "form_type": obj.form_type.value if obj.form_type else None,
            "filing_date": obj.filing_date.isoformat() if obj.filing_date else None,
            "period_end_date": obj.period_end_date.isoformat() if obj.period_end_date else None,
            "accession_number": obj.accession_number,
            "amount": obj.amount,
            "currency": obj.currency,
            "description": obj.description,
            "file_number": getattr(obj, "file_number", None),
            "sections": getattr(obj, "sections", None),
            "metadata": getattr(obj, "metadata", {}),
        }
        return msgspec.json.encode(data)

    return encode(obj)


def decode_attrs(typ: Type[Any], b: bytes) -> Any:
    """Decode JSON bytes into an instance of `typ`.

    Currently supports `SECFiling` and basic nested reconstruction used in tests.
    """
    data = decode(b)
    # If msgspec returned bytes/str fallback, ensure we have a dict
    if not isinstance(data, dict):
        raise TypeError("decoded data is not a mapping")

    if typ is SECFiling:
        # reconstruct nested EntityID
        eid_d = data.get("entity_id", {})
        eid = EntityID(
            entity_type=EntityType(eid_d.get("entity_type")),
            identifier=eid_d.get("identifier"),
            source=eid_d.get("source"),
        )
        tp_d = data.get("timepoint", {})
        tp = TimePoint(
            effective_date=(
                datetime.fromisoformat(tp_d.get("effective_date"))
                if tp_d.get("effective_date")
                else None
            ),
            source_timestamp=(
                datetime.fromisoformat(tp_d.get("source_timestamp"))
                if tp_d.get("source_timestamp")
                else None
            ),
        )
        form_type = FormType(data.get("form_type")) if data.get("form_type") else None
        filing_date = datetime.fromisoformat(data["filing_date"]) if data.get("filing_date") else None
        period_end_date = datetime.fromisoformat(data["period_end_date"]) if data.get("period_end_date") else None

        return SECFiling(
            entity_id=eid,
            timepoint=tp,
            form_type=form_type,
            filing_date=filing_date,
            period_end_date=period_end_date,
            accession_number=data.get("accession_number"),
            amount=data.get("amount"),
            currency=data.get("currency"),
            description=data.get("description"),
            metadata=data.get("metadata", {}),
        )

    # fallback: try to construct with the dict directly
    return typ(**data)
