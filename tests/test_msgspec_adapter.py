import json
import uuid
from datetime import datetime, timezone

from foip_msgspec_adapter import decode_attrs, encode_attrs
from shared_metadata_schema import (EntityID, EntityType, FormType, SECFiling,
                                    TimePoint)


def test_roundtrip_secfiling_nested():
    eid = EntityID(entity_type=EntityType.COMPANY, identifier="C-123", source="erp")
    tp = TimePoint(effective_date=datetime(2025, 1, 1, tzinfo=timezone.utc))

    filing = SECFiling(
        entity_id=eid,
        timepoint=tp,
        amount=None,
        currency="USD",
        description="Test filing",
        metadata={"source": "test"},
        form_type=FormType.TEN_K,
        filing_date=datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc),
        period_end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
        accession_number="0000-ACC-1",
    )

    data = encode_attrs(filing)
    assert isinstance(data, (bytes, bytearray))

    obj = decode_attrs(SECFiling, data)
    assert isinstance(obj, SECFiling)
    assert obj.form_type == FormType.TEN_K
    assert obj.entity_id.identifier == "C-123"
    assert obj.timepoint.effective_date == tp.effective_date


def test_encode_contains_expected_fields():
    eid = EntityID(entity_type=EntityType.COMPANY, identifier="C-xyz", source="sec")
    tp = TimePoint(effective_date=datetime(2024, 6, 1, tzinfo=timezone.utc))
    filing = SECFiling(
        entity_id=eid,
        timepoint=tp,
        amount=100.0,
        currency="USD",
        description="Encode test",
        metadata={"k": "v"},
        form_type=FormType.TEN_Q,
        filing_date=datetime(2024, 6, 2, tzinfo=timezone.utc),
        period_end_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
        accession_number="ACC-2",
    )

    data = encode_attrs(filing)
    decoded = json.loads(data)
    # spot check some keys exist
    assert decoded["entity_id"]["identifier"] == "C-xyz"
    assert decoded["form_type"] == "10-Q"
    assert decoded["metadata"]["k"] == "v"
