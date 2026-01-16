"""Feature-extract pipeline scaffold.

Consumes new filings from the storage/ingestor interfaces, extracts a small
typed feature record for each filing, and writes feature files to `out_dir`.
Dry-run by default: writing occurs only when `FOIP_DO_EXTRACT=yes`.

Design notes:
- Keep extraction deterministic and unit-testable.
- Feature extractor is a pure function to ease testing and ML reproducibility.
"""
from __future__ import annotations

import json
import logging
import os
import attr
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger("foip.feature_extract")

try:
    from foip import shared_storage_worm as storage
except Exception:  # pragma: no cover - optional import
    storage = None


@attr.define
class FeatureRecord:
    entity_id: str
    filing_id: str
    word_count: int
    extracted_at: str


def extract_features_from_text(entity_id: str, filing_id: str, text: str) -> FeatureRecord:
    """Deterministic feature extraction from filing text.

    This is intentionally minimal: word count and timestamps. Extend with
    domain-specific tokenizers or NLP models as needed.
    """
    words = len(text.split()) if text else 0
    # Use timezone-aware UTC timestamp instead of deprecated utcnow()
    # Preferred expression: datetime.now(timezone.utc).isoformat()
    # Use RFC3339 'Z' suffix for UTC to avoid '+00:00' offset.
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return FeatureRecord(
        entity_id=entity_id,
        filing_id=filing_id,
        word_count=words,
        extracted_at=ts,
    )


def process_new_filings(out_dir: Path, entity_ids: Optional[Iterable[str]] = None, *, do_write: bool = False) -> List[Path]:
    """Process filings and write feature records to `out_dir`.

    - If `storage` is available, it should provide an iterator or API to list
      recent filings. This scaffold looks for `storage.iter_filings()` as a
      hook; otherwise it is a no-op.
    - `do_write` controls whether feature files are actually written.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []

    if storage is None:
        logger.warning("storage adapter missing; skipping processing")
        return written

    iterator = getattr(storage, "iter_filings", None)
    if iterator is None:
        logger.warning("storage.iter_filings hook not found; skipping")
        return written

    for filing in iterator(entity_ids) if entity_ids else iterator():
        # Expect filing to be a mapping-like object with fields
        try:
            entity_id = filing.get("entity_id")
            filing_id = filing.get("filing_id")
            text = filing.get("text", "")
        except Exception:
            logger.exception("invalid filing received: %r", filing)
            continue

        rec = extract_features_from_text(entity_id, filing_id, text)
        if do_write:
            out_path = out_dir / f"features_{entity_id}_{filing_id}.json"
            out_path.write_text(json.dumps(attr.asdict(rec), ensure_ascii=False))
            written.append(out_path)
            logger.info("wrote feature %s", out_path)

    return written


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="out/features")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    do_write = args.write or os.environ.get("FOIP_DO_EXTRACT", "no").lower() in ("1", "yes", "true")
    metas = process_new_filings(Path(args.out), do_write=do_write)
    logger.info("processed %d feature files", len(metas))
