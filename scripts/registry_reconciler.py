"""Registry reconciler scaffold.

Compares external vendor IDs with the local `shared_metadata_registry` and
produces a reconciliation report. By default it runs in dry-run mode and only
reports differences; enable `FOIP_DO_RECONCILE=yes` to attempt automatic fixes
(not implemented by default; the script will show where hooks should be
added).
"""
from __future__ import annotations

import logging
import os
import attr
from typing import Dict, List, Optional

logger = logging.getLogger("foip.registry_reconciler")

try:
    from foip import shared_metadata_registry as registry
except Exception:  # pragma: no cover - best-effort import
    registry = None


@attr.define
class ReconcileResult:
    entity_id: str
    local_ids: Dict[str, str]
    external_ids: Dict[str, str]
    mismatch: bool


def fetch_external_ids(entity_id: str) -> Dict[str, str]:
    """Placeholder: fetch external IDs for `entity_id` from vendor APIs.

    For now this returns an empty dict; replace with actual vendor calls.
    """
    # TODO: implement vendor adapters
    return {}


def reconcile_once(entity_ids: Optional[List[str]] = None) -> List[ReconcileResult]:
    """Run a single reconciliation pass.

    - If `entity_ids` is None, iterate over all entities in the registry.
    - Returns a list of `ReconcileResult` describing mismatches.
    """
    if registry is None:
        logger.warning("shared_metadata_registry not available; nothing to reconcile")
        return []

    to_check = entity_ids or list(registry.iter_entity_ids()) if hasattr(registry, "iter_entity_ids") else []
    results: List[ReconcileResult] = []

    for eid in to_check:
        try:
            local = registry.get_ids(eid) if hasattr(registry, "get_ids") else {}
        except Exception:
            logger.exception("failed to get local ids for %s", eid)
            local = {}

        external = fetch_external_ids(eid)
        mismatch = local != external
        results.append(ReconcileResult(entity_id=eid, local_ids=local, external_ids=external, mismatch=mismatch))

    return results


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    entity = os.environ.get("FOIP_RECONCILE_ENTITY")
    do_reconcile = os.environ.get("FOIP_DO_RECONCILE", "no").lower() in ("1", "yes", "true")

    ids = [entity] if entity else None
    results = reconcile_once(ids)
    for r in results:
        logger.info("reconcile %s mismatch=%s local=%s external=%s", r.entity_id, r.mismatch, r.local_ids, r.external_ids)

    if do_reconcile:
        logger.info("reconcile run requested changes: the scaffold does not auto-fix by default")

    # exit code 0 even if mismatches found; CI can enforce policies by examining output
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
