"""Health & Gate runner scaffold.

Runs static and dynamic checks (mypy, pytest, smoke-import) and emits a
structured report. By default this script only runs locally and does not create
issues or push changes; enable `FOIP_GATE_CREATE_ISSUE=yes` and provide
`FOIP_GITHUB_TOKEN` if you want it to open issues (not implemented by default).

Best practices:
- Run checks in a clean environment (CI) using Docker or GitHub Actions.
- Return non-zero exit code when gate fails to integrate into CI pipelines.
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import attr
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("foip.health_gate")


@attr.define
class GateResult:
    name: str
    ok: bool
    returncode: int
    output: str

    def as_dict(self) -> Dict[str, object]:
        return {"name": self.name, "ok": self.ok, "returncode": self.returncode, "output": self.output}


def run_cmd(cmd: str, *, cwd: Optional[Path] = None) -> GateResult:
    logger.info("running: %s", cmd)
    try:
        proc = subprocess.run(shlex.split(cmd), cwd=str(cwd) if cwd else None, capture_output=True, text=True)
        ok = proc.returncode == 0
        output = (proc.stdout or "") + (proc.stderr or "")
        return GateResult(cmd, ok, proc.returncode, output)
    except Exception as exc:
        return GateResult(cmd, False, 2, str(exc))


def run_checks() -> Dict[str, object]:
    results = []

    # 1) mypy
    results.append(run_cmd("python -m mypy src/foip"))

    # 2) pytest (ignore backup_v1)
    results.append(run_cmd("pytest -q --ignore=backup_v1"))

    # 3) smoke import
    results.append(run_cmd("python -c \"import foip; print('ok')\""))

    # Use timezone-aware UTC timestamp
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    summary = {"timestamp": timestamp, "results": [r.as_dict() for r in results]}
    return summary


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    summary = run_checks()
    # print JSON summary for CI consumption
    print(json.dumps(summary, indent=2))

    # Determine exit code: non-zero if any check failed
    any_failure = any(not r["ok"] for r in summary["results"])  # type: ignore[index]
    return 0 if not any_failure else 1


if __name__ == "__main__":
    raise SystemExit(main())
