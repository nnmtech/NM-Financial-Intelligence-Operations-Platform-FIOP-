"""Auto-remediation bot scaffold (safe / non-destructive).

This scaffold detects failing tests on a branch and suggests minimal fixes.
It is intentionally conservative: by default it only reports problems and the
recommended commands to run. Enable `FOIP_DO_REMEDIATE=yes` to allow the
script to run formatters (`black`) or linters auto-fixes (`ruff --fix`) locally
— but it will not create commits or push changes unless explicitly wired.

Design notes / best practices:
- Run in CI using a bot account with tight permissions if enabling autofix.
- Make remediations idempotent and small (formatting/typing fixes), avoid
  semantic changes.
"""
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import attr
from typing import List

logger = logging.getLogger("foip.auto_remediate")


@attr.define
class Failure:
    name: str
    returncode: int
    output: str


def run_cmd(cmd: str) -> Failure:
    try:
        proc = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
        out = (proc.stdout or "") + (proc.stderr or "")
        return Failure(cmd, proc.returncode, out)
    except Exception as exc:
        return Failure(cmd, 2, str(exc))


def detect_failing_tests() -> List[Failure]:
    # Run pytest (quiet) and capture failures
    res = run_cmd("pytest -q --maxfail=1")
    failures: List[Failure] = []
    if res.returncode != 0:
        failures.append(res)
    return failures


def suggest_remediations(failures: List[Failure]) -> List[str]:
    suggestions: List[str] = []
    for f in failures:
        # Simple heuristics
        if "E   AssertionError" in f.output or "assert" in f.output:
            suggestions.append("Run targeted test or inspect assertion; consider adding a deterministic fixture or adjust expected value")
        if "SyntaxError" in f.output or "IndentationError" in f.output:
            suggestions.append("Run 'python -m pyflakes' or open the file to fix syntax issues")
        if "type" in f.output.lower() or "mypy" in f.name:
            suggestions.append("Run 'python -m mypy src/foip' and fix type errors; consider adding type annotations")
        # generic suggestions
        suggestions.append("Run 'black .' and 'ruff --fix .' locally, then re-run tests")
    return suggestions


def attempt_autofix(run_autofix: bool) -> List[Failure]:
    results: List[Failure] = []
    if not run_autofix:
        return results

    # Try code formatters / linters if available
    results.append(run_cmd("black ."))
    results.append(run_cmd("ruff --fix ."))
    return results


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    failures = detect_failing_tests()
    if not failures:
        print(json.dumps({"ok": True, "failures": []}))
        return 0

    suggestions = suggest_remediations(failures)
    run_autofix = os.environ.get("FOIP_DO_REMEDIATE", "no").lower() in ("1", "yes", "true")
    autofix_results = attempt_autofix(run_autofix)

    report = {"ok": False, "failures": [attr.asdict(f) for f in failures], "suggestions": suggestions, "autofix_results": [attr.asdict(r) for r in autofix_results]}
    print(json.dumps(report, indent=2)[:20000])
    # Non-zero exit to indicate CI should block merges
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
