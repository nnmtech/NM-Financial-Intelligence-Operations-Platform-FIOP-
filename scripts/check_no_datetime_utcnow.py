#!/usr/bin/env python3
"""Simple lint checker: disallow `datetime.utcnow()` usage.

This script scans Python files in the repository and reports any
occurrences of the literal `datetime.utcnow(` which produces naive
UTC datetimes. The project standard is to use timezone-aware datetimes
via `datetime.now(timezone.utc)` or normalize using helpers.
"""
import sys
from pathlib import Path


def scan(root: Path) -> int:
    matches = []
    for p in root.rglob("*.py"):
        # Skip virtualenv and hidden folders
        if any(
            part.startswith(".") or part == "venv" or part == ".venv"
            for part in p.parts
        ):
            continue
        # Skip the checker script itself (and any copies) to avoid self-reporting
        if p.name == Path(__file__).name:
            continue
        try:
            text = p.read_text(encoding="utf8")
        except Exception:
            continue
        if "datetime.utcnow(" in text:
            # Report line numbers
            for i, line in enumerate(text.splitlines(), start=1):
                if "datetime.utcnow(" in line:
                    matches.append((p.relative_to(root), i, line.strip()))

    if matches:
        print(
            "Found datetime.utcnow() usages (prefer timezone-aware datetimes):",
            file=sys.stderr,
        )
        for p, ln, snippet in matches:
            print(f"  {p}:{ln}: {snippet}", file=sys.stderr)
        return 2

    print("No datetime.utcnow() occurrences found.")
    return 0


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    raise_code = scan(root)
    sys.exit(raise_code)
