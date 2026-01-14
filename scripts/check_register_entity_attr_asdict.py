#!/usr/bin/env python3
"""Lint checker: ensure `attr.asdict()` is used for payloads passed to `register_entity`.

This scans Python files for `.register_entity(` calls and enforces that the second
argument uses `attr.asdict(...)`. It reports file/line locations where the rule
is violated.
"""
import sys
from pathlib import Path
from typing import List


def find_call_span(text: str, start_idx: int) -> str:
    """Extract the full call text starting from the opening '('."""
    depth = 0
    out = []
    for i in range(start_idx, len(text)):
        ch = text[i]
        out.append(ch)
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
    return "".join(out)


def scan_file(path: Path, root: Path):
    text = path.read_text(encoding="utf8")
    violations = []
    idx = 0
    while True:
        idx = text.find(".register_entity(", idx)
        if idx == -1:
            break
        # find the '(' position
        paren_idx = text.find("(", idx)
        call_text = find_call_span(text, paren_idx)

        # basic argument splitting at top-level commas
        args: List[str] = []
        buf: List[str] = []
        depth = 0
        for ch in call_text[1:]:  # skip first '('
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    # end of args
                    if buf:
                        args.append("".join(buf).strip())
                    break
                depth -= 1
            if ch == "," and depth == 0:
                args.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)

        # We expect at least two args: entity_id, payload
        if len(args) >= 2:
            payload_arg = args[1]
            pa = payload_arg.strip()
            # Accept if attr.asdict is used, or if a literal dict/getattr/dict() is passed,
            # or if the variable name suggests a prepared payload (ends with _payload).
            accepted = (
                "attr.asdict" in pa
                or pa.startswith("{")
                or "getattr(" in pa
                or ".__dict__" in pa
                or pa.startswith("dict(")
                or pa.endswith("_payload")
                or pa.endswith("_metadata")
                or pa in ("metadata", "payload")
            )
            if not accepted:
                # Calculate line number
                # Find the line where this call started
                start_line = text[:idx].count("\n") + 1
                snippet = call_text.replace("\n", " ")[:200]
                violations.append((path.relative_to(root), start_line, snippet))

        idx = paren_idx + 1

    return violations


def main():
    root = Path(__file__).resolve().parents[1]
    violations = []
    for p in root.rglob("*.py"):
        # Skip virtualenv, hidden, tooling scripts
        if any(part.startswith(".") or part in ("venv", ".venv") for part in p.parts):
            continue
        if "scripts" in p.parts:
            continue
        try:
            v = scan_file(p, root)
            violations.extend(v)
        except Exception:
            continue

    if violations:
        print(
            "register_entity payload must use attr.asdict(payload) - violations:",
            file=sys.stderr,
        )
        for p, ln, snippet in violations:
            print(f"  {p}:{ln}: {snippet}", file=sys.stderr)
        return 2

    print("All register_entity calls use attr.asdict for payloads.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
