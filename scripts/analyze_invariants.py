"""
Analyze corpus state outputs for SUM(v) invariant violations.

Usage:
  python scripts/analyze_invariants.py
  python scripts/analyze_invariants.py --corpus corpus/fuzz/postgres
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[1]


def sum_kv_values(output: Any) -> Optional[int]:
    """
    Convert replay output rows ([[k, v], ...]) into a SUM(v).
    Returns None if output is not a compatible numeric KV row list.
    """
    if not isinstance(output, list):
        return None
    total = 0
    for row in output:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            return None
        try:
            total += int(row[1])
        except (TypeError, ValueError):
            return None
    return total


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan corpus JSON states and report distinct SUM(v) totals."
    )
    parser.add_argument(
        "--corpus",
        default=str(ROOT / "corpus"),
        help="Corpus root directory to scan recursively.",
    )
    args = parser.parse_args()

    corpus_root = Path(args.corpus)
    if not corpus_root.exists():
        print(f"Corpus path not found: {corpus_root}", file=sys.stderr)
        return 2

    total_json_files = 0
    files_with_output = 0
    valid_state_files = 0
    skipped_files = 0
    sum_counts: Dict[int, int] = {}

    for path in sorted(corpus_root.rglob("*.json")):
        total_json_files += 1
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            skipped_files += 1
            continue

        if not isinstance(data, dict) or "output" not in data:
            continue
        files_with_output += 1

        total = sum_kv_values(data.get("output"))
        if total is None:
            skipped_files += 1
            continue

        valid_state_files += 1
        sum_counts[total] = sum_counts.get(total, 0) + 1

    distinct_sums = len(sum_counts)
    print(f"Corpus: {corpus_root}")
    print(f"JSON files scanned: {total_json_files}")
    print(f"Files with output: {files_with_output}")
    print(f"Valid state outputs: {valid_state_files}")
    print(f"Skipped/invalid files: {skipped_files}")
    print(f"Distinct SUM(v): {distinct_sums}")

    for total in sorted(sum_counts):
        print(f"  SUM(v)={total}: {sum_counts[total]} file(s)")

    if distinct_sums > 1:
        print("⚠️ INVARIANT VIOLATION FOUND: Concurrency altered the data total.")
    elif distinct_sums == 1:
        print("Invariant holds: all sampled states have the same SUM(v).")
    else:
        print("No valid state outputs found in corpus.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
