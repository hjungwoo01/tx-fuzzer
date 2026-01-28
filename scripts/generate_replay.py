"""
Generate a deterministic Replay config from a BEC-1 profile (interleave strategy).

Converts p1_name (Reader) and p2_name (Writer) plus target_key into a strict
Lost Update (G1) interleaving schedule for the Go runner's replay mode.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except ImportError as exc:
    raise SystemExit("[ERROR] Missing dependency pyyaml. Install with `pip install pyyaml`.") from exc


def _iter_steps(txn: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return list of steps that may have args (steps then queries)."""
    out: List[Dict[str, Any]] = []
    for step in txn.get("steps", []):
        if isinstance(step, dict):
            out.append(step)
    for step in txn.get("queries", []):
        if isinstance(step, dict):
            out.append(step)
    return out


def _steps_to_read_and_write(txn: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    """(1-based step count to first read on a key, 1-based step count to first write)."""
    steps_to_read: Optional[int] = None
    steps_to_write: Optional[int] = None
    for i, step in enumerate(_iter_steps(txn), start=1):
        elle = step.get("elle") if isinstance(step, dict) else None
        if not isinstance(elle, dict) or "key" not in elle:
            continue
        f = str(elle.get("f", "")).lower().strip(":").replace("-", "")
        if f in ("read", "r") and steps_to_read is None:
            steps_to_read = i
        if f in ("write", "w") and steps_to_write is None:
            steps_to_write = i
    return (steps_to_read, steps_to_write)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert BEC-1 profile into a deterministic Lost Update (G1) replay YAML."
    )
    parser.add_argument("--base", type=Path, required=True, help="Base workload YAML (e.g. diluted_skew.yaml).")
    parser.add_argument("--profile", type=Path, required=True, help="bec1_target.json (p1_name, p2_name, target_key).")
    parser.add_argument("--out", type=Path, required=True, help="Output replay config path.")
    args = parser.parse_args()

    if not args.base.exists():
        print(f"[generate_replay] ERROR: Base not found: {args.base}", file=sys.stderr)
        return 1
    if not args.profile.exists():
        print(f"[generate_replay] ERROR: Profile not found: {args.profile}", file=sys.stderr)
        return 1

    with args.base.open(encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    with args.profile.open(encoding="utf-8") as f:
        profile = json.load(f)

    target_key = profile.get("target_key")
    p1_name = profile.get("p1_name")
    p2_name = profile.get("p2_name")

    if not (p1_name and p2_name and target_key is not None):
        print(f"[Skip] Profile missing actors. Profile: {profile}", file=sys.stderr)
        sys.exit(0)

    # Normalize key type
    if isinstance(target_key, str) and target_key.isdigit():
        try:
            target_key = int(target_key)
        except ValueError:
            pass

    txns = (config.get("workload") or {}).get("transactions") or []

    def make_fixed_txn(name: str, new_name: str) -> Dict[str, Any]:
        tmpl = next((t for t in txns if t.get("name") == name), None)
        if not tmpl:
            raise ValueError(f"Txn {name} not found in base workload")
        new_txn = json.loads(json.dumps(tmpl))
        new_txn["name"] = new_name
        for step in _iter_steps(new_txn):
            if "args" in step and isinstance(step["args"], list) and step["args"]:
                step["args"][0] = target_key
        return new_txn

    try:
        fixed_p1 = make_fixed_txn(p1_name, "p1_fixed")
        fixed_p2 = make_fixed_txn(p2_name, "p2_fixed")
    except ValueError as e:
        print(f"[generate_replay] ERROR: {e}", file=sys.stderr)
        return 1

    # Step counts for standard Read-then-Write (for schedule step counts)
    p1_read, p1_write = _steps_to_read_and_write(fixed_p1)
    p2_read, p2_write = _steps_to_read_and_write(fixed_p2)
    if p1_read is None or p1_write is None or p2_read is None or p2_write is None:
        print("[generate_replay] Skip: both txns need at least one read and one write on a key (will fall back to key-flood).", file=sys.stderr)
        sys.exit(0)
    if p1_read > p1_write or p2_read > p2_write:
        print("[generate_replay] Skip: first read must come before first write in both txns (will fall back to key-flood).", file=sys.stderr)
        sys.exit(0)

    # Lost Update (G1) interleaving (Go runner format: alias, action, steps):
    #   Client 0: Start P1 → Step P1 (Read) [Pause P1]
    #   Client 1: Start P2 → Step P2 (Read) → Step P2 (Write) [P2 overwrites P1's snapshot view]
    #   Client 0: Step P1 (Write) [P1 overwrites P2 blindly] → Commit
    #   Client 1: Commit
    d_p2_write = max(1, p2_write - p2_read)
    d_p1_write = max(1, p1_write - p1_read)
    replay_schedule: List[Dict[str, Any]] = [
        {"alias": "P1", "action": "step", "steps": p1_read},   # P1 Read [Pause P1]
        {"alias": "P2", "action": "step", "steps": p2_read},   # P2 Read
        {"alias": "P2", "action": "step", "steps": d_p2_write},  # P2 Write - overwrites P1's snapshot view
        {"alias": "P1", "action": "step", "steps": d_p1_write},  # P1 Write (blind) - Lost Update
        {"alias": "P1", "action": "commit"},
        {"alias": "P2", "action": "commit"},
    ]

    output_config: Dict[str, Any] = {
        "database": config.get("database", {}),
        "workload": {
            "datatype": (config.get("workload") or {}).get("datatype", "register"),
            "transactions": [fixed_p1, fixed_p2],
        },
        "scheduling": {
            "duration": "1s",
            "clients": 2,
        },
        "replay": {
            "enabled": True,
            "transactions": [
                {"alias": "P1", "txn": "p1_fixed", "process": 0},
                {"alias": "P2", "txn": "p2_fixed", "process": 1},
            ],
            "schedule": replay_schedule,
        },
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        yaml.safe_dump(output_config, f, sort_keys=False, default_flow_style=False)

    print(f"Replay generated for {p1_name} vs {p2_name} on key={target_key}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
