"""
Run the Go-based random workload runner and extract a simple JSON trace
from the generated Elle history (history.edn).

Pipeline:
  1. Invoke `go run ./cmd/runner -config <yaml> -out <history.edn>`.
  2. Parse the EDN history and collect all :query operations as a linear
     trace of {client_id, sql, type}.
  3. Write that trace to logs/trace_latest.json for replay_litmus.py.

Transaction length and key distribution come from the workload YAML, not this script.
Use workloads/fuzz.yaml for short txns (1-3 ops) and wide keys (1-100) to reduce deadlocks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from analyze_history import load_history  # type: ignore[import]

_UPDATE_PREFIX_RE = re.compile(r"^\s*UPDATE\b", re.IGNORECASE)
_INSERT_INTO_KV_RE = re.compile(r"^\s*INSERT\s+INTO\s+KV\b", re.IGNORECASE)
_WHERE_KEY_EQ_RE = re.compile(r"WHERE\s+K\s*=\s*\$([0-9]+)", re.IGNORECASE)
_WHERE_KEY_IN_RE = re.compile(r"WHERE\s+K\s+IN\s*\(([^)]*)\)", re.IGNORECASE)
_INSERT_KV_VALUES_RE = re.compile(
    r"INSERT\s+INTO\s+KV\s*\(([^)]*)\)\s*VALUES\s*\(([^)]*)\)",
    re.IGNORECASE,
)
_PLACEHOLDER_RE = re.compile(r"\$([0-9]+)")
_TXN_NUM_SUFFIX_RE = re.compile(r"(\d+)$")


def run_go_runner(config_path: Path, history_path: Path) -> None:
    """
    Call the Go runner with the given YAML config to produce history.edn.
    """
    cmd = [
        "go",
        "run",
        "./cmd/runner",
        "-config",
        str(config_path),
        "-out",
        str(history_path),
    ]
    print(f"[run_random] running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(ROOT), check=True)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _derive_txn_write_id(process: int, txn_id: str) -> int:
    """
    Derive a stable, transaction-unique integer write identifier.
    """
    m = _TXN_NUM_SUFFIX_RE.search(txn_id)
    if m:
        return ((process + 1) << 48) | int(m.group(1))
    digest = hashlib.sha256(f"{process}:{txn_id}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    return value if value > 0 else 1


def _normalize_key(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return stripped
        return value
    return value


def _split_sql_csv(section: str) -> List[str]:
    return [token.strip() for token in (section or "").split(",") if token.strip()]


def _coerce_sql_literal(token: str) -> Any:
    raw = (token or "").strip().strip("`\"")
    if not raw:
        return raw
    if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
        return raw[1:-1]
    try:
        return int(raw)
    except ValueError:
        return raw


def _extract_update_keys(sql: str, args: List[Any], value: Dict[str, Any]) -> List[Any]:
    key_values: List[Any] = []

    eq_match = _WHERE_KEY_EQ_RE.search(sql)
    if eq_match:
        key_idx = int(eq_match.group(1)) - 1
        if 0 <= key_idx < len(args):
            key_values.append(_normalize_key(args[key_idx]))
    else:
        in_match = _WHERE_KEY_IN_RE.search(sql)
        if in_match:
            for ph in _PLACEHOLDER_RE.findall(in_match.group(1)):
                key_idx = int(ph) - 1
                if 0 <= key_idx < len(args):
                    key_values.append(_normalize_key(args[key_idx]))

    if not key_values:
        elle_key = value.get("elle_key")
        if elle_key is not None:
            key_values.append(_normalize_key(elle_key))

    if not key_values and args:
        key_values.append(_normalize_key(args[-1]))

    deduped: List[Any] = []
    seen: set[str] = set()
    for key in key_values:
        marker = repr(key)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(key)
    return deduped


def _extract_insert_key(sql: str, args: List[Any], value: Dict[str, Any]) -> Any:
    match = _INSERT_KV_VALUES_RE.search(sql)
    if match:
        columns = [col.strip().strip("`\"").lower() for col in _split_sql_csv(match.group(1))]
        values = _split_sql_csv(match.group(2))
        if "k" in columns:
            col_idx = columns.index("k")
            if 0 <= col_idx < len(values):
                token = values[col_idx]
                ph = _PLACEHOLDER_RE.fullmatch(token)
                if ph:
                    arg_idx = int(ph.group(1)) - 1
                    if 0 <= arg_idx < len(args):
                        return _normalize_key(args[arg_idx])
                if token == "%s":
                    if 0 <= col_idx < len(args):
                        return _normalize_key(args[col_idx])
                literal = _coerce_sql_literal(token)
                if literal != "":
                    return _normalize_key(literal)
    elle_key = value.get("elle_key")
    if elle_key is not None:
        return _normalize_key(elle_key)
    if args:
        return _normalize_key(args[0])
    return None


def _rewrite_update_as_blind_overwrite(
    *,
    sql: str,
    args: List[Any],
    process: int,
    txn_id: str,
    value: Dict[str, Any],
) -> tuple[str, List[Any]]:
    keys = _extract_update_keys(sql, args, value)
    if not keys:
        # Fallback to a deterministic key to keep replay executable.
        keys = [((process % 100) + 1)]
    write_id = _derive_txn_write_id(process, txn_id)
    rewritten_args = [write_id, *keys]
    if len(keys) == 1:
        rewritten_sql = "UPDATE kv SET v=$1 WHERE k=$2"
    else:
        placeholders = ", ".join(f"${i + 2}" for i in range(len(keys)))
        rewritten_sql = f"UPDATE kv SET v=$1 WHERE k IN ({placeholders})"
    return rewritten_sql, rewritten_args


def _rewrite_insert_as_blind_overwrite(
    *,
    sql: str,
    args: List[Any],
    process: int,
    txn_id: str,
    value: Dict[str, Any],
) -> tuple[str, List[Any]]:
    key = _extract_insert_key(sql, args, value)
    if key is None:
        key = (process % 100) + 1
    write_id = _derive_txn_write_id(process, txn_id)
    # Keep replay SQL backend-portable by forcing the same blind-overwrite form used for UPDATE.
    return "UPDATE kv SET v=$1 WHERE k=$2", [write_id, key]


def history_to_trace(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Elle-style history events into a flat trace for replay:
      {client_id, sql, args, type} where type is "invoke" | "ok" | "fail".
    """
    trace: List[Dict[str, Any]] = []
    for idx, ev in enumerate(events):
        f = str(ev.get("f", "")).lstrip(":")
        if f != "query":
            continue
        ev_type = str(ev.get("type", "")).lower()
        if ev_type not in ("invoke", "ok", "fail"):
            continue
        val = ev.get("value") or {}
        if not isinstance(val, dict):
            continue
        sql = val.get("sql")
        if not sql:
            continue
        process = _coerce_int(ev.get("process", 0), default=0)
        raw_args = val.get("args")
        args = list(raw_args) if isinstance(raw_args, list) else []

        sql_str = str(sql)
        txn_token = str(ev.get("txn_id") or f"proc{process}-event{idx}")
        if _UPDATE_PREFIX_RE.match(sql_str):
            sql_str, args = _rewrite_update_as_blind_overwrite(
                sql=sql_str,
                args=args,
                process=process,
                txn_id=txn_token,
                value=val,
            )
        elif _INSERT_INTO_KV_RE.match(sql_str):
            sql_str, args = _rewrite_insert_as_blind_overwrite(
                sql=sql_str,
                args=args,
                process=process,
                txn_id=txn_token,
                value=val,
            )

        trace.append(
            {
                "client_id": process,
                "sql": sql_str,
                "args": args,
                "type": ev_type,
            }
        )
    return trace


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Go random workload runner and extract history.edn into JSON trace."
    )
    parser.add_argument(
        "--workload",
        "--config",
        dest="config",
        default="workloads/fuzz.yaml",
        help="YAML workload/config file for the Go runner.",
    )
    parser.add_argument(
        "--history-out",
        default="history.edn",
        help="Path where the Go runner should write Elle history (EDN).",
    )
    parser.add_argument(
        "--trace-out",
        default="logs/trace_latest.json",
        help="Path to write the extracted JSON trace.",
    )
    args = parser.parse_args()

    cfg_path = (ROOT / args.config).resolve()
    if not cfg_path.exists():
        print(f"[run_random] config not found: {cfg_path}", file=sys.stderr)
        return 2

    history_path = (ROOT / args.history_out).resolve()
    trace_path = (ROOT / args.trace_out).resolve()
    trace_path.parent.mkdir(parents=True, exist_ok=True)

    run_go_runner(cfg_path, history_path)

    print(f"[run_random] parsing history: {history_path}")
    events = load_history(history_path)
    trace = history_to_trace(events)
    print(f"[run_random] extracted {len(trace)} query events")

    with trace_path.open("w", encoding="utf-8") as f:
        json.dump(trace, f, indent=2)
    print(f"[run_random] wrote JSON trace to {trace_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
