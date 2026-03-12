"""
Coverage-Guided Evolutionary Fuzzing Campaign: run_random → replay_litmus → corpus.

Loop:
  1. Generation: baseline trace with run_random.py (or load from corpus).
  2. Injection: mutate (timing, reorder, data).
  3. Execution: replay with replay_litmus.py; capture final kv state or error signature.
  4. Observation: hash output (state or error signature).
  5. Retention: if hash is new, save trace to corpus and add to population queue.
  6. Evolution: next iteration picks a parent from the population (not the base trace) and mutates that.

Usage:
  python scripts/run_campaign.py
  python scripts/run_campaign.py --iterations 5000 --target mysql
  python scripts/run_campaign.py --seed tests/litmus/lost_update.json --target postgres
"""

from __future__ import annotations

import argparse
import copy
import datetime
import hashlib
import json
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from run_litmus import DEFAULT_DSN, get_adapter  # type: ignore[import]


def _event_counts_toward_step(ev: Dict[str, Any]) -> bool:
    """True if replay_litmus.py increments step for this event (invoke/begin/commit/rollback/sql)."""
    raw_type = str(ev.get("type", "")).lower()
    if raw_type in ("begin", "commit", "rollback", "sql"):
        return True
    if raw_type == "invoke":
        return True  # invoke is always executed (BEGIN/COMMIT/ROLLBACK/SQL)
    return False  # ok, fail, etc. are skipped


def _event_sql(ev: Dict[str, Any]) -> str:
    return str(ev.get("sql", "")).strip()


def _is_write_event(ev: Dict[str, Any]) -> bool:
    """True if this event is replayed and likely mutates DB state."""
    if not _event_counts_toward_step(ev):
        return False
    sql_upper = _event_sql(ev).upper()
    return sql_upper.startswith(("UPDATE", "INSERT", "DELETE", "UPSERT", "MERGE", "REPLACE"))


def _is_read_event(ev: Dict[str, Any]) -> bool:
    """True if this event is replayed and likely reads DB state."""
    if not _event_counts_toward_step(ev):
        return False
    return _event_sql(ev).upper().startswith("SELECT")


_WHERE_KEY_EQ_DOLLAR_RE = re.compile(r"WHERE\s+K\s*=\s*\$([0-9]+)", re.IGNORECASE)
_WHERE_KEY_EQ_BIND_RE = re.compile(r"WHERE\s+K\s*=\s*%s", re.IGNORECASE)
_WHERE_KEY_IN_RE = re.compile(r"WHERE\s+K\s+IN\s*\(([^)]*)\)", re.IGNORECASE)
_INSERT_KV_VALUES_RE = re.compile(
    r"INSERT\s+INTO\s+KV\s*\(([^)]*)\)\s*VALUES\s*\(([^)]*)\)",
    re.IGNORECASE,
)
_DOLLAR_PARAM_RE = re.compile(r"\$([0-9]+)")


def _normalize_key(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return value
        try:
            return int(stripped)
        except ValueError:
            return stripped
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


def _dedupe_keys(keys: List[Any]) -> List[Any]:
    out: List[Any] = []
    seen: set[str] = set()
    for key in keys:
        marker = repr(key)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(key)
    return out


def _extract_event_keys(ev: Dict[str, Any]) -> List[Any]:
    """
    Best-effort key extraction for key-centric SQL patterns used by kv workloads.
    Used by smart scheduling mutations to target conflict windows.
    """
    sql = _event_sql(ev)
    args_raw = ev.get("args")
    args = list(args_raw) if isinstance(args_raw, list) else []
    keys: List[Any] = []

    eq_match = _WHERE_KEY_EQ_DOLLAR_RE.search(sql)
    if eq_match:
        key_idx = int(eq_match.group(1)) - 1
        if 0 <= key_idx < len(args):
            keys.append(_normalize_key(args[key_idx]))
    elif _WHERE_KEY_EQ_BIND_RE.search(sql):
        if args:
            keys.append(_normalize_key(args[-1]))

    in_match = _WHERE_KEY_IN_RE.search(sql)
    if in_match:
        in_clause = in_match.group(1)
        for token in _split_sql_csv(in_clause):
            ph = _DOLLAR_PARAM_RE.fullmatch(token)
            if ph:
                key_idx = int(ph.group(1)) - 1
                if 0 <= key_idx < len(args):
                    keys.append(_normalize_key(args[key_idx]))
                continue
            if token == "%s":
                bind_idx = len(keys)
                if 0 <= bind_idx < len(args):
                    keys.append(_normalize_key(args[bind_idx]))
                continue
            keys.append(_normalize_key(_coerce_sql_literal(token)))

    insert_match = _INSERT_KV_VALUES_RE.search(sql)
    if insert_match:
        columns = [col.strip().strip("`\"").lower() for col in _split_sql_csv(insert_match.group(1))]
        values = _split_sql_csv(insert_match.group(2))
        if "k" in columns:
            col_idx = columns.index("k")
            if 0 <= col_idx < len(values):
                token = values[col_idx]
                ph = _DOLLAR_PARAM_RE.fullmatch(token)
                if ph:
                    key_idx = int(ph.group(1)) - 1
                    if 0 <= key_idx < len(args):
                        keys.append(_normalize_key(args[key_idx]))
                elif token == "%s":
                    if 0 <= col_idx < len(args):
                        keys.append(_normalize_key(args[col_idx]))
                else:
                    keys.append(_normalize_key(_coerce_sql_literal(token)))

    # Fallback for common single-key reads/writes.
    if not keys and len(args) == 1 and (_is_read_event(ev) or _is_write_event(ev)):
        keys.append(_normalize_key(args[0]))
    if not keys and args and "WHERE K" in sql.upper():
        keys.append(_normalize_key(args[-1]))

    return _dedupe_keys(keys)


def _synthesize_args_from_sql(sql: str, rng: random.Random) -> Optional[List[int]]:
    """
    Build placeholder args for SQL containing $1/$2/... when trace args are missing.
    This keeps replay executable and gives mutation room even on sparse traces.
    """
    param_ids = sorted({int(m.group(1)) for m in re.finditer(r"\$(\d+)", sql)})
    if not param_ids:
        return None
    arg_count = max(param_ids)
    args = [rng.randint(0, 1_000_000) for _ in range(arg_count)]
    upper_sql = sql.upper()
    for m in re.finditer(r"WHERE\s+K\s*=\s*\$(\d+)", upper_sql):
        key_arg = int(m.group(1)) - 1
        if 0 <= key_arg < len(args):
            args[key_arg] = rng.randint(1, 100)
    return args


def trace_index_to_replay_step(trace: List[Dict[str, Any]], list_index: int) -> int:
    """Convert 0-based list index to 1-based replay step (only events that replay executes)."""
    step = 0
    for i, ev in enumerate(trace):
        if _event_counts_toward_step(ev):
            step += 1
        if i == list_index:
            return step
    return step


def _clear_delay_marks(trace: List[Dict[str, Any]]) -> None:
    for ev in trace:
        ev.pop("delay", None)


def mutate_timing_smart(trace: List[Dict[str, Any]], rng: random.Random) -> bool:
    """
    Mutation A (smart):
      - Prefer injecting delay before writes.
      - Strongly prefer writes that follow a read on the same key
        (especially within the same client), i.e. RMW/conflict windows.
    Returns True if a delay target was selected.
    """
    replayable_indices = [i for i, ev in enumerate(trace) if _event_counts_toward_step(ev)]
    if not replayable_indices:
        return False

    rmw_same_client: List[int] = []
    read_then_write_same_key: List[int] = []
    write_candidates: List[int] = []
    last_read_any: Dict[str, int] = {}
    last_read_by_client: Dict[Tuple[str, str], int] = {}

    for i, ev in enumerate(trace):
        if not _event_counts_toward_step(ev):
            continue
        keys = _extract_event_keys(ev)
        client = str(ev.get("client_id", ""))
        if _is_read_event(ev):
            for key in keys:
                marker = repr(key)
                last_read_any[marker] = i
                last_read_by_client[(client, marker)] = i
            continue
        if not _is_write_event(ev):
            continue
        write_candidates.append(i)
        saw_same_client = False
        saw_any_reader = False
        for key in keys:
            marker = repr(key)
            if marker in last_read_any:
                saw_any_reader = True
            if (client, marker) in last_read_by_client:
                saw_same_client = True
        if saw_same_client:
            rmw_same_client.append(i)
        elif saw_any_reader:
            read_then_write_same_key.append(i)

    # WEIGHTS
    weighted_indices: List[int] = []
    for idx in rmw_same_client:
        weighted_indices.extend([idx] * 8)
    for idx in read_then_write_same_key:
        weighted_indices.extend([idx] * 5)
    for idx in write_candidates:
        weighted_indices.extend([idx] * 3)
    weighted_indices.extend(replayable_indices)
    if not weighted_indices:
        return False

    target_idx = rng.choice(weighted_indices)
    rmw_set = set(rmw_same_client)
    overlap_set = set(read_then_write_same_key)
    if target_idx in rmw_set:
        delay = rng.choice([0.05, 0.1, 0.15, 0.2])
    elif target_idx in overlap_set:
        delay = rng.choice([0.03, 0.05, 0.1])
    elif target_idx in set(write_candidates):
        delay = rng.choice([0.02, 0.05, 0.1])
    else:
        delay = rng.choice([0.01, 0.02, 0.05])
    trace[target_idx]["delay"] = delay
    return True


def mutate_reorder(trace: List[Dict[str, Any]], rng: random.Random) -> bool:
    """Mutation B: swap adjacent cross-client events; this preserves each client's local order."""
    if len(trace) < 2:
        return False
    candidates = [
        i for i in range(len(trace) - 1)
        if trace[i].get("client_id") != trace[i + 1].get("client_id")
    ]
    if not candidates:
        return False
    i = rng.choice(candidates)
    trace[i], trace[i + 1] = trace[i + 1], trace[i]
    return True


def _can_move_left_preserving_client_order(
    trace: List[Dict[str, Any]],
    from_idx: int,
    to_idx: int,
) -> bool:
    """
    Check whether moving trace[from_idx] to to_idx keeps per-client local order unchanged.

    This is safe iff the moved event does not jump ahead of any earlier event
    from the same client.
    """
    if from_idx <= to_idx:
        return False
    if to_idx < 0 or from_idx >= len(trace):
        return False
    moved_client = trace[from_idx].get("client_id")
    for k in range(to_idx, from_idx):
        if trace[k].get("client_id") == moved_client:
            return False
    return True


def _move_left_preserving_client_order(
    trace: List[Dict[str, Any]],
    from_idx: int,
    to_idx: int,
) -> bool:
    """Move one event earlier only when client-local ordering is preserved."""
    if not _can_move_left_preserving_client_order(trace, from_idx, to_idx):
        return False
    ev = trace.pop(from_idx)
    trace.insert(to_idx, ev)
    return True


def mutate_reorder_smart(trace: List[Dict[str, Any]], rng: random.Random, window: int = 8) -> bool:
    """
    Conflict-aware reorder:
      - Prefer moving events that touch the same key across different clients.
      - Prefer write/write and read/write key conflicts over generic swaps.
      - Preserve local ordering within each client.
    Returns True if order changed.
    """
    if len(trace) < 2:
        return False
    key_cache = [_extract_event_keys(ev) if _event_counts_toward_step(ev) else [] for ev in trace]
    candidates: List[Tuple[int, int, int]] = []
    for i, ev_i in enumerate(trace):
        if not _event_counts_toward_step(ev_i):
            continue
        keys_i = {repr(k) for k in key_cache[i]}
        if not keys_i:
            continue
        ci = ev_i.get("client_id")
        i_is_write = _is_write_event(ev_i)
        i_is_read = _is_read_event(ev_i)
        upper = min(len(trace), i + window + 1)
        for j in range(i + 1, upper):
            ev_j = trace[j]
            if not _event_counts_toward_step(ev_j):
                continue
            if ci == ev_j.get("client_id"):
                continue
            if not _can_move_left_preserving_client_order(trace, from_idx=j, to_idx=i):
                continue
            keys_j = {repr(k) for k in key_cache[j]}
            if not keys_j or not (keys_i & keys_j):
                continue
            j_is_write = _is_write_event(ev_j)
            j_is_read = _is_read_event(ev_j)
            score = 1
            if i_is_write and j_is_write:
                score = 8
            elif (i_is_read and j_is_write) or (i_is_write and j_is_read):
                score = 6
            elif i_is_read and j_is_read:
                score = 2
            # Closer swaps perturb less while still changing ordering.
            score += max(0, window - (j - i))
            candidates.append((score, i, j))

    if candidates:
        weighted_pairs: List[Tuple[int, int]] = []
        for score, i, j in candidates:
            weighted_pairs.extend([(i, j)] * max(1, score))
        i, j = rng.choice(weighted_pairs)
        # Move j event before i only when client-local ordering remains intact.
        if _move_left_preserving_client_order(trace, from_idx=j, to_idx=i):
            return True
    return mutate_reorder(trace, rng)


def mutate_data(trace: List[Dict[str, Any]], rng: random.Random) -> bool:
    """
    Mutation C: change SQL args, preferring write statements that can alter final state.
    Returns True if changed.
    """
    replayable_sql = [i for i, ev in enumerate(trace) if _event_counts_toward_step(ev) and _event_sql(ev)]
    if not replayable_sql:
        return False
    write_indices = [i for i in replayable_sql if _is_write_event(trace[i])]
    i = rng.choice(write_indices or replayable_sql)
    args = trace[i].get("args")
    if not isinstance(args, list) or not args:
        synthesized = _synthesize_args_from_sql(_event_sql(trace[i]), rng)
        if not synthesized:
            return False
        trace[i]["args"] = synthesized
        args = synthesized
    if not args:
        return False
    sql_upper = _event_sql(trace[i]).upper()
    j = rng.randint(0, len(args) - 1)
    val = args[j]
    trace[i]["args"] = copy.deepcopy(args)
    if isinstance(val, str) and val in ("1", "2", "3", "4"):
        trace[i]["args"][j] = rng.choice([x for x in ("1", "2", "3", "4") if x != val])
        return True
    if isinstance(val, int):
        is_key_arg = (sql_upper.startswith("SELECT") and len(args) == 1) or ("WHERE K=" in sql_upper and j == len(args) - 1)
        if is_key_arg:
            new_val = rng.randint(1, 100)
        elif _is_write_event(trace[i]):
            # Keep write values in the same practical range as workload generators.
            new_val = rng.randint(0, 1_000_000)
        else:
            new_val = rng.randint(0, 1_000_000)
        if new_val == val:
            new_val = 1 if val == 0 else val - 1
        trace[i]["args"][j] = new_val
        return True
    if isinstance(val, float):
        trace[i]["args"][j] = float(rng.randint(0, 1_000_000))
        return True
    return False


def run_random_trace(
    config: str = "workloads/example.yaml",
    trace_out: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Run run_random.py and load the produced JSON trace into memory."""
    trace_path = trace_out or (ROOT / "logs" / "trace_latest.json")
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_random.py"),
        "--config", config,
        "--trace-out", str(trace_path),
    ]
    subprocess.run(cmd, cwd=str(ROOT), check=True, capture_output=True)
    with trace_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_seed_trace_file(seed_path: Path) -> List[Dict[str, Any]]:
    """
    Load a specific JSON trace file for seed-based fuzzing.
    Accepts either:
      - a raw trace list [{...}, ...]
      - a corpus state record {"mutated_events": [...], ...}
    """
    candidate = seed_path.expanduser()
    if not candidate.is_absolute():
        cwd_candidate = (Path.cwd() / candidate).resolve()
        root_candidate = (ROOT / candidate).resolve()
        if cwd_candidate.exists():
            candidate = cwd_candidate
        elif root_candidate.exists():
            candidate = root_candidate
        else:
            candidate = cwd_candidate
    if not candidate.exists():
        stem = Path(seed_path).stem
        suggestions: List[str] = []
        for pattern in (
            f"*{stem}*.json",
            f"*{stem}*.trace.json",
            f"test*{stem}*.py",
        ):
            for p in ROOT.rglob(pattern):
                if len(suggestions) >= 5:
                    break
                suggestions.append(str(p.resolve()))
            if len(suggestions) >= 5:
                break
        hint = ""
        if suggestions:
            hint = "\nDid you mean one of:\n  - " + "\n  - ".join(suggestions)
        raise FileNotFoundError(f"seed trace file not found: {candidate}{hint}")

    with candidate.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("mutated_events"), list):
        return payload["mutated_events"]
    raise ValueError(
        f"seed trace must be a JSON list or a record with 'mutated_events': {candidate}"
    )


def workload_name_from_config(config: str) -> str:
    """Extract workload name from config path (e.g., workloads/bank.yaml -> bank)."""
    name = Path(config).stem.strip()
    return name or "unknown_workload"


def _coerce_output_text(value: Any) -> str:
    """Normalize subprocess output chunks to text for robust parsing/logging."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _last_step_from_output(stdout: Any, stderr: Any) -> str:
    """Parse stdout/stderr for all step=N (or step N); return max step as string, or '?' if none."""
    combined = _coerce_output_text(stdout) + "\n" + _coerce_output_text(stderr)
    steps = [int(m.group(1)) for m in re.finditer(r"step[=\s]+(\d+)", combined, re.IGNORECASE)]
    return str(max(steps)) if steps else "?"


def replay_trace_subprocess(
    trace_path: Path,
    target: str,
    dsn: str,
    isolation: str,
    inject_step: Optional[int] = None,
    delay_seconds: float = 0.0,
) -> Tuple[int, str, str]:
    """Run replay_litmus.py as subprocess. Returns (returncode, stdout, stderr)."""
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "replay_litmus.py"),
        "--trace", str(trace_path),
        "--target", target,
        "--dsn", dsn,
        "--isolation", isolation,
    ]
    if inject_step is not None:
        cmd.extend(["--inject-delay-at-step", str(inject_step), "--delay-seconds", str(delay_seconds)])
    try:
        result = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=False,
            timeout=10,
        )
        return (
            result.returncode,
            _coerce_output_text(result.stdout),
            _coerce_output_text(result.stderr),
        )
    except subprocess.TimeoutExpired as e:
        # Rich fingerprinting: last successful step from partial output
        out = _coerce_output_text(getattr(e, "stdout", None))
        err = _coerce_output_text(getattr(e, "stderr", None))
        last_step = _last_step_from_output(out, err)
        sig = f"TIMEOUT_AT_STEP_{last_step}"
        return 124, sig, ""


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


def parse_state_from_replay_stdout(stdout: str) -> Optional[Dict[str, Any]]:
    """
    replay_litmus prints a JSON payload on success:
      {"execution_history": [...], "final_state": [...]}
    Parse the last valid JSON line and return the normalized payload.
    Returns None if not found/parsable.
    """
    for line in reversed((stdout or "").splitlines()):
        candidate = line.strip()
        if not candidate or not (candidate.startswith("{") or candidate.startswith("[")):
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            if "execution_history" in parsed and "final_state" in parsed:
                return {
                    "execution_history": _jsonable(parsed.get("execution_history", [])),
                    "final_state": _jsonable(parsed.get("final_state", [])),
                }
            continue
        if isinstance(parsed, list):
            # Backward compatibility: older replay emitted only final state list.
            return {"execution_history": [], "final_state": _jsonable(parsed)}
    return None


def get_delay_from_trace(trace: List[Dict[str, Any]]) -> Tuple[Optional[int], float]:
    """If any event has a 'delay' field, return (1-based step, delay_seconds). Else (None, 0)."""
    for i, ev in enumerate(trace):
        if "delay" in ev:
            step = trace_index_to_replay_step(trace, i)
            return step, float(ev["delay"])
    return None, 0.0


def fetch_kv_state(target: str, dsn: str, isolation: str) -> List[Tuple[Any, Any]]:
    """Connect and run SELECT k, v FROM kv ORDER BY k; return rows."""
    adapter = get_adapter(target, dsn, isolation, logger=None)
    adapter.connect()
    try:
        cur = getattr(adapter, "cur1", None)
        conn = getattr(adapter, "conn1", None)
        if cur is None or conn is None:
            return []
        cur.execute("SELECT k, v FROM kv ORDER BY k")
        return cur.fetchall()
    finally:
        adapter.teardown()


def _final_state_row_to_pair(row: Any) -> Optional[List[Any]]:
    if isinstance(row, dict):
        if "k" in row and "v" in row:
            return [_jsonable(row.get("k")), _jsonable(row.get("v"))]
        return None
    if isinstance(row, (list, tuple)) and len(row) >= 2:
        return [_jsonable(row[0]), _jsonable(row[1])]
    return None


def _state_sort_key(value: Any) -> Tuple[str, str]:
    canonical = _jsonable(value)
    return (type(canonical).__name__, json.dumps(canonical, sort_keys=True, separators=(",", ":")))


def _canonical_final_state(final_state: Any) -> List[List[Any]]:
    rows = final_state if isinstance(final_state, list) else []
    pairs: List[List[Any]] = []
    for row in rows:
        pair = _final_state_row_to_pair(row)
        if pair is not None:
            pairs.append(pair)
    pairs.sort(key=lambda row: (_state_sort_key(row[0]), _state_sort_key(row[1])))
    return pairs


def output_hash(output: Any) -> str:
    """
    Stable hash for the coverage oracle:
      - Success payloads hash only the final kv table state.
      - Failures hash the error signature text.
    """
    if isinstance(output, dict):
        fingerprint = {"final_state": _canonical_final_state(output.get("final_state", []))}
    else:
        fingerprint = {"error_signature": str(output)}
    payload = json.dumps(fingerprint, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def parse_error_signature(returncode: int, stdout: str, stderr: str) -> str:
    """Build a short error signature from replay output (e.g. 'TIMEOUT_AT_STEP_7')."""
    combined = (stdout or "") + "\n" + (stderr or "")
    last_step = _last_step_from_output(stdout or "", stderr or "")
    if "deadlock" in combined.lower() or "timeout" in combined.lower() or "canceled" in combined.lower():
        return f"TIMEOUT_AT_STEP_{last_step}"
    if returncode != 0:
        return f"ERROR_AT_STEP_{last_step}"
    return f"EXIT_{returncode}_AT_STEP_{last_step}"


class CampaignRunner:
    """
    Coverage-guided fuzzing campaign: mutate traces, replay, retain only new states.
    """

    def __init__(
        self,
        target: str = "postgres",
        dsn: Optional[str] = None,
        isolation: str = "READ COMMITTED",
        corpus_dir: Optional[Path] = None,
        rng_seed: Optional[int] = None,
        baseline_no_mutate: bool = False,
        baseline_fresh_trace: bool = False,
    ) -> None:
        self.target = target
        self.dsn = dsn or DEFAULT_DSN.get(target)
        if not self.dsn:
            raise ValueError(f"No DSN for target {target!r}. Pass --dsn.")
        self.isolation = isolation.strip().upper().replace("_", " ")
        if self.isolation == "SERIALIZATION":
            self.isolation = "SERIALIZABLE"
        self.baseline_no_mutate = baseline_no_mutate
        self.baseline_fresh_trace = baseline_fresh_trace
        default_root = ROOT / ("baseline_corpus" if self.baseline_no_mutate else "corpus")
        self.corpus_root = Path(corpus_dir or default_root)
        self.corpus_root.mkdir(parents=True, exist_ok=True)
        # Resolved in run() to corpus/<workload>/<target>/.
        self.corpus_dir = self.corpus_root
        self.rng = random.Random(rng_seed)
        self.seen_hashes: set[str] = set()
        self.unique_states = 0
        # Population queue: traces that discovered new states; used as parents for evolution.
        self.population: List[List[Dict[str, Any]]] = []
        self.max_population = 10_000  # Cap to avoid unbounded memory
        self.seed_trace_events = 200  # Replayable events used as mutation base per campaign seed.
        # Keep exploring newly generated seeds instead of only exploiting first-discovered parents.
        self.explore_parent_rate = 0.45
        self.temp_trace_path = ROOT / "temp_trace.json"
        # Index file capturing metadata for each discovered state so we can rerun/inspect later.
        self.index_path = self.corpus_dir / "states_index.jsonl"
        # Set in run() to a run-specific subdir (<corpus_dir>/run_<timestamp>) so each run has its own folder.
        self._run_dir: Optional[Path] = None

    def _generate_seed_trace(self, config: str) -> List[Dict[str, Any]]:
        """Step 1: Run run_random.py to produce baseline trace."""
        return run_random_trace(config=config, trace_out=(ROOT / "logs" / "trace_latest.json"))

    def _prepare_seed_trace(self, full_trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Keep only replayable events and pick a write-rich window.
        Using the first N events biases toward read-heavy startup traces.
        """
        replayable = [ev for ev in full_trace if _event_counts_toward_step(ev)]
        if not replayable:
            return []
        target_len = min(self.seed_trace_events, len(replayable))
        if len(replayable) <= target_len:
            return copy.deepcopy(replayable)
        max_start = len(replayable) - target_len
        best_start = self.rng.randint(0, max_start)
        best_score = -1
        attempts = min(64, max_start + 1)
        for _ in range(attempts):
            start = self.rng.randint(0, max_start)
            window = replayable[start:start + target_len]
            write_count = sum(1 for ev in window if _is_write_event(ev))
            client_count = len({ev.get("client_id") for ev in window if "client_id" in ev})
            score = (write_count * 10) + client_count
            if score > best_score:
                best_score = score
                best_start = start
        return copy.deepcopy(replayable[best_start:best_start + target_len])

    def _build_base_trace(self, config: str, max_attempts: int = 5) -> List[Dict[str, Any]]:
        """
        Generate several seeds and keep the write-rich replayable slice.
        """
        best_trace: List[Dict[str, Any]] = []
        best_writes = -1
        for attempt in range(1, max_attempts + 1):
            full_trace = self._generate_seed_trace(config)
            candidate = self._prepare_seed_trace(full_trace)
            write_count = sum(1 for ev in candidate if _is_write_event(ev))
            if write_count > best_writes:
                best_writes = write_count
                best_trace = candidate
            if candidate and write_count > 0:
                if attempt > 1:
                    print(f"  [campaign] selected seed attempt {attempt} with {write_count} write events.")
                return candidate
            print(f"  [campaign] seed attempt {attempt} was weak ({len(candidate)} events, {write_count} writes); retrying...")
        return best_trace

    def _prepare_baseline_trace(self, full_trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Baseline trace preparation:
          - keep all replayable events (no size cap)
          - strip any inherited delay markers so baseline remains mutation-free
        """
        replayable = [ev for ev in full_trace if _event_counts_toward_step(ev)]
        cleaned: List[Dict[str, Any]] = []
        for ev in replayable:
            cloned = copy.deepcopy(ev)
            cloned.pop("delay", None)
            cleaned.append(cloned)
        return cleaned

    def _mutate_trace(self, base_trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Step 2: Clone and apply smart scheduling mutations (conflict-aware delay + optional reorder)."""
        trace = copy.deepcopy(base_trace)
        _clear_delay_marks(trace)
        changed = False

        # Control experiment lock-down:
        # Intentionally disable all data/args/sql mutations to isolate race conditions.
        # Only scheduling mutations are allowed below.
        # changed = mutate_data(trace, self.rng) or changed

        # Conflict-aware reorder is primary because it perturbs cross-client
        # event ordering around shared-key conflict windows.
        if self.rng.random() < 0.95:
            changed = mutate_reorder_smart(trace, self.rng) or changed
        # Delay injection remains useful for engines/traces that retain transactional overlap.
        if self.rng.random() < 0.75:
            changed = mutate_timing_smart(trace, self.rng) or changed

        # Avoid no-op iterations: always force at least one scheduling mutation.
        if not changed:
            changed = mutate_reorder_smart(trace, self.rng)
            if not changed:
                mutate_timing_smart(trace, self.rng)
        return trace

    def _mutate(self, base_trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Backward-compatible alias for mutation entrypoint."""
        return self._mutate_trace(base_trace)

    def _select_parent(self, base_trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parent selection for evolution:
          - Explore fresh base traces regularly (important after stall reseeds).
          - Exploit discovered corpus parents the rest of the time.
        """
        if not self.population:
            return base_trace
        if self.rng.random() < self.explore_parent_rate:
            return base_trace
        return self.rng.choice(self.population)

    def _execute(self, trace: List[Dict[str, Any]]) -> Tuple[bool, Any, str, Optional[str], float]:
        """
        Step 3: Write trace to temp file, run replay, return (success, output, outcome_type).
        output: replay JSON payload on success, else error signature string.
        outcome_type: "Success" | "Deadlock" | "Error".
        """
        with self.temp_trace_path.open("w", encoding="utf-8") as f:
            json.dump(trace, f, indent=2)
        inject_step, delay_seconds = get_delay_from_trace(trace)
        start = time.monotonic()
        returncode, stdout, stderr = replay_trace_subprocess(
            self.temp_trace_path,
            target=self.target,
            dsn=self.dsn,
            isolation=self.isolation,
            inject_step=inject_step,
            delay_seconds=delay_seconds,
        )
        duration = time.monotonic() - start
        if returncode == 0:
            payload = parse_state_from_replay_stdout(stdout)
            if payload is None:
                fallback_state = fetch_kv_state(self.target, self.dsn, self.isolation)
                payload = {
                    "execution_history": [],
                    "final_state": [list(row) for row in fallback_state],
                }
            return True, payload, "Success", None, duration
        # Use rich signature from subprocess if already TIMEOUT_AT_STEP_N; else parse
        raw = (stdout or "").strip()
        if raw.startswith("TIMEOUT_AT_STEP_") or raw.startswith("ERROR_AT_STEP_"):
            sig = raw
        else:
            sig = parse_error_signature(returncode, stdout, stderr)
        is_deadlock = "TIMEOUT" in sig or "deadlock" in (stdout + stderr).lower()
        outcome_type = "Deadlock" if is_deadlock else "Error"
        return False, sig, outcome_type, sig, duration

    def _coverage_oracle(
        self,
        output: Any,
        outcome_type: str,
        trace: List[Dict[str, Any]],
        iteration: int,
        config: str,
        error_sig: Optional[str],
        duration: float,
    ) -> bool:
        """
        Step 4: Hash output; if new, save to corpus and increment unique_states.
        Also append a JSONL record with metadata so states/workloads can be inspected or replayed.
        Returns True if this was a new state.
        """
        h = output_hash(output)
        if h in self.seen_hashes:
            return False
        self.seen_hashes.add(h)
        self.unique_states += 1
        # Retention: add this trace to the population queue for evolutionary parent selection.
        if not self.baseline_no_mutate:
            self.population.append(copy.deepcopy(trace))
            if len(self.population) > self.max_population:
                self.population.pop(self.rng.randint(0, len(self.population) - 1))
        output_dir = self._run_dir if self._run_dir is not None else self.corpus_dir
        corpus_path = output_dir / f"state_{h}.json"
        # Normalize outcome type to a stable, uppercased label.
        norm_outcome = (outcome_type or "").upper()
        # Build full record including the mutated events so we can fully replay/inspect later.
        record = {
            "hash": h,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "iteration": iteration,
            "target": self.target,
            "workload_config": config,
            # The actual recipe for the bug / interesting state.
            "mutated_events": trace,
            "outcome_type": norm_outcome,
            "error_signature": error_sig,
            "output": _jsonable(output),
            "execution_time": duration,
            "dsn": self.dsn,
            "isolation": self.isolation,
        }
        # Save the full record in the per-state JSON file.
        with corpus_path.open("w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)
        # Append a smaller JSONL record (without full mutated_events) as an index.
        index_record = {k: v for k, v in record.items() if k != "mutated_events"}
        index_record["trace_file"] = str(corpus_path)
        index_path = output_dir / "states_index.jsonl"
        try:
            with index_path.open("a", encoding="utf-8") as idx_f:
                idx_f.write(json.dumps(index_record) + "\n")
        except Exception:
            # Indexing is best-effort; do not fail the campaign if logging the record breaks.
            pass
        # Newline so the status line (printed with \r) is not overwritten
        print(f"\n  🚀 Found New State #{self.unique_states}! (Type: {outcome_type}) -> {corpus_path.name}")
        return True

    def run(
        self,
        iterations: int = 1000,
        config: str = "workloads/example.yaml",
        seed_trace_path: Optional[Path] = None,
    ) -> None:
        """Run the campaign loop: seed → mutate → execute → oracle."""
        workload_name = workload_name_from_config(config)
        self.corpus_dir = self.corpus_root / workload_name / self.target
        self.corpus_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.corpus_dir / "states_index.jsonl"
        # Each run gets its own subfolder under workload/target corpus dir.
        run_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        self._run_dir = self.corpus_dir / f"run_{run_timestamp}"
        self._run_dir.mkdir(parents=True, exist_ok=True)
        mode = "baseline" if self.baseline_no_mutate else "smart-mutate"
        print(
            f"Campaign: mode={mode} target={self.target} iterations={iterations} "
            f"corpus={self.corpus_dir} run_dir={self._run_dir.name}"
        )
        fixed_seed_mode = seed_trace_path is not None
        baseline_fresh_mode = self.baseline_no_mutate and self.baseline_fresh_trace and not fixed_seed_mode
        if fixed_seed_mode:
            print(f"Step 1: Loading fixed seed trace from {seed_trace_path}...")
            base_trace = load_seed_trace_file(seed_trace_path)
            if not base_trace:
                raise RuntimeError("Seed trace is empty; cannot start campaign.")
            base_trace = [copy.deepcopy(ev) for ev in base_trace if _event_counts_toward_step(ev)]
            if not base_trace:
                raise RuntimeError("Seed trace had no replayable events; cannot start campaign.")
            if self.baseline_no_mutate:
                base_trace = self._prepare_baseline_trace(base_trace)
        elif self.baseline_no_mutate:
            if baseline_fresh_mode:
                print("Step 1: Baseline mode enabled; generating a fresh random trace (no mutation)...")
            else:
                print("Step 1: Baseline mode enabled; generating one fixed random base trace (no mutation)...")
            base_trace = self._prepare_baseline_trace(self._generate_seed_trace(config))
            if not base_trace:
                raise RuntimeError("No replayable events found in generated baseline trace; cannot start campaign.")
        else:
            print("Step 1: Generating baseline trace with run_random.py...")
            base_trace = self._build_base_trace(config)
            if not base_trace:
                raise RuntimeError("No replayable events found in generated trace; cannot start campaign.")
        write_count = sum(1 for ev in base_trace if _is_write_event(ev))
        print(f"  Loaded {len(base_trace)} replayable events as base_trace ({write_count} write events).")
        if fixed_seed_mode:
            print("  Fixed seed mode: skipping corpus population preload (mutating only the provided seed lineage).")
            if self.baseline_no_mutate and self.baseline_fresh_trace:
                print("  Note: --baseline-fresh-trace is ignored when --seed is provided.")
        elif self.baseline_no_mutate:
            print("  Baseline mode: loading only seen hashes (no population/mutation).")
            # Load existing hashes from prior runs under this corpus root.
            for run_dir in sorted(self.corpus_dir.glob("run_*")):
                if run_dir.is_dir():
                    for p in run_dir.glob("state_*.json"):
                        try:
                            with p.open("r", encoding="utf-8") as f:
                                rec = json.load(f)
                            if isinstance(rec, dict) and "hash" in rec:
                                self.seen_hashes.add(str(rec["hash"]))
                        except (KeyError, json.JSONDecodeError):
                            pass
            for p in self.corpus_dir.glob("state_*.json"):
                try:
                    with p.open("r", encoding="utf-8") as f:
                        rec = json.load(f)
                    if isinstance(rec, dict) and "hash" in rec:
                        self.seen_hashes.add(str(rec["hash"]))
                except (KeyError, json.JSONDecodeError):
                    pass
            if self.seen_hashes:
                self.unique_states = len(self.seen_hashes)
                print(f"  Loaded {self.unique_states} known states from existing baseline corpus.")
        else:
            # Optionally seed population and seen_hashes from existing corpus (all run_* dirs + legacy top-level).
            for run_dir in sorted(self.corpus_dir.glob("run_*")):
                if run_dir.is_dir():
                    for p in run_dir.glob("state_*.json"):
                        try:
                            with p.open("r", encoding="utf-8") as f:
                                rec = json.load(f)
                            if isinstance(rec, dict) and "mutated_events" in rec:
                                self.population.append(rec["mutated_events"])
                                if "hash" in rec:
                                    self.seen_hashes.add(rec["hash"])
                            elif isinstance(rec, list):
                                self.population.append(rec)
                        except (KeyError, json.JSONDecodeError):
                            pass
            for p in self.corpus_dir.glob("state_*.json"):
                try:
                    with p.open("r", encoding="utf-8") as f:
                        rec = json.load(f)
                    if isinstance(rec, dict) and "mutated_events" in rec:
                        self.population.append(rec["mutated_events"])
                        if "hash" in rec:
                            self.seen_hashes.add(rec["hash"])
                    elif isinstance(rec, list):
                        # Legacy format: file is just the trace list.
                        self.population.append(rec)
                except (KeyError, json.JSONDecodeError):
                    pass
            if self.population:
                self.unique_states = len(self.seen_hashes)
                print(f"  Loaded {len(self.population)} traces from corpus into population.")
        if self.baseline_no_mutate:
            if baseline_fresh_mode:
                print("Step 2–4: Fresh Trace Generation (no mutation) → Execution → Coverage Oracle (loop)")
            else:
                print("Step 2–4: Fixed Trace Replay (no mutation) → Execution → Coverage Oracle (loop)")
        else:
            print("Step 2–4: Mutation → Execution → Coverage Oracle (loop)")
        print("-" * 60)

        last_discovery_iteration = 0
        it = 0
        while True:
            it += 1
            if iterations > 0 and it > iterations:
                break
            if self.baseline_no_mutate:
                if baseline_fresh_mode:
                    current_trace = self._prepare_baseline_trace(self._generate_seed_trace(config))
                    if not current_trace:
                        print("\n  [baseline] generated trace had no replayable events; skipping iteration.")
                        continue
                else:
                    current_trace = copy.deepcopy(base_trace)
                _, output, outcome_type, error_sig, duration = self._execute(current_trace)
                if self._coverage_oracle(output, outcome_type, current_trace, it, config, error_sig, duration):
                    last_discovery_iteration = it
                print(f"\rIteration: {it} | Unique States Found: {self.unique_states}", end="", flush=True)
                continue
            # Keep a single startup base trace for apples-to-apples comparisons.
            if (it - last_discovery_iteration) > 50:
                print("\n  🔄 Stalled for 50 iterations. Keeping startup base_trace fixed; continuing mutation search.")
                last_discovery_iteration = it
            # Evolution: blend exploration from fresh base traces with corpus exploitation.
            parent = self._select_parent(base_trace)
            mutated = self._mutate_trace(parent)
            success, output, outcome_type, error_sig, duration = self._execute(mutated)
            if self._coverage_oracle(output, outcome_type, mutated, it, config, error_sig, duration):
                last_discovery_iteration = it
            # Single status line, updated in place
            print(f"\rIteration: {it} | Unique States Found: {self.unique_states}", end="", flush=True)

        print()  # newline after last status
        print("-" * 60)
        print(f"Done. Total unique states: {self.unique_states} (saved in {self._run_dir})")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Coverage-guided fuzzing campaign: run_random → replay_litmus → corpus.",
    )
    parser.add_argument(
        "--target",
        default="postgres",
        choices=["postgres", "mysql"],
        help="Database backend for replay.",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Connection DSN (defaults from run_litmus).",
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help="Isolation level for replay.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1000,
        metavar="N",
        help="Number of mutation/replay iterations (0 or --forever = run until Ctrl+C).",
    )
    parser.add_argument(
        "--forever",
        action="store_true",
        help="Run continuously until interrupted.",
    )
    parser.add_argument(
        "--config",
        default="workloads/fuzz.yaml",
        help="Workload config for run_random.py seed generation.",
    )
    parser.add_argument(
        "--corpus",
        default=None,
        type=Path,
        help=(
            "Corpus root directory (default: corpus/; baseline mode default: baseline_corpus/; "
            "writes to <root>/<workload>/<target>/)."
        ),
    )
    parser.add_argument(
        "--seed",
        type=Path,
        default=None,
        help="Path to a specific JSON trace file to fuzz.",
    )
    parser.add_argument(
        "--rng-seed",
        type=int,
        default=None,
        help="Random seed for reproducibility of mutation choices.",
    )
    parser.add_argument(
        "--explore-parent-rate",
        type=float,
        default=0.45,
        help=(
            "In smart mode, probability of mutating the current freshly generated base trace "
            "instead of a discovered corpus parent (range 0.0-1.0)."
        ),
    )
    parser.add_argument(
        "--baseline",
        "--baseline-no-mutate",
        dest="baseline_no_mutate",
        action="store_true",
        help=(
            "Disable mutation and replay one fixed base trace repeatedly (natural scheduler only). "
            "Default output root switches to baseline_corpus/ unless --corpus is set."
        ),
    )
    parser.add_argument(
        "--baseline-fresh-trace",
        action="store_true",
        help=(
            "Baseline-only opt-in: regenerate a fresh workload trace every iteration "
            "(measures workload drift, not fixed-seed scheduler nondeterminism)."
        ),
    )
    parser.add_argument(
        "--litmus",
        default=None,
        choices=["update-conflict-out"],
        help="Run a named deterministic litmus reproducer instead of fuzzing.",
    )
    parser.add_argument(
        "--pg-version", "--pg",
        default=None,
        choices=["12.3", "12.4"],
        help="PostgreSQL version for litmus reproducers (sets DSN automatically).",
    )
    parser.add_argument(
        "--mode",
        default=None,
        choices=["scheduled", "fuzz", "discover", "baseline"],
        help="Execution mode: 'scheduled' for deterministic, 'discover' for heuristic campaign, 'baseline' for no-delay stats.",
    )
    parser.add_argument(
        "--schedule-variant",
        default="update",
        choices=["update", "delete", "both"],
        help="Which permutation variant to run (default: update).",
    )
    parser.add_argument(
        "--k-bounded",
        type=int,
        default=None,
        metavar="K",
        help="K-bounded schedule enumeration depth for litmus reproducers.",
    )
    parser.add_argument(
        "--no-delays",
        action="store_true",
        help="Force baseline mode (no delays).",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=200,
        help="Number of trials for baseline mode (default: 200).",
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=500,
        help="Mutation budget for discovery mode (default: 500).",
    )

    args = parser.parse_args()

    # Dispatch to litmus reproducer if --litmus is set
    if args.litmus == "update-conflict-out":
        # Discovery / baseline mode via the new engine
        mode = args.mode or ("baseline" if args.no_delays else "discover")
        if mode in ("discover", "baseline", "full"):
            from discover_update_conflict_out import (
                run_baseline,
                run_discovery_campaign,
                run_full_demo,
                dsn_for_pg,
                wait_for_pg,
            )
            if args.pg_version and not args.dsn:
                dsn = dsn_for_pg(args.pg_version)
            elif args.dsn:
                dsn = args.dsn
            else:
                dsn = dsn_for_pg("12.3")

            print(f"Waiting for Postgres readiness...", end=" ", flush=True)
            if not wait_for_pg(dsn, timeout=30.0):
                print("TIMEOUT")
                return 2
            print("ready.")

            if mode == "baseline":
                run_baseline(
                    dsn,
                    variant=args.schedule_variant if args.schedule_variant != "both" else "update",
                    trials=args.trials,
                )
                return 0
            elif mode == "full":
                report = run_full_demo(
                    variant=args.schedule_variant if args.schedule_variant != "both" else "update",
                    k=args.k_bounded or 3,
                    budget=args.budget,
                )
                return 0 if report.get("discovery", {}).get("found") else 1
            else:  # discover
                artifact = run_discovery_campaign(
                    dsn,
                    variant=args.schedule_variant if args.schedule_variant != "both" else "update",
                    k=args.k_bounded or 3,
                    budget=args.budget,
                )
                return 0 if artifact else 1

        # Legacy scheduled/k-bounded mode
        from repro_update_conflict_out import (
            run_canonical,
            run_comparison,
            run_k_bounded_search,
            dsn_for_pg_version,
            wait_for_postgres,
        )

        if args.pg_version and not args.dsn:
            dsn = dsn_for_pg_version(args.pg_version)
        elif args.dsn:
            dsn = args.dsn
        else:
            dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

        print(f"Waiting for Postgres readiness...", end=" ", flush=True)
        if not wait_for_postgres(dsn, timeout=30.0):
            print("TIMEOUT")
            return 2
        print("ready.")

        variants = ["update", "delete"] if args.schedule_variant == "both" else [args.schedule_variant]

        if args.k_bounded is not None:
            for v in variants:
                run_k_bounded_search(dsn, v, max_k=args.k_bounded)
        else:
            for v in variants:
                run_canonical(dsn, v)
        return 0

    if args.baseline_fresh_trace and not args.baseline_no_mutate:
        print("Warning: --baseline-fresh-trace has no effect without --baseline.", file=sys.stderr)

    runner = CampaignRunner(
        target=args.target,
        dsn=args.dsn,
        isolation=args.isolation,
        corpus_dir=args.corpus,
        rng_seed=args.rng_seed,
        baseline_no_mutate=args.baseline_no_mutate,
        baseline_fresh_trace=args.baseline_fresh_trace,
    )
    runner.explore_parent_rate = min(1.0, max(0.0, float(args.explore_parent_rate)))
    iters = args.iterations if not args.forever else 0
    runner.run(iterations=iters, config=args.config, seed_trace_path=args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
