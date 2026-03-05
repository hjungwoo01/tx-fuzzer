"""
Independent Coverage-Guided Evolutionary Fuzzer.

This script intentionally does NOT modify or depend on `run_campaign.py`.
It is a separate pipeline that implements a full evolutionary loop:

1. Seed Corpus (Gene Pool)
   - Generate one baseline trace with the Go runner (`cmd/runner`).
   - Initialize `Corpus` with that un-mutated trace.

2. Parent Selection (Multi-Armed Bandit)
   - Select a parent from `Corpus` using UCB-style scoring + epsilon exploration.
   - Favor parents that discovered novel states recently, but still explore older arms.

3. Mutation Stacking (Genetic Drift)
   - Mutate by stacking delay injections:
     * add delay to a new operation, OR
     * tweak an existing delay slightly.
   - Existing delays are never cleared; they accumulate generation-over-generation.

4. Coverage Oracle (Fitness Function)
   - Replay mutated trace via `scripts/replay_litmus.py`.
   - Hash only the final KV state.
   - Duplicate hash => discard trace.
   - Novel hash => save artifacts and immediately append this trace into `Corpus`.

5. Retention Output
   - Successful novel traces are saved under `corpus_evolutionary_v1/`.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import math
import os
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = ROOT / "scripts"

# Allow importing run_litmus defaults without touching existing files.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from run_litmus import DEFAULT_DSN  # type: ignore[import]


# ----------------------------
# Trace/replay utility helpers
# ----------------------------

def _event_counts_toward_replay(ev: Dict[str, Any]) -> bool:
    """Return True when replay_litmus would execute this event."""
    raw_type = str(ev.get("type", "")).lower()
    if raw_type in {"begin", "commit", "rollback", "sql", "invoke"}:
        return True
    return False


def _event_sql(ev: Dict[str, Any]) -> str:
    return str(ev.get("sql", "")).strip()


def _is_write_event(ev: Dict[str, Any]) -> bool:
    """Best-effort write detector used to bias delay injection."""
    if not _event_counts_toward_replay(ev):
        return False
    sql = _event_sql(ev).upper()
    return sql.startswith(("UPDATE", "INSERT", "DELETE", "UPSERT", "MERGE", "REPLACE"))


def _to_jsonable(value: Any) -> Any:
    """Convert values into deterministic JSON-safe structures."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, tuple):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    return str(value)


def _final_state_row_to_pair(row: Any) -> Optional[List[Any]]:
    if isinstance(row, dict):
        if "k" in row and "v" in row:
            return [_to_jsonable(row.get("k")), _to_jsonable(row.get("v"))]
        return None
    if isinstance(row, (list, tuple)) and len(row) >= 2:
        return [_to_jsonable(row[0]), _to_jsonable(row[1])]
    return None


def _state_sort_key(value: Any) -> Tuple[str, str]:
    canonical = _to_jsonable(value)
    return (
        type(canonical).__name__,
        json.dumps(canonical, sort_keys=True, separators=(",", ":")),
    )


def _canonical_final_state(final_state: Any) -> List[List[Any]]:
    """Normalize row order/type rendering so equal states hash identically."""
    rows = final_state if isinstance(final_state, list) else []
    pairs: List[List[Any]] = []
    for row in rows:
        pair = _final_state_row_to_pair(row)
        if pair is not None:
            pairs.append(pair)
    pairs.sort(key=lambda item: (_state_sort_key(item[0]), _state_sort_key(item[1])))
    return pairs


def _output_state_hash(payload: Dict[str, Any]) -> str:
    """Coverage hash: only final state contributes to novelty identity."""
    fingerprint = {"final_state": _canonical_final_state(payload.get("final_state", []))}
    encoded = json.dumps(fingerprint, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _coerce_output_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _last_step_from_output(stdout: Any, stderr: Any) -> str:
    combined = _coerce_output_text(stdout) + "\n" + _coerce_output_text(stderr)
    steps = [int(m.group(1)) for m in re.finditer(r"step[=\s]+(\d+)", combined, re.IGNORECASE)]
    return str(max(steps)) if steps else "?"


def _parse_state_payload(stdout: str) -> Optional[Dict[str, Any]]:
    """
    replay_litmus emits a JSON line:
      {"execution_history": [...], "final_state": [...]}
    Parse the last valid payload-looking line.
    """
    for line in reversed((stdout or "").splitlines()):
        text = line.strip()
        if not text or not text.startswith("{"):
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        if "final_state" not in parsed:
            continue
        return {
            "execution_history": _to_jsonable(parsed.get("execution_history", [])),
            "final_state": _to_jsonable(parsed.get("final_state", [])),
        }
    return None


def _normalize_isolation(value: str) -> str:
    iso = (value or "").strip().upper().replace("_", " ")
    if iso == "SERIALIZATION":
        return "SERIALIZABLE"
    return iso or "READ COMMITTED"


def _extract_dsn_from_workload_config(config_path: Path) -> Optional[str]:
    """
    Read `database.dsn` from workload YAML. This keeps generation and replay
    pointed at the same database when --dsn is not passed.
    """
    text = config_path.read_text(encoding="utf-8")

    # Preferred path: YAML parser when available.
    try:
        import yaml  # type: ignore[import]

        payload = yaml.safe_load(text)
        if isinstance(payload, dict):
            db = payload.get("database")
            if isinstance(db, dict):
                dsn = db.get("dsn")
                if isinstance(dsn, str) and dsn.strip():
                    return dsn.strip()
    except Exception:
        pass

    # Fallback: simple regex.
    match = re.search(r'^\s*dsn:\s*["\']?([^"\']+)["\']?\s*$', text, flags=re.MULTILINE)
    if match:
        candidate = match.group(1).strip()
        if candidate:
            return candidate
    return None


def _resolve_python_executable() -> str:
    """
    Prefer project virtualenv interpreter when present so replay_litmus.py can
    access optional DB adapters (e.g., psycopg2) even if current shell python differs.
    """
    venv_python = ROOT / "venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _parse_scalar_token(token: str) -> Any:
    """Parse a simple EDN-ish scalar token into Python value."""
    raw = token.strip()
    if not raw:
        return None
    if raw == "nil":
        return None
    if raw == "true":
        return True
    if raw == "false":
        return False
    if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
        inner = raw[1:-1]
        try:
            return json.loads(f"\"{inner}\"")
        except Exception:
            return inner.replace('\\"', '"').replace("\\\\", "\\")
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def _parse_args_fragment(args_fragment: str) -> List[Any]:
    """Parse `:args [ ... ]` content with a light token parser."""
    args: List[Any] = []
    fragment = (args_fragment or "").strip()
    if not fragment:
        return args
    token_re = re.compile(r'"(?:\\.|[^"\\])*"|[^\s\]]+')
    for token in token_re.findall(fragment):
        args.append(_parse_scalar_token(token))
    return args


def _parse_query_event_line_regex(line: str) -> Optional[Dict[str, Any]]:
    """
    Regex-only parser for one history line. This is dependency-free and
    intentionally permissive so malformed non-query lines won't break seed generation.
    """
    if ":f :query" not in line or ":value" not in line:
        return None

    type_match = re.search(r":type\s+:([A-Za-z0-9_-]+)", line)
    process_match = re.search(r":process\s+(-?\d+)", line)
    sql_match = re.search(r':sql\s+"((?:\\.|[^"\\])*)"', line)
    args_match = re.search(r":args\s+\[([^\]]*)\]", line)
    if type_match is None or sql_match is None:
        return None

    event_type = type_match.group(1).lower()
    process = int(process_match.group(1)) if process_match else 0
    sql = _parse_scalar_token(f"\"{sql_match.group(1)}\"")
    args = _parse_args_fragment(args_match.group(1)) if args_match else []

    return {
        "type": event_type,
        "f": "query",
        "process": process,
        "value": {
            "sql": str(sql),
            "args": args,
        },
    }


def _load_history_tolerant(history_path: Path) -> Tuple[List[Dict[str, Any]], int]:
    """
    Best-effort history loader with two stages:
      1) EDN line parsing when `edn_format` is available.
      2) Regex-only parsing fallback (no extra deps).
    Returns (events, dropped_lines_count).
    """
    raw_text = history_path.read_text(encoding="utf-8")
    lines = raw_text.splitlines()
    dropped = 0
    events: List[Dict[str, Any]] = []

    # Stage 1: EDN parser when available.
    try:
        from collections.abc import Mapping as AbcMapping, Sequence as AbcSequence

        from edn_format import Keyword, Symbol, loads as edn_loads  # type: ignore[import]

        def convert_edn(value: Any) -> Any:
            if isinstance(value, Keyword):
                return value.name
            if isinstance(value, Symbol):
                return value.name
            if isinstance(value, AbcMapping):
                return {convert_edn(k): convert_edn(v) for k, v in value.items()}
            if isinstance(value, (list, tuple, AbcSequence)) and not isinstance(value, (str, bytes)):
                return [convert_edn(v) for v in value]
            if isinstance(value, set):
                return {convert_edn(v) for v in value}
            return value

        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith(";"):
                continue
            try:
                parsed = edn_loads(line)
            except Exception:
                dropped += 1
                continue
            converted = convert_edn(parsed)
            if isinstance(converted, dict):
                events.append(converted)
        if events:
            return events, dropped
    except Exception:
        # Stage 2 handles missing edn_format or parse failures.
        pass

    # Stage 2: dependency-free regex parser.
    dropped = 0
    events = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith(";"):
            continue
        parsed = _parse_query_event_line_regex(line)
        if parsed is None:
            dropped += 1
            continue
        events.append(parsed)
    return events, dropped


def _history_events_to_trace(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Minimal history->trace conversion independent of scripts/run_random.py.
    Keeps query invoke/ok/fail events and preserves SQL + args as recorded.
    """
    trace: List[Dict[str, Any]] = []
    for ev in events:
        f = str(ev.get("f", "")).lstrip(":")
        if f != "query":
            continue
        ev_type = str(ev.get("type", "")).lower()
        if ev_type not in {"invoke", "ok", "fail"}:
            continue
        value = ev.get("value")
        if not isinstance(value, dict):
            continue
        sql = value.get("sql")
        if not sql:
            continue
        process_raw = ev.get("process", 0)
        try:
            process = int(process_raw)
        except Exception:
            process = 0
        raw_args = value.get("args")
        args = list(raw_args) if isinstance(raw_args, list) else []
        trace.append(
            {
                "client_id": process,
                "sql": str(sql),
                "args": args,
                "type": ev_type,
            }
        )
    return trace


# ----------------------------
# Bandit and campaign structs
# ----------------------------

@dataclass
class BanditArm:
    """
    Per-parent statistics for the multi-armed bandit policy.

    selections:
      How often this parent was selected to produce a child.
    novelty_hits:
      How often this parent produced a novel state.
    last_novel_iteration:
      Recency signal. Higher recency means stronger short-term exploitation.
    """

    selections: int = 0
    novelty_hits: int = 0
    last_novel_iteration: int = 0

    def mean_reward(self) -> float:
        if self.selections <= 0:
            return 0.0
        return float(self.novelty_hits) / float(self.selections)


class EvolutionaryCampaign:
    """
    Full evolutionary fuzzing loop with a state-coverage retention oracle.
    """

    def __init__(
        self,
        *,
        config: Path,
        target: str,
        dsn: str,
        isolation: str,
        iterations: int,
        rng_seed: Optional[int],
        output_dir: Path,
        replay_timeout_seconds: float,
        bandit_epsilon: float,
        bandit_ucb_scale: float,
        bandit_recency_weight: float,
    ) -> None:
        self.config = config
        self.target = target
        self.dsn = dsn
        self.isolation = _normalize_isolation(isolation)
        self.iterations = max(0, int(iterations))
        self.output_dir = output_dir
        self.replay_timeout_seconds = max(1.0, float(replay_timeout_seconds))

        # Multi-armed bandit knobs (exploration/exploitation controls).
        self.bandit_epsilon = min(1.0, max(0.0, float(bandit_epsilon)))
        self.bandit_ucb_scale = max(0.0, float(bandit_ucb_scale))
        self.bandit_recency_weight = max(0.0, float(bandit_recency_weight))

        self.rng = random.Random(rng_seed)
        self.python_executable = _resolve_python_executable()

        # Required component: Seed corpus starts from one un-mutated generated trace.
        self.Corpus: List[List[Dict[str, Any]]] = []
        self.bandit_arms: List[BanditArm] = []

        # Coverage oracle memory: final-state hashes seen so far.
        self.seen_state_hashes: set[str] = set()

        # Scratch and artifact paths.
        self.logs_dir = ROOT / "logs"
        self.seed_trace_path = self.logs_dir / "evolve_seed_trace.json"
        self.seed_history_path = self.logs_dir / "evolve_seed_history.edn"
        self.temp_trace_path = self.logs_dir / "evolve_temp_trace.json"
        self.index_path = self.output_dir / "states_index.jsonl"

        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.novel_count = 0
        self.fail_count = 0
        self.duplicate_count = 0
        # Keep replay cost bounded even when seed history is very large.
        self.seed_trace_events = 400

    # ----------------------------
    # Seed generation / bootstraps
    # ----------------------------

    def _run_go_seed_history(self) -> None:
        """
        Generate fresh seed history directly with the Go runner.
        This avoids Python-side optional dependency issues in run_random.py.
        """
        cmd = [
            "go",
            "run",
            "./cmd/runner",
            "-config",
            str(self.config),
            "-out",
            str(self.seed_history_path),
        ]
        env = os.environ.copy()
        if not env.get("DATABASE_URL"):
            env["DATABASE_URL"] = self.dsn
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=False,
            check=False,
        )
        if proc.returncode != 0:
            out = _coerce_output_text(proc.stdout)
            err = _coerce_output_text(proc.stderr)
            raise RuntimeError(
                "Seed generation failed via Go runner.\n"
                f"cmd={' '.join(cmd)}\n"
                f"stdout:\n{out}\n"
                f"stderr:\n{err}"
            )

    def _generate_initial_seed_trace(self) -> List[Dict[str, Any]]:
        """
        Produce one un-mutated seed trace:
          Go runner history -> tolerant history parser -> replay trace.
        """
        self._run_go_seed_history()
        if not self.seed_history_path.exists():
            raise RuntimeError(f"Seed history file missing: {self.seed_history_path}")

        events, dropped = _load_history_tolerant(self.seed_history_path)
        if not events:
            raise RuntimeError(
                "Seed history parsing produced zero events. "
                f"history_file={self.seed_history_path}"
            )

        raw_trace = _history_events_to_trace(events)
        if not raw_trace:
            raise RuntimeError("Seed history contained no query events usable for replay.")

        try:
            with self.seed_trace_path.open("w", encoding="utf-8") as f:
                json.dump(raw_trace, f, indent=2)
        except Exception:
            pass

        print(
            "[evolve] seed history parsed "
            f"(events={len(events)}, dropped_lines={dropped}, trace_events={len(raw_trace)})."
        )

        replayable = [copy.deepcopy(ev) for ev in raw_trace if _event_counts_toward_replay(ev)]
        if not replayable:
            raise RuntimeError("Generated seed trace had no replayable events.")
        target_len = min(self.seed_trace_events, len(replayable))
        if len(replayable) <= target_len:
            return replayable

        max_start = len(replayable) - target_len
        best_start = self.rng.randint(0, max_start)
        best_score = -1
        attempts = min(64, max_start + 1)
        for _ in range(attempts):
            start = self.rng.randint(0, max_start)
            window = replayable[start:start + target_len]
            write_count = sum(1 for ev in window if _is_write_event(ev))
            client_count = len({ev.get("client_id") for ev in window})
            score = (write_count * 10) + client_count
            if score > best_score:
                best_score = score
                best_start = start

        return replayable[best_start:best_start + target_len]

    def _initialize_seed_corpus(self) -> None:
        seed_trace = self._generate_initial_seed_trace()
        self.Corpus = [seed_trace]
        self.bandit_arms = [BanditArm()]
        print(
            f"[evolve] seed corpus initialized with 1 trace "
            f"({len(seed_trace)} replayable events)."
        )

        # Prime the coverage memory with the un-mutated seed run.
        print("[evolve] replaying seed trace once to initialize baseline state hash...")
        success, payload, err_sig, duration = self._execute_trace(seed_trace)
        if not success or payload is None:
            print(
                "[evolve] warning: seed replay failed; continuing with empty seen-state set "
                f"(error={err_sig}, duration={duration:.3f}s)."
            )
            return

        state_hash = _output_state_hash(payload)
        self.seen_state_hashes.add(state_hash)
        self._save_success_artifacts(
            trace=seed_trace,
            payload=payload,
            state_hash=state_hash,
            iteration=0,
            parent_index=0,
            mutation_note="seed_unmutated_baseline",
            replay_duration=duration,
        )
        print(
            "[evolve] baseline state recorded "
            f"(hash={state_hash}, seen_states={len(self.seen_state_hashes)})."
        )

    # ----------------------------
    # Multi-armed bandit selection
    # ----------------------------

    def _select_parent_index(self, iteration: int) -> int:
        """
        Parent selection policy:
        - epsilon branch: random exploration.
        - otherwise: UCB + recency-bias exploitation.
        """
        if not self.Corpus:
            raise RuntimeError("Corpus is empty; seed initialization failed.")
        if len(self.Corpus) == 1:
            return 0

        if self.rng.random() < self.bandit_epsilon:
            return self.rng.randrange(len(self.Corpus))

        total_pulls = sum(arm.selections for arm in self.bandit_arms) + 1
        scores: List[float] = []
        for arm in self.bandit_arms:
            mean_reward = arm.mean_reward()
            exploration_bonus = self.bandit_ucb_scale * math.sqrt(
                math.log(total_pulls + 1.0) / (arm.selections + 1.0)
            )

            # Recency: recently successful parents get a temporary boost.
            if arm.last_novel_iteration > 0:
                age = max(0, iteration - arm.last_novel_iteration)
                recency_bonus = 1.0 / (1.0 + float(age))
            else:
                recency_bonus = 0.0

            score = mean_reward + exploration_bonus + (self.bandit_recency_weight * recency_bonus)
            scores.append(score)

        max_score = max(scores)
        candidate_indices = [i for i, score in enumerate(scores) if abs(score - max_score) <= 1e-12]
        return self.rng.choice(candidate_indices)

    def _update_parent_reward(self, parent_index: int, novel: bool, iteration: int) -> None:
        arm = self.bandit_arms[parent_index]
        arm.selections += 1
        if novel:
            arm.novelty_hits += 1
            arm.last_novel_iteration = iteration

    # ----------------------------
    # Delay-stacking mutation
    # ----------------------------

    @staticmethod
    def _event_has_delay(ev: Dict[str, Any]) -> bool:
        try:
            float(ev.get("delay"))
            return "delay" in ev
        except (TypeError, ValueError):
            return False

    def _stack_delay_mutation(self, parent_trace: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], str]:
        """
        Mutation-stacking rule:
          - never remove inherited delays,
          - either tweak one existing delay OR add a delay on a new operation.
        """
        child = copy.deepcopy(parent_trace)
        replayable = [i for i, ev in enumerate(child) if _event_counts_toward_replay(ev)]
        if not replayable:
            return child, "no_replayable_events"

        delayed = [i for i in replayable if self._event_has_delay(child[i])]
        non_delayed = [i for i in replayable if i not in delayed]

        # Prefer stacking by adding new delays when possible; tweak otherwise.
        do_tweak = bool(delayed) and (not non_delayed or self.rng.random() < 0.40)
        if do_tweak:
            idx = self.rng.choice(delayed)
            old_delay = float(child[idx]["delay"])
            scale = self.rng.uniform(0.85, 1.20)
            jitter = self.rng.uniform(-0.004, 0.004)
            new_delay = old_delay * scale + jitter
            new_delay = max(0.001, min(0.350, new_delay))
            child[idx]["delay"] = round(new_delay, 4)
            mutation_note = f"tweak delay@event[{idx}] {old_delay:.4f}s->{new_delay:.4f}s"
        else:
            write_candidates = [i for i in non_delayed if _is_write_event(child[i])]
            idx = self.rng.choice(write_candidates or non_delayed)
            base_delay = self.rng.choice([0.004, 0.006, 0.010, 0.015, 0.020, 0.030, 0.050, 0.080, 0.120])
            child[idx]["delay"] = float(base_delay)
            mutation_note = f"add delay@event[{idx}]={base_delay:.3f}s"

        # Optional second additive mutation to accelerate multi-delay buildup.
        if self.rng.random() < 0.30:
            delayed_after_first = [i for i in replayable if self._event_has_delay(child[i])]
            remaining = [i for i in replayable if i not in delayed_after_first]
            if remaining:
                write_candidates = [i for i in remaining if _is_write_event(child[i])]
                idx2 = self.rng.choice(write_candidates or remaining)
                extra_delay = self.rng.choice([0.003, 0.005, 0.008, 0.012, 0.018, 0.025, 0.040])
                child[idx2]["delay"] = float(extra_delay)
                mutation_note += f"; stack delay@event[{idx2}]={extra_delay:.3f}s"

        return child, mutation_note

    # ----------------------------
    # Replay execution
    # ----------------------------

    def _execute_trace(
        self,
        trace: List[Dict[str, Any]],
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str], float]:
        """
        Replay one trace and return:
          (success, payload_or_none, error_signature_or_none, duration_seconds)
        """
        with self.temp_trace_path.open("w", encoding="utf-8") as f:
            json.dump(trace, f, indent=2)

        cmd = [
            self.python_executable,
            str(SCRIPTS_DIR / "replay_litmus.py"),
            "--trace",
            str(self.temp_trace_path),
            "--target",
            self.target,
            "--dsn",
            self.dsn,
            "--isolation",
            self.isolation,
        ]

        start = time.monotonic()
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(ROOT),
                capture_output=True,
                text=False,
                timeout=self.replay_timeout_seconds,
                check=False,
            )
            duration = time.monotonic() - start
        except subprocess.TimeoutExpired as exc:
            duration = time.monotonic() - start
            step = _last_step_from_output(getattr(exc, "stdout", None), getattr(exc, "stderr", None))
            return False, None, f"TIMEOUT_AT_STEP_{step}", duration

        stdout = _coerce_output_text(proc.stdout)
        stderr = _coerce_output_text(proc.stderr)

        if proc.returncode != 0:
            step = _last_step_from_output(stdout, stderr)
            combined = (stdout + "\n" + stderr).lower()
            if "deadlock" in combined or "timeout" in combined or "canceled" in combined:
                return False, None, f"TIMEOUT_AT_STEP_{step}", duration
            return False, None, f"ERROR_AT_STEP_{step}", duration

        payload = _parse_state_payload(stdout)
        if payload is None:
            return False, None, "MISSING_FINAL_STATE_PAYLOAD", duration
        return True, payload, None, duration

    # ----------------------------
    # Coverage oracle + retention
    # ----------------------------

    def _save_success_artifacts(
        self,
        *,
        trace: List[Dict[str, Any]],
        payload: Dict[str, Any],
        state_hash: str,
        iteration: int,
        parent_index: int,
        mutation_note: str,
        replay_duration: float,
    ) -> None:
        """Persist trace + outcome so discoveries are reproducible."""
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
        trace_path = self.output_dir / f"trace_{state_hash}.json"
        state_path = self.output_dir / f"state_{state_hash}.json"

        with trace_path.open("w", encoding="utf-8") as f:
            json.dump(trace, f, indent=2)

        record = {
            "hash": state_hash,
            "timestamp": timestamp,
            "iteration": iteration,
            "parent_index": parent_index,
            "mutation_note": mutation_note,
            "target": self.target,
            "dsn": self.dsn,
            "isolation": self.isolation,
            "replay_duration_seconds": replay_duration,
            "trace_file": str(trace_path),
            "execution_history": _to_jsonable(payload.get("execution_history", [])),
            "final_state": _to_jsonable(payload.get("final_state", [])),
        }
        with state_path.open("w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)

        with self.index_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")

    def _coverage_oracle_and_retain(
        self,
        *,
        child_trace: List[Dict[str, Any]],
        payload: Dict[str, Any],
        iteration: int,
        parent_index: int,
        mutation_note: str,
        replay_duration: float,
        append_to_corpus: bool,
    ) -> bool:
        """
        Coverage oracle:
        - seen hash => discard
        - novel hash => save + retain
        """
        state_hash = _output_state_hash(payload)
        if state_hash in self.seen_state_hashes:
            return False

        self.seen_state_hashes.add(state_hash)
        self.novel_count += 1
        self._save_success_artifacts(
            trace=child_trace,
            payload=payload,
            state_hash=state_hash,
            iteration=iteration,
            parent_index=parent_index,
            mutation_note=mutation_note,
            replay_duration=replay_duration,
        )

        if append_to_corpus:
            # Critical retention rule: immediately add this exact mutated trace
            # as a viable future parent.
            self.Corpus.append(copy.deepcopy(child_trace))
            self.bandit_arms.append(BanditArm(last_novel_iteration=iteration))

        print(
            f"[evolve] NEW_STATE hash={state_hash} iteration={iteration} "
            f"parent={parent_index} corpus_size={len(self.Corpus)} "
            f"note={mutation_note}"
        )
        return True

    # ----------------------------
    # Main evolutionary loop
    # ----------------------------

    def run(self) -> None:
        self._initialize_seed_corpus()
        print(
            f"[evolve] starting evolutionary loop: iterations={self.iterations} "
            f"output_dir={self.output_dir}"
        )

        for iteration in range(1, self.iterations + 1):
            parent_index = self._select_parent_index(iteration)
            parent_trace = self.Corpus[parent_index]

            child_trace, mutation_note = self._stack_delay_mutation(parent_trace)
            success, payload, err_sig, duration = self._execute_trace(child_trace)
            if not success or payload is None:
                self.fail_count += 1
                self._update_parent_reward(parent_index, novel=False, iteration=iteration)
                print(
                    f"[evolve] iteration={iteration} replay_failed "
                    f"parent={parent_index} error={err_sig} duration={duration:.3f}s"
                )
                continue

            novel = self._coverage_oracle_and_retain(
                child_trace=child_trace,
                payload=payload,
                iteration=iteration,
                parent_index=parent_index,
                mutation_note=mutation_note,
                replay_duration=duration,
                append_to_corpus=True,
            )
            self._update_parent_reward(parent_index, novel=novel, iteration=iteration)
            if not novel:
                self.duplicate_count += 1

            if iteration % 10 == 0 or novel:
                print(
                    f"[evolve] status iteration={iteration} novel={self.novel_count} "
                    f"dupe={self.duplicate_count} fail={self.fail_count} "
                    f"seen={len(self.seen_state_hashes)} corpus={len(self.Corpus)}"
                )

        print("[evolve] completed.")
        print(
            f"[evolve] summary: iterations={self.iterations}, novel={self.novel_count}, "
            f"duplicates={self.duplicate_count}, failures={self.fail_count}, "
            f"final_corpus_size={len(self.Corpus)}, output_dir={self.output_dir}"
        )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Independent coverage-guided evolutionary campaign with corpus retention "
            "and delay-stacking mutations."
        ),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=ROOT / "evolve_workload.yaml",
        help="Workload YAML used to generate the initial seed trace.",
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
        help="Database DSN (defaults from scripts/run_litmus.py).",
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help="Isolation level passed to replay_litmus.py.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=500,
        help="Number of evolutionary iterations.",
    )
    parser.add_argument(
        "--rng-seed",
        type=int,
        default=None,
        help="Random seed for reproducible parent/mutation choices.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "corpus_evolutionary_v1",
        help="Directory for successful novel traces and state records.",
    )
    parser.add_argument(
        "--replay-timeout-seconds",
        type=float,
        default=12.0,
        help="Per-replay timeout used by the evolutionary loop.",
    )
    parser.add_argument(
        "--bandit-epsilon",
        type=float,
        default=0.15,
        help="Probability of random parent exploration.",
    )
    parser.add_argument(
        "--bandit-ucb-scale",
        type=float,
        default=0.90,
        help="UCB exploration coefficient.",
    )
    parser.add_argument(
        "--bandit-recency-weight",
        type=float,
        default=0.60,
        help="Additional score weight for recently successful parents.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    config = args.config.resolve()
    if not config.exists():
        print(f"[evolve] config not found: {config}", file=sys.stderr)
        return 2

    dsn = args.dsn or _extract_dsn_from_workload_config(config) or DEFAULT_DSN.get(args.target)
    if not dsn:
        print(f"[evolve] no default DSN for target={args.target!r}; pass --dsn", file=sys.stderr)
        return 2

    campaign = EvolutionaryCampaign(
        config=config,
        target=args.target,
        dsn=dsn,
        isolation=args.isolation,
        iterations=args.iterations,
        rng_seed=args.rng_seed,
        output_dir=args.output_dir.resolve(),
        replay_timeout_seconds=args.replay_timeout_seconds,
        bandit_epsilon=args.bandit_epsilon,
        bandit_ucb_scale=args.bandit_ucb_scale,
        bandit_recency_weight=args.bandit_recency_weight,
    )
    campaign.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
