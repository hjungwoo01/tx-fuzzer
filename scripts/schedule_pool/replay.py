"""
Structured replay verification for ScheduleArtifacts.

Replays an exact schedule N times against a fresh database and reports
hash-match rate and bug-replay rate as first-class metrics.
"""

from __future__ import annotations

import inspect
import threading
import time
from typing import Any, Dict, List, Optional

from .types import FinalStateHash, ReplayResult, ReplayVerification
from .schedule import Schedule, ScheduleArtifact

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DBAdapter, make_adapter
from scripts.experiments.micro_workloads import MICRO_WORKLOADS, MicroWorkload


def _run_single_replay(
    workload: MicroWorkload,
    db: str,
    dsn: str,
    schedule: Schedule,
    variant: str,
    n_workers: int,
    **adapter_kwargs: Any,
) -> Dict[str, Any]:
    """Execute one trial applying exact delays from a schedule.

    Returns dict with canonical_hash, bug_found, bug_reason, elapsed_ms.
    """
    t0 = time.monotonic()

    # Fresh setup
    setup_adapter = make_adapter(db, dsn, **adapter_kwargs)
    try:
        setup_adapter.setup(workload.setup_sql(db))
    finally:
        setup_adapter.close()

    delay_map = schedule.to_delay_map()

    adapters = [
        make_adapter(db, dsn, **adapter_kwargs)
        for _ in range(n_workers)
    ]
    outcomes: List[Dict[str, Any]] = [{}] * n_workers
    error_cats: List[str] = []

    fn = workload.run_buggy if variant == "buggy" else workload.run_fixed

    barrier = threading.Barrier(n_workers, timeout=10)

    def worker(idx: int) -> None:
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass

        begin_key = f"s{idx}_t0_q0_begin"
        if begin_key in delay_map and delay_map[begin_key] > 0:
            time.sleep(delay_map[begin_key])

        exec_delays = [
            v for k, v in delay_map.items()
            if k.startswith(f"s{idx}_") and "_exec" in k
        ]
        if exec_delays:
            time.sleep(max(exec_delays))

        try:
            result = fn(adapters[idx], idx)
            outcomes[idx] = result
            ec = result.get("error", "OK")
            if ec != "OK":
                error_cats.append(ec)
        except Exception as e:
            outcomes[idx] = {"outcome": "exception", "error": str(e)}
            error_cats.append("OTHER")

    threads = []
    for i in range(n_workers):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=30)

    # Read canonical state
    read_adapter = make_adapter(db, dsn, **adapter_kwargs)
    try:
        sig = inspect.signature(workload.canonical)
        if len(sig.parameters) > 1:
            state = workload.canonical(read_adapter, outcomes)
        else:
            state = workload.canonical(read_adapter)
    finally:
        read_adapter.close()

    for a in adapters:
        try:
            a.close()
        except Exception:
            pass

    bug_found, bug_reason = workload.oracle(outcomes, state)
    elapsed = (time.monotonic() - t0) * 1000
    h = workload.canonical_hash(state)

    return {
        "canonical_hash": h,
        "bug_found": bug_found,
        "bug_reason": bug_reason,
        "elapsed_ms": elapsed,
    }


def verify_replay(
    artifact: ScheduleArtifact,
    *,
    db: str,
    dsn: str,
    n_workers: int = 4,
    n_replays: int = 20,
    **adapter_kwargs: Any,
) -> ReplayVerification:
    """Replay a ScheduleArtifact and verify FinalStateHash consistency.

    Executes the artifact's schedule n_replays times on a fresh database
    and returns a structured ReplayVerification summarizing match rate.

    Args:
        artifact: the ScheduleArtifact to replay
        db: database kind (postgres/mysql/sqlite)
        dsn: connection string
        n_workers: number of concurrent sessions
        n_replays: number of replay executions
        **adapter_kwargs: passed to make_adapter

    Returns:
        ReplayVerification with per-replay results and aggregate stats.
    """
    wl = MICRO_WORKLOADS[artifact.workload]
    expected_hash = artifact.discovered_state_hash or ""

    replays: List[ReplayResult] = []
    hashes: List[str] = []
    bug_count = 0

    for i in range(n_replays):
        result = _run_single_replay(
            workload=wl,
            db=db,
            dsn=dsn,
            schedule=artifact.schedule,
            variant=artifact.variant,
            n_workers=n_workers,
            **adapter_kwargs,
        )
        actual_hash = result["canonical_hash"]
        hashes.append(actual_hash)
        match = actual_hash == expected_hash
        if result["bug_found"]:
            bug_count += 1

        replays.append(ReplayResult(
            replay_idx=i,
            expected_hash=expected_hash,
            actual_hash=actual_hash,
            match=match,
            bug_found=result["bug_found"],
            bug_reason=result["bug_reason"],
            elapsed_ms=result["elapsed_ms"],
        ))

    hash_match_count = sum(1 for r in replays if r.match)
    unique_hashes = len(set(hashes))

    return ReplayVerification(
        artifact_id=artifact.schedule_id,
        expected_hash=expected_hash,
        total_replays=n_replays,
        hash_match_count=hash_match_count,
        hash_match_rate=hash_match_count / max(n_replays, 1),
        bug_replay_count=bug_count,
        bug_replay_rate=bug_count / max(n_replays, 1),
        unique_hashes=unique_hashes,
        replays=replays,
    )
