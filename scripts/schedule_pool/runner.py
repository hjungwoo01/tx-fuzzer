"""
Pool-based schedule search runner.

Integrates the schedule pool with the existing MicroRunner to implement
the pool iteration loop:

  1) Sample a parent schedule (or empty) from the pool
  2) Mutate it to form a candidate schedule
  3) Execute the candidate on a fresh base workload
  4) Observe canonical state / bug outcome
  5) Retain if novel or useful

This module adds "schedule_pool" as a new strategy alongside
stress/random/heuristic.
"""

from __future__ import annotations

import hashlib
import json
import random as _random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DBAdapter, make_adapter

# Deferred imports to break circular: runner → micro_runner → micro_workloads → schedule_pool.__init__ → runner
# These are imported inside the functions/methods that need them.
CoveragePoint = None  # type: ignore[assignment]
MicroRunner = None  # type: ignore[assignment]
TrialResult = None  # type: ignore[assignment]
MicroWorkload = None  # type: ignore[assignment]


MICRO_WORKLOADS = None  # type: ignore[assignment]


def _ensure_imports() -> None:
    """Lazy import of micro_runner/micro_workloads to break circular dependency."""
    global CoveragePoint, MicroRunner, TrialResult, MicroWorkload, MICRO_WORKLOADS
    if CoveragePoint is None:
        from scripts.experiments.micro_runner import (
            CoveragePoint as _CP,
            MicroRunner as _MR,
            TrialResult as _TR,
        )
        from scripts.experiments.micro_workloads import (
            MicroWorkload as _MW,
            MICRO_WORKLOADS as _MWL,
        )
        CoveragePoint = _CP
        MicroRunner = _MR
        TrialResult = _TR
        MicroWorkload = _MW
        MICRO_WORKLOADS = _MWL

from .types import DelayPoint, FinalStateHash, ReplayResult, ReplayVerification
from .schedule import Schedule, ScheduleArtifact
from .event_id import get_workload_event_ids
from .pool import PoolConfig, SchedulePool
from .mutator import MutationConfig, ScheduleMutator
from .retention import RetentionPolicy, RetentionReason
from .base_strategy import BaseStrategy, HeuristicStrategy, RandomStrategy
from .execution import execute_trial
from .injection import InjectionMode

# Backward-compat alias
ScheduleRecord = ScheduleArtifact


class SchedulePoolRunner:
    """
    Execute a single trial using a schedule (delay map) from the pool.

    Delegates to :func:`scripts.schedule_pool.execution.execute_trial`, the
    one executor shared with replay and minimization.

    ``injection_mode`` controls how delay points are honored:

    * ``"start_skew"`` (default, historical): each worker sleeps
      ``begin`` + ``max(exec)`` before its transaction; commit delays are
      ignored.  Kept as the default so published results stay reproducible.
    * ``"step"``: every delay point pauses the worker exactly where it is
      named (before BEGIN, before the q-th statement, before COMMIT).
    """

    def __init__(
        self,
        workload: MicroWorkload,
        db: str,
        dsn: str,
        n_workers: int = 4,
        variant: str = "buggy",
        elle_logger: Any = None,
        injection_mode: str = "start_skew",
        worker_timeout_s: float = 30.0,
        **adapter_kwargs: Any,
    ) -> None:
        self.workload = workload
        self.db = db
        self.dsn = dsn
        self.n_workers = n_workers
        self.variant = variant
        self.elle_logger = elle_logger
        self.injection_mode = InjectionMode.parse(injection_mode)
        self.worker_timeout_s = worker_timeout_s
        self.adapter_kwargs = adapter_kwargs

    def run_trial_with_schedule(self, schedule: Schedule) -> TrialResult:
        """
        Execute one trial applying the delays from a schedule.

        Fresh DB state is guaranteed by re-running the workload setup each call.
        """
        _ensure_imports()
        return execute_trial(
            self.workload,
            db=self.db,
            dsn=self.dsn,
            schedule=schedule,
            variant=self.variant,
            n_workers=self.n_workers,
            mode=self.injection_mode,
            elle_logger=self.elle_logger,
            worker_timeout_s=self.worker_timeout_s,
            adapter_kwargs=self.adapter_kwargs,
        )


def run_pool_search(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    variant: str = "buggy",
    duration_s: float = 60.0,
    n_workers: int = 4,
    seed: int = 42,
    pool: Optional[SchedulePool] = None,
    pool_config: Optional[PoolConfig] = None,
    mutation_config: Optional[MutationConfig] = None,
    base_strategy: Optional[BaseStrategy] = None,
    sample_interval_s: float = 0.5,
    rep: int = 0,
    stabilize_replays: int = 10,
    stabilize_mutations: int = 5,
    injection_mode: str = "start_skew",
    **adapter_kwargs: Any,
) -> Tuple[List[CoveragePoint], List[TrialResult], SchedulePool]:
    """
    Run pool-based schedule search for duration_s.

    The pool is an orthogonal extension layer. When no parent is sampled
    from the pool, a fresh schedule is generated via base_strategy
    (default: RandomStrategy).  Mutation then refines the seed.

    Dual-objective: explores canonical state space, then after first bug hit,
    switches to EXPLOIT phase and runs post-hit stabilization to measure
    replay rate and find more stable/compact bug triggers.

    Args:
        base_strategy: Strategy for generating fresh seed schedules.
                       Defaults to RandomStrategy(max_delay_ms=100).
        stabilize_replays: number of exact replays after first bug hit
        stabilize_mutations: number of focused mutations after first bug hit

    Returns coverage points, trial results, and the final pool.
    """
    _ensure_imports()
    wl = MICRO_WORKLOADS[workload_name]
    rng = _random.Random(seed)

    # Base strategy for fresh seeds (orthogonal design)
    if base_strategy is None:
        base_strategy = RandomStrategy(max_delay_ms=100.0)

    # Initialize pool
    if pool is None:
        pool = SchedulePool(config=pool_config or PoolConfig())

    # Event IDs for this workload
    all_events = get_workload_event_ids(workload_name, n_workers)

    # Mutator
    mutator = ScheduleMutator(
        all_event_ids=all_events,
        config=mutation_config or MutationConfig(),
        rng=rng,
    )

    # Retention policy
    retention = RetentionPolicy(pool)

    # Runner
    pool_runner = SchedulePoolRunner(
        workload=wl,
        db=db,
        dsn=dsn,
        n_workers=n_workers,
        variant=variant,
        injection_mode=injection_mode,
        **adapter_kwargs,
    )
    mode_value = pool_runner.injection_mode.value

    coverage_points: List[CoveragePoint] = []
    trial_results: List[TrialResult] = []
    unique_hashes: set = set()
    bugs_found = 0
    txns_done = 0
    first_bug_time: Optional[float] = None
    stabilized_schedules: set = set()  # content hashes already stabilized
    start = time.monotonic()
    last_sample = start

    while (time.monotonic() - start) < duration_s:
        # 1. Sample or fresh seed from base strategy
        parent_record = pool.sample(rng, workload=workload_name, db=db)
        parent_schedule = parent_record.schedule if parent_record else base_strategy.seed(all_events, rng)
        parent_id = parent_record.schedule_id if parent_record else None

        # 2. Mutate
        candidate_schedule = mutator.mutate(parent_schedule)

        # 3. Execute on fresh workload state
        tr = pool_runner.run_trial_with_schedule(candidate_schedule)
        trial_results.append(tr)
        txns_done += 1
        unique_hashes.add(tr.canonical_hash)
        if tr.bug_found:
            bugs_found += 1

        # 4. Build candidate record
        record = ScheduleRecord(
            parent_schedule_id=parent_id,
            db=db,
            workload=workload_name,
            variant=variant,
            strategy="schedule_pool",
            seed=seed,
            schedule=candidate_schedule,
            discovered_state_hash=tr.canonical_hash,
            bug_found=tr.bug_found,
            bug_reason=tr.bug_reason,
            bug_category=tr.bug_category,
            n_workers=n_workers,
            injection_mode=mode_value,
        )

        # 5. Retention check
        retention.accept(record)

        # 6. Post-hit stabilization: on first bug from a new schedule
        if tr.bug_found and (time.monotonic() - start) < duration_s:
            sched_hash = candidate_schedule.content_hash()
            if sched_hash not in stabilized_schedules:
                stabilized_schedules.add(sched_hash)
                if first_bug_time is None:
                    first_bug_time = time.monotonic() - start
                # 6a. Exact replays to measure replay rate
                replay_hits = 1  # count the initial hit
                for _ in range(stabilize_replays):
                    if (time.monotonic() - start) >= duration_s:
                        break
                    rtr = pool_runner.run_trial_with_schedule(candidate_schedule)
                    trial_results.append(rtr)
                    txns_done += 1
                    unique_hashes.add(rtr.canonical_hash)
                    if rtr.bug_found:
                        bugs_found += 1
                        replay_hits += 1
                replay_rate = replay_hits / (stabilize_replays + 1)
                record.replay_rate_buggy = replay_rate

                # 6b. Focused mutations: small jitter around bug schedule
                for _ in range(stabilize_mutations):
                    if (time.monotonic() - start) >= duration_s:
                        break
                    mut_sched = mutator.mutate(candidate_schedule)
                    mtr = pool_runner.run_trial_with_schedule(mut_sched)
                    trial_results.append(mtr)
                    txns_done += 1
                    unique_hashes.add(mtr.canonical_hash)
                    if mtr.bug_found:
                        bugs_found += 1
                        mut_record = ScheduleRecord(
                            parent_schedule_id=record.schedule_id,
                            db=db,
                            workload=workload_name,
                            variant=variant,
                            strategy="schedule_pool",
                            seed=seed,
                            schedule=mut_sched,
                            discovered_state_hash=mtr.canonical_hash,
                            bug_found=mtr.bug_found,
                            bug_reason=mtr.bug_reason,
                            bug_category=mtr.bug_category,
                            n_workers=n_workers,
                            injection_mode=mode_value,
                        )
                        retention.accept(mut_record)

        # Coverage sampling
        now = time.monotonic()
        if (now - last_sample) >= sample_interval_s:
            elapsed = now - start
            coverage_points.append(CoveragePoint(
                time_s=round(elapsed, 3),
                db=db,
                workload=workload_name,
                strategy="schedule_pool",
                rep=rep,
                unique_states=len(unique_hashes),
                txns_done=txns_done,
                bugs_found=bugs_found,
            ))
            last_sample = now

    # Final sample
    elapsed = time.monotonic() - start
    coverage_points.append(CoveragePoint(
        time_s=round(elapsed, 3),
        db=db,
        workload=workload_name,
        strategy="schedule_pool",
        rep=rep,
        unique_states=len(unique_hashes),
        txns_done=txns_done,
        bugs_found=bugs_found,
    ))

    return coverage_points, trial_results, pool


def run_pure_strategy(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    base_strategy: BaseStrategy,
    variant: str = "buggy",
    duration_s: float = 60.0,
    n_workers: int = 4,
    seed: int = 42,
    sample_interval_s: float = 0.5,
    rep: int = 0,
    strategy_label: Optional[str] = None,
    injection_mode: str = "start_skew",
    **adapter_kwargs: Any,
) -> Tuple[List[CoveragePoint], List[TrialResult]]:
    """
    Pool-free fixed-strategy runner.

    Each trial draws a *fresh* seed schedule from ``base_strategy`` and
    executes it. No retention, no parent sampling, no mutation — so any
    coverage advantage of the pool variants over this is attributable
    purely to the pool layer.

    This is the entry point used by the external-baseline harness
    (``evaluate_baselines.py``) to run PCT, IID-Bernoulli, naive uniform,
    etc., on the same delay-injection substrate.
    """
    _ensure_imports()
    wl = MICRO_WORKLOADS[workload_name]
    rng = _random.Random(seed)

    all_events = get_workload_event_ids(workload_name, n_workers)

    pool_runner = SchedulePoolRunner(
        workload=wl,
        db=db,
        dsn=dsn,
        n_workers=n_workers,
        variant=variant,
        injection_mode=injection_mode,
        **adapter_kwargs,
    )

    label = strategy_label or base_strategy.name

    coverage_points: List[CoveragePoint] = []
    trial_results: List[TrialResult] = []
    unique_hashes: set = set()
    bugs_found = 0
    txns_done = 0
    start = time.monotonic()
    last_sample = start

    while (time.monotonic() - start) < duration_s:
        sched = base_strategy.seed(all_events, rng)
        tr = pool_runner.run_trial_with_schedule(sched)
        trial_results.append(tr)
        txns_done += 1
        unique_hashes.add(tr.canonical_hash)
        if tr.bug_found:
            bugs_found += 1

        now = time.monotonic()
        if (now - last_sample) >= sample_interval_s:
            elapsed = now - start
            coverage_points.append(CoveragePoint(
                time_s=round(elapsed, 3),
                db=db,
                workload=workload_name,
                strategy=label,
                rep=rep,
                unique_states=len(unique_hashes),
                txns_done=txns_done,
                bugs_found=bugs_found,
            ))
            last_sample = now

    elapsed = time.monotonic() - start
    coverage_points.append(CoveragePoint(
        time_s=round(elapsed, 3),
        db=db,
        workload=workload_name,
        strategy=label,
        rep=rep,
        unique_states=len(unique_hashes),
        txns_done=txns_done,
        bugs_found=bugs_found,
    ))

    return coverage_points, trial_results


def run_random_pool_search(
    *,
    max_delay_ms: float = 100.0,
    **kwargs: Any,
) -> Tuple[List[CoveragePoint], List[TrialResult], SchedulePool]:
    """Run the pool workflow with Random as the fresh-seed scheduler."""
    return run_pool_search(
        base_strategy=RandomStrategy(max_delay_ms=max_delay_ms),
        **kwargs,
    )


def run_heuristic_pool_search(
    *,
    k: int = 4,
    min_delay_ms: float = 5.0,
    max_delay_ms: float = 50.0,
    **kwargs: Any,
) -> Tuple[List[CoveragePoint], List[TrialResult], SchedulePool]:
    """Run the pool workflow with Heuristic as the fresh-seed scheduler."""
    return run_pool_search(
        base_strategy=HeuristicStrategy(
            k=k,
            min_delay_ms=min_delay_ms,
            max_delay_ms=max_delay_ms,
        ),
        **kwargs,
    )


def replay_schedule(
    *,
    db: str,
    dsn: str,
    workload_name: str,
    schedule: Schedule,
    variant: str = "buggy",
    n_workers: int = 4,
    n_replays: int = 20,
    injection_mode: str = "start_skew",
    **adapter_kwargs: Any,
) -> Dict[str, Any]:
    """
    Replay an exact schedule n_replays times for determinism measurement.

    Returns dict with:
      total, bug_count, bug_rate, outcomes, canonical_hashes
    """
    _ensure_imports()
    wl = MICRO_WORKLOADS[workload_name]
    runner = SchedulePoolRunner(
        workload=wl,
        db=db,
        dsn=dsn,
        n_workers=n_workers,
        variant=variant,
        injection_mode=injection_mode,
        **adapter_kwargs,
    )

    results: List[Dict[str, Any]] = []
    bug_count = 0
    hashes: List[str] = []

    for i in range(n_replays):
        tr = runner.run_trial_with_schedule(schedule)
        hit = tr.bug_found
        if hit:
            bug_count += 1
        hashes.append(tr.canonical_hash)
        results.append({
            "replay": i,
            "bug_found": hit,
            "canonical_hash": tr.canonical_hash,
            "elapsed_ms": tr.elapsed_ms,
        })

    unique_hashes = len(set(hashes))
    return {
        "total": n_replays,
        "bug_count": bug_count,
        "bug_rate": bug_count / max(n_replays, 1),
        "unique_canonical_hashes": unique_hashes,
        "replays": results,
    }
