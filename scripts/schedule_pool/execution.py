"""
The single trial executor shared by exploration, replay, and minimization.

One trial =
  1. reset the database to the workload's seed state (variant-aware setup),
  2. start ``n_workers`` threads behind a barrier, each running the chosen
     variant's transaction through an :class:`InstrumentedAdapter`
     (so schedule delay points are injected and every call is traced),
  3. wait with one shared deadline; interrupt and label any stuck worker,
  4. read the canonical end state through a :class:`StrictAdapter`
     (a failed read is a HarnessError, never a "finding"),
  5. ask the workload's oracle for a categorized verdict.

Previously this logic was copy-pasted in ``runner.py``, ``replay.py`` and
``micro_runner.py`` and had drifted (e.g. per-thread 30 s joins, a shared
``[{}] * n`` outcome list, canonical reads that swallowed SQL errors).
"""

from __future__ import annotations

import inspect
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from .injection import (
    HarnessError,
    InjectionMode,
    InstrumentedAdapter,
    StrictAdapter,
    TraceEvent,
    WorkerPlan,
    interrupt_adapter,
)
from .schedule import Schedule
from .types import BugCategory


def _setup_statements(workload: Any, db: str, variant: str) -> List[str]:
    setup_for = getattr(workload, "setup_for", None)
    if callable(setup_for):
        return setup_for(db, variant)
    return workload.setup_sql(db)


def _variant_callable(workload: Any, variant: str) -> Callable[..., Dict[str, Any]]:
    variant_fn = getattr(workload, "variant_fn", None)
    if callable(variant_fn):
        return variant_fn(variant)
    return workload.run_buggy if variant == "buggy" else workload.run_fixed


def _verdict(workload: Any, outcomes: List[Dict[str, Any]], state: Dict[str, Any]):
    verdict_fn = getattr(workload, "verdict", None)
    if callable(verdict_fn):
        v = verdict_fn(outcomes, state)
        return v.bug_found, v.reason, v.category
    bug, reason = workload.oracle(outcomes, state)
    return bug, reason, (BugCategory.ISOLATION_ANOMALY if bug else BugCategory.NONE)


def execute_trial(
    workload: Any,
    *,
    db: str,
    dsn: str,
    schedule: Schedule,
    variant: str = "buggy",
    n_workers: int = 4,
    mode: Any = InjectionMode.START_SKEW,
    elle_logger: Any = None,
    worker_timeout_s: float = 30.0,
    strict_canonical: bool = True,
    stagger_ms: float = 0.0,
    adapter_kwargs: Optional[Dict[str, Any]] = None,
):
    """Run one trial of ``workload`` under ``schedule``; return a TrialResult.

    ``stagger_ms`` > 0 starts worker ``w`` at ``w * stagger_ms`` after the
    barrier.  With a stagger longer than one transaction the *baseline* is
    serial execution, so every anomaly found is attributable to the
    schedule's pauses (the minimal schedule then explains the bug).  With 0
    (default, historical) all workers start together, which already races.
    """
    from scripts.experiments.db_adapter import make_adapter
    from scripts.experiments.micro_runner import TrialResult

    mode = InjectionMode.parse(mode)
    akw = dict(adapter_kwargs or {})
    t_trial = time.monotonic()

    # 1. Fresh seed state.
    setup_adapter = make_adapter(db, dsn, **akw)
    try:
        setup_adapter.setup(_setup_statements(workload, db, variant))
    finally:
        setup_adapter.close()

    fn = _variant_callable(workload, variant)
    plans = [WorkerPlan.from_schedule(schedule, w) for w in range(n_workers)]

    # Shared trial clock: reset by the barrier action at the moment all
    # workers are released, so trace times are comparable across workers.
    t0 = [time.perf_counter()]

    def clock() -> float:
        return (time.perf_counter() - t0[0]) * 1000.0

    def _release() -> None:
        t0[0] = time.perf_counter()

    raw: List[Any] = []
    try:
        for _ in range(n_workers):
            raw.append(make_adapter(db, dsn, **akw))
    except BaseException:
        for a in raw:
            try:
                a.close()
            except Exception:
                pass
        raise

    proxies = [
        InstrumentedAdapter(raw[w], w, plans[w], mode, clock) for w in range(n_workers)
    ]
    outcomes: List[Dict[str, Any]] = [{} for _ in range(n_workers)]
    error_cats: List[str] = []
    elle = elle_logger
    barrier = threading.Barrier(n_workers, action=_release, timeout=10)

    def worker(idx: int) -> None:
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass
        if stagger_ms > 0 and idx > 0:
            time.sleep(idx * stagger_ms / 1000.0)
        if elle:
            elle.log_invoke(process=idx, f="txn", value=None)
        if mode is InjectionMode.START_SKEW:
            skew = plans[idx].start_skew_ms()
            if skew > 0:
                time.sleep(skew / 1000.0)
        try:
            result = fn(proxies[idx], idx)
            outcomes[idx] = result
            ec = result.get("error", "OK")
            if ec != "OK":
                error_cats.append(ec)
                if elle:
                    elle.log_fail(process=idx, f="txn", value=result, error=ec)
            elif elle:
                elle.log_ok(process=idx, f="txn", value=result)
        except Exception as e:  # workload code raised: record, don't crash trial
            outcomes[idx] = {"outcome": "exception", "error": str(e), "worker": f"w{idx}"}
            error_cats.append("OTHER")
            if elle:
                elle.log_fail(process=idx, f="txn", value=None, error=str(e))

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(n_workers)]
    timed_out = False
    try:
        for t in threads:
            t.start()
        # One shared deadline (the old code waited up to 30 s *per* thread).
        deadline = time.monotonic() + worker_timeout_s
        for t in threads:
            t.join(max(0.0, deadline - time.monotonic()))
        stuck = [i for i, t in enumerate(threads) if t.is_alive()]
        if stuck:
            timed_out = True
            for i in stuck:
                interrupt_adapter(raw[i])
            for i in stuck:
                threads[i].join(5.0)
            for i in stuck:
                outcomes[i] = {"outcome": "timeout", "error": "TIMEOUT", "worker": f"w{i}"}
                error_cats.append("TIMEOUT")
            still = [i for i in stuck if threads[i].is_alive()]
            if still:
                raise HarnessError(
                    f"worker(s) {still} still running {worker_timeout_s:.0f}s after "
                    f"start and an interrupt; the database may be holding locks"
                )

        # 4. Canonical end state (strict: a failed read is a harness bug).
        read_adapter = make_adapter(db, dsn, **akw)
        try:
            reader = StrictAdapter(read_adapter, getattr(workload, "name", "")) \
                if strict_canonical else read_adapter
            if len(inspect.signature(workload.canonical).parameters) > 1:
                state = workload.canonical(reader, outcomes)
            else:
                state = workload.canonical(reader)
        finally:
            read_adapter.close()
    finally:
        for a in raw:
            try:
                a.close()
            except Exception:
                pass

    # 5. Verdict.
    bug_found, bug_reason, category = _verdict(workload, outcomes, state)
    trace: List[TraceEvent] = sorted(
        (ev for p in proxies for ev in p.trace), key=lambda e: (e.start_ms, e.worker))

    return TrialResult(
        outcomes=outcomes,
        canonical_state=state,
        canonical_hash=workload.canonical_hash(state),
        bug_found=bug_found,
        bug_reason=bug_reason,
        elapsed_ms=(time.monotonic() - t_trial) * 1000.0,
        error_categories=error_cats,
        bug_category=category,
        trace=trace,
        timed_out=timed_out,
    )
