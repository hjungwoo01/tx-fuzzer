"""
Structured replay verification for ScheduleArtifacts.

Replays an exact schedule N times against a fresh database and reports
hash-match rate and bug-replay rate as first-class metrics.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .types import ReplayResult, ReplayVerification
from .schedule import Schedule, ScheduleArtifact

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.micro_workloads import MICRO_WORKLOADS, MicroWorkload

from .execution import execute_trial

#: Replays assume this many workers when an artifact does not record it
#: (format_version 1 files).  New artifacts always record n_workers.
LEGACY_DEFAULT_WORKERS = 4


def _run_single_replay(
    workload: MicroWorkload,
    db: str,
    dsn: str,
    schedule: Schedule,
    variant: str,
    n_workers: int,
    injection_mode: str = "start_skew",
    stagger_ms: float = 0.0,
    **adapter_kwargs: Any,
) -> Dict[str, Any]:
    """Execute one trial applying exact delays from a schedule.

    Returns dict with canonical_hash, bug_found, bug_reason, bug_category,
    elapsed_ms (and the TrialResult under "trial").
    """
    tr = execute_trial(
        workload,
        db=db,
        dsn=dsn,
        schedule=schedule,
        variant=variant,
        n_workers=n_workers,
        mode=injection_mode,
        stagger_ms=stagger_ms,
        adapter_kwargs=adapter_kwargs,
    )
    return {
        "canonical_hash": tr.canonical_hash,
        "bug_found": tr.bug_found,
        "bug_reason": tr.bug_reason,
        "bug_category": tr.bug_category,
        "elapsed_ms": tr.elapsed_ms,
        "trial": tr,
    }


def verify_replay(
    artifact: ScheduleArtifact,
    *,
    db: str,
    dsn: str,
    n_workers: Optional[int] = None,
    n_replays: int = 20,
    injection_mode: Optional[str] = None,
    variant: Optional[str] = None,
    **adapter_kwargs: Any,
) -> ReplayVerification:
    """Replay a ScheduleArtifact and verify FinalStateHash consistency.

    Executes the artifact's schedule n_replays times on a fresh database
    and returns a structured ReplayVerification summarizing match rate.

    Args:
        artifact: the ScheduleArtifact to replay
        db: database kind (postgres/mysql/sqlite)
        dsn: connection string
        n_workers: number of concurrent sessions (default: the artifact's
            recorded value, else LEGACY_DEFAULT_WORKERS)
        n_replays: number of replay executions
        injection_mode: override the artifact's recorded injection mode
        variant: override the artifact's variant (e.g. "fixed" to check that
            a fix holds under the exact schedule that broke the buggy code)
        **adapter_kwargs: passed to make_adapter

    Returns:
        ReplayVerification with per-replay results and aggregate stats.
    """
    wl = MICRO_WORKLOADS[artifact.workload]
    expected_hash = artifact.discovered_state_hash or ""
    workers = n_workers or artifact.n_workers or LEGACY_DEFAULT_WORKERS
    mode = injection_mode or artifact.injection_mode or "start_skew"
    run_variant = variant or artifact.variant

    replays: List[ReplayResult] = []
    hashes: List[str] = []
    bug_count = 0

    for i in range(n_replays):
        result = _run_single_replay(
            workload=wl,
            db=db,
            dsn=dsn,
            schedule=artifact.schedule,
            variant=run_variant,
            n_workers=workers,
            injection_mode=mode,
            stagger_ms=artifact.stagger_ms,
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


def assess_replay(verification: ReplayVerification, *, expect_bug: bool) -> Tuple[str, str]:
    """Turn a ReplayVerification into a (status, explanation) pair.

    status is one of:
      REPRODUCED      -- expect_bug and the oracle fired on every replay
      FLAKY           -- expect_bug and the oracle fired on some replays
      NOT_REPRODUCED  -- expect_bug and the oracle never fired
      CLEAN           -- not expect_bug and the oracle never fired
      BROKEN          -- not expect_bug but the oracle fired (e.g. a "fix"
                         that does not hold under this schedule)

    The FinalStateHash comparison is reported separately.  Artifacts that
    never recorded a hash (legacy, hand-written) are judged on end-state
    *determinism* (one distinct hash across replays) rather than printed as
    a misleading "0% match".
    """
    n = verification.total_replays
    k = verification.bug_replay_count
    if expect_bug:
        if n and k == n:
            status = "REPRODUCED"
        elif k:
            status = "FLAKY"
        else:
            status = "NOT_REPRODUCED"
        bug_part = f"bug reproduced in {k}/{n} replays"
    else:
        status = "CLEAN" if k == 0 else "BROKEN"
        bug_part = f"oracle fired in {k}/{n} replays"

    if verification.expected_hash:
        hash_part = (f"end-state hash matched in {verification.hash_match_count}/{n}"
                     f" ({verification.unique_hashes} distinct)")
    elif verification.unique_hashes == 1:
        hash_part = "no recorded hash; all replays reached one end state (deterministic)"
    else:
        hash_part = (f"no recorded hash; replays reached {verification.unique_hashes} "
                     f"distinct end states")
    return status, f"{bug_part}; {hash_part}"
