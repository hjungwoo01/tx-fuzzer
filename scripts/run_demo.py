#!/usr/bin/env python3
"""
isotest demo: explore → save artifact → replay → verify FinalStateHash.

Usage:
  python scripts/run_demo.py explore [--db sqlite] [--workload bank_overdraft]
  python scripts/run_demo.py replay  examples/repros/bank_overdraft_sqlite.json
  python scripts/run_demo.py explore-replay [--db sqlite] [--workload bank_overdraft]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.schedule_pool import (
    Schedule,
    ScheduleArtifact,
    SchedulePool,
    PoolConfig,
    ScheduleMutator,
    MutationConfig,
    RetentionPolicy,
    RandomStrategy,
    ElleLogger,
    verify_replay,
    make_trial_event_ids,
)
from scripts.schedule_pool.types import FinalStateHash
from scripts.schedule_pool.event_id import get_workload_event_ids
from scripts.schedule_pool.runner import SchedulePoolRunner
from scripts.experiments.db_adapter import make_adapter
from scripts.experiments.micro_workloads import MICRO_WORKLOADS

import random


DEFAULT_DSNS = {
    "sqlite": "",  # filled dynamically
    "postgres": os.environ.get(
        "DATABASE_URL",
        "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
    ),
    "mysql": os.environ.get(
        "MYSQL_URL",
        "mysql://root:root@localhost:3307/test",
    ),
}


def cmd_explore(args: argparse.Namespace) -> list[ScheduleArtifact]:
    """Run a short exploration session and save any bug-triggering artifacts."""
    db = args.db
    workload_name = args.workload
    duration = args.duration
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if db == "sqlite":
        dsn = os.environ.get("SQLITE_DSN", "")
        if not dsn:
            dsn = tempfile.mktemp(suffix=".sqlite", prefix="isotest_demo_")
        DEFAULT_DSNS["sqlite"] = dsn
    dsn = DEFAULT_DSNS[db]

    wl = MICRO_WORKLOADS[workload_name]
    n_workers = 4
    seed = args.seed
    rng = random.Random(seed)

    all_events = get_workload_event_ids(workload_name, n_workers)
    pool = SchedulePool(config=PoolConfig())
    mutator = ScheduleMutator(all_events, config=MutationConfig(), rng=rng)
    retention = RetentionPolicy(pool)
    base_strategy = RandomStrategy(max_delay_ms=100.0)

    elle_path = output_dir / f"elle_history_{db}_{workload_name}.edn"
    elle = ElleLogger(elle_path)

    pool_runner = SchedulePoolRunner(
        workload=wl, db=db, dsn=dsn,
        n_workers=n_workers, variant="buggy",
        elle_logger=elle,
    )

    print(f"[isotest] Exploring {workload_name} on {db} for {duration}s (seed={seed})")
    print(f"[isotest] Elle history → {elle_path}")

    saved_artifacts: list[ScheduleArtifact] = []
    trials = 0
    bugs = 0
    unique_hashes: set[str] = set()
    start = time.monotonic()

    while (time.monotonic() - start) < duration:
        parent_record = pool.sample(rng, workload=workload_name, db=db)
        parent_schedule = (
            parent_record.schedule if parent_record
            else base_strategy.seed(all_events, rng)
        )
        parent_id = parent_record.schedule_id if parent_record else None

        candidate = mutator.mutate(parent_schedule)
        tr = pool_runner.run_trial_with_schedule(candidate)
        trials += 1
        unique_hashes.add(tr.canonical_hash)

        record = ScheduleArtifact(
            parent_schedule_id=parent_id,
            db=db,
            workload=workload_name,
            variant="buggy",
            strategy="schedule_pool",
            seed=seed,
            schedule=candidate,
            discovered_state_hash=tr.canonical_hash,
            bug_found=tr.bug_found,
            bug_reason=tr.bug_reason,
        )

        if tr.bug_found:
            bugs += 1
            # Measure replay rate
            replay_hits = 1
            for _ in range(5):
                rtr = pool_runner.run_trial_with_schedule(candidate)
                if rtr.bug_found:
                    replay_hits += 1
            record.replay_rate_buggy = replay_hits / 6

            # Save artifact
            artifact_path = output_dir / f"{workload_name}_{db}.json"
            record.save(artifact_path)
            saved_artifacts.append(record)
            print(f"  [BUG] {tr.bug_reason} | replay_rate={record.replay_rate_buggy:.0%} | saved → {artifact_path}")

        retention.accept(record)

    elle.close()

    print(f"\n[isotest] Exploration complete:")
    print(f"  trials={trials}, bugs={bugs}, unique_states={len(unique_hashes)}, pool_size={pool.size}")
    if saved_artifacts:
        print(f"  Saved {len(saved_artifacts)} artifact(s) to {output_dir}/")
    else:
        print(f"  No bugs found in {duration}s — try a longer duration or different workload")
    return saved_artifacts


def cmd_replay(args: argparse.Namespace) -> None:
    """Replay a saved ScheduleArtifact and verify FinalStateHash."""
    artifact_path = Path(args.artifact)
    if not artifact_path.exists():
        print(f"[isotest] ERROR: artifact not found: {artifact_path}")
        sys.exit(1)

    artifact = ScheduleArtifact.load(artifact_path)
    db = artifact.db
    n_replays = args.n_replays

    if db == "sqlite":
        dsn = os.environ.get("SQLITE_DSN", "")
        if not dsn:
            dsn = tempfile.mktemp(suffix=".sqlite", prefix="isotest_replay_")
    else:
        dsn = DEFAULT_DSNS.get(db, "")

    print(f"[isotest] Replaying artifact {artifact.artifact_id}")
    print(f"  workload={artifact.workload}, db={db}, variant={artifact.variant}")
    print(f"  delay_points={artifact.delay_count}, expected_hash={artifact.discovered_state_hash}")
    print(f"  n_replays={n_replays}")
    print()

    verification = verify_replay(
        artifact,
        db=db,
        dsn=dsn,
        n_replays=n_replays,
    )

    print(f"[isotest] Replay verification:")
    print(f"  hash_match_rate = {verification.hash_match_rate:.0%} ({verification.hash_match_count}/{verification.total_replays})")
    print(f"  bug_replay_rate = {verification.bug_replay_rate:.0%} ({verification.bug_replay_count}/{verification.total_replays})")
    print(f"  unique_hashes   = {verification.unique_hashes}")

    if verification.hash_match_rate == 1.0:
        print("\n  PASS: all replays matched the expected FinalStateHash")
    elif verification.hash_match_rate >= 0.8:
        print(f"\n  WARN: {verification.hash_match_rate:.0%} match rate — mostly deterministic")
    else:
        print(f"\n  LOW:  {verification.hash_match_rate:.0%} match rate — schedule may be timing-sensitive")


def cmd_explore_replay(args: argparse.Namespace) -> None:
    """Explore, then immediately replay the first artifact found."""
    artifacts = cmd_explore(args)
    if not artifacts:
        print("\n[isotest] No artifacts to replay — exploration found no bugs.")
        return

    artifact_path = Path(args.output_dir) / f"{args.workload}_{args.db}.json"
    if not artifact_path.exists():
        print(f"\n[isotest] Artifact not found at {artifact_path}")
        return

    print("\n" + "=" * 60)
    print("  REPLAY VERIFICATION")
    print("=" * 60 + "\n")

    # Re-use the replay args
    replay_args = argparse.Namespace(
        artifact=str(artifact_path),
        n_replays=args.n_replays,
    )
    cmd_replay(replay_args)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="isotest: client-side schedule exploration and replay",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # explore
    p_explore = sub.add_parser("explore", help="Run exploration session")
    p_explore.add_argument("--db", default="sqlite", choices=["sqlite", "postgres", "mysql"])
    p_explore.add_argument("--workload", default="bank_overdraft",
                           choices=list(MICRO_WORKLOADS.keys()))
    p_explore.add_argument("--duration", type=float, default=10.0)
    p_explore.add_argument("--seed", type=int, default=42)
    p_explore.add_argument("--output-dir", default="examples/repros")
    p_explore.add_argument("--n-replays", type=int, default=10)

    # replay
    p_replay = sub.add_parser("replay", help="Replay a saved artifact")
    p_replay.add_argument("artifact", help="Path to ScheduleArtifact JSON file")
    p_replay.add_argument("--n-replays", type=int, default=20)

    # explore-replay
    p_er = sub.add_parser("explore-replay", help="Explore then replay")
    p_er.add_argument("--db", default="sqlite", choices=["sqlite", "postgres", "mysql"])
    p_er.add_argument("--workload", default="bank_overdraft",
                      choices=list(MICRO_WORKLOADS.keys()))
    p_er.add_argument("--duration", type=float, default=10.0)
    p_er.add_argument("--seed", type=int, default=42)
    p_er.add_argument("--output-dir", default="examples/repros")
    p_er.add_argument("--n-replays", type=int, default=10)

    args = parser.parse_args()

    if args.command == "explore":
        cmd_explore(args)
    elif args.command == "replay":
        cmd_replay(args)
    elif args.command == "explore-replay":
        cmd_explore_replay(args)


if __name__ == "__main__":
    main()
