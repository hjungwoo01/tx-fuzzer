"""Quick difficulty comparison: original vs hardened trial functions."""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import time
import random as rng
from scripts.experiments.run_final_eval import (
    _mysql_rr_trial_with_delays, _mysql_rr_trial_with_delays_hard,
    MYSQL_RR_EVENTS, MYSQL_RR_EVENTS_HARD,
    _pg_uco_trial_with_delays, _pg_uco_trial_with_delays_hard,
    PG_UCO_EVENTS, PG_UCO_EVENTS_HARD,
    _sqlite_dirty_trial_with_delays, _sqlite_dirty_trial_with_delays_hard,
    SQLITE_DIRTY_EVENTS, SQLITE_DIRTY_EVENTS_HARD,
    _uco_setup_schema,
)
from scripts.experiments.case_studies import MYSQL_RR_NONEXISTENT_SCHEMA
from scripts.experiments.db_adapter import make_adapter

MYSQL_DSN = "mysql://root:root@localhost:33080/test"
PG_DSN = "postgres://postgres:postgres@localhost:54323/postgres"
SQLITE_DSN = "file:difficulty_test_shared?mode=memory&cache=shared"
N = 50


def run_mysql(trial_fn, event_ids, label, delay_range=0.1):
    hits = 0
    errs = 0
    for _ in range(N):
        try:
            setup = make_adapter("mysql", MYSQL_DSN)
            setup.setup(MYSQL_RR_NONEXISTENT_SCHEMA)
            setup.close()
            if delay_range == 0:
                delays = {}
            else:
                delays = {e: rng.uniform(0, delay_range) for e in event_ids}
            r = trial_fn(MYSQL_DSN, delays)
            if r.get("bug_found"):
                hits += 1
        except Exception:
            errs += 1
    print(f"  {label}: {hits}/{N} bugs ({hits/N*100:.0f}%) [{errs} errors]")
    return hits


def run_pg(trial_fn, event_ids, label, delay_range=0.1):
    hits = 0
    errs = 0
    for _ in range(N):
        try:
            _uco_setup_schema(PG_DSN)
            if delay_range == 0:
                delays = {}
            else:
                delays = {e: rng.uniform(0, delay_range) for e in event_ids}
            r = trial_fn(PG_DSN, delays)
            if r.get("bug_found"):
                hits += 1
        except Exception:
            errs += 1
    print(f"  {label}: {hits}/{N} bugs ({hits/N*100:.0f}%) [{errs} errors]")
    return hits


def run_sqlite(trial_fn, event_ids, label, delay_range=0.1):
    hits = 0
    errs = 0
    # Setup once: create the kv table
    setup = make_adapter("sqlite", SQLITE_DSN,
                          sqlite_shared_cache=True,
                          sqlite_read_uncommitted=True)
    setup.setup(["CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v INTEGER)",
                 "INSERT OR REPLACE INTO kv VALUES ('x', 0)"])
    setup.close()
    for _ in range(N):
        try:
            if delay_range == 0:
                delays = {}
            else:
                delays = {e: rng.uniform(0, delay_range) for e in event_ids}
            r = trial_fn(SQLITE_DSN, delays, shared_cache=True, read_uncommitted=True)
            if r.get("bug_found"):
                hits += 1
        except Exception:
            errs += 1
    print(f"  {label}: {hits}/{N} bugs ({hits/N*100:.0f}%) [{errs} errors]")
    return hits


if __name__ == "__main__":
    print(f"=== Difficulty Test ({N} trials each) ===\n")


    print("MySQL RR Phantom - STRESS (zero delays):")
    t0 = time.time()
    run_mysql(_mysql_rr_trial_with_delays, MYSQL_RR_EVENTS, "ORIGINAL ", delay_range=0)
    t1 = time.time()
    print(f"    ({t1-t0:.1f}s)")
    run_mysql(_mysql_rr_trial_with_delays_hard, MYSQL_RR_EVENTS_HARD, "HARDENED ", delay_range=0)
    t2 = time.time()
    print(f"    ({t2-t1:.1f}s)")

    print("\nMySQL RR Phantom - RANDOM (0-100ms delays):")
    t0 = time.time()
    run_mysql(_mysql_rr_trial_with_delays, MYSQL_RR_EVENTS, "ORIGINAL ", delay_range=0.1)
    t1 = time.time()
    print(f"    ({t1-t0:.1f}s)")
    run_mysql(_mysql_rr_trial_with_delays_hard, MYSQL_RR_EVENTS_HARD, "HARDENED ", delay_range=0.1)
    t2 = time.time()
    print(f"    ({t2-t1:.1f}s)")


    print("\nPG UCO - STRESS (zero delays):")
    t0 = time.time()
    run_pg(_pg_uco_trial_with_delays, PG_UCO_EVENTS, "ORIGINAL ", delay_range=0)
    t1 = time.time()
    print(f"    ({t1-t0:.1f}s)")
    run_pg(_pg_uco_trial_with_delays_hard, PG_UCO_EVENTS_HARD, "HARDENED ", delay_range=0)
    t2 = time.time()
    print(f"    ({t2-t1:.1f}s)")

    print("\nPG UCO - RANDOM (0-100ms delays):")
    t0 = time.time()
    run_pg(_pg_uco_trial_with_delays, PG_UCO_EVENTS, "ORIGINAL ", delay_range=0.1)
    t1 = time.time()
    print(f"    ({t1-t0:.1f}s)")
    run_pg(_pg_uco_trial_with_delays_hard, PG_UCO_EVENTS_HARD, "HARDENED ", delay_range=0.1)
    t2 = time.time()
    print(f"    ({t2-t1:.1f}s)")


    print("\nSQLite Dirty Read - STRESS (zero delays):")
    t0 = time.time()
    run_sqlite(_sqlite_dirty_trial_with_delays, SQLITE_DIRTY_EVENTS, "ORIGINAL ", delay_range=0)
    t1 = time.time()
    print(f"    ({t1-t0:.1f}s)")
    run_sqlite(_sqlite_dirty_trial_with_delays_hard, SQLITE_DIRTY_EVENTS_HARD, "HARDENED ", delay_range=0)
    t2 = time.time()
    print(f"    ({t2-t1:.1f}s)")

    print("\nSQLite Dirty Read - RANDOM (0-100ms delays):")
    t0 = time.time()
    run_sqlite(_sqlite_dirty_trial_with_delays, SQLITE_DIRTY_EVENTS, "ORIGINAL ", delay_range=0.1)
    t1 = time.time()
    print(f"    ({t1-t0:.1f}s)")
    run_sqlite(_sqlite_dirty_trial_with_delays_hard, SQLITE_DIRTY_EVENTS_HARD, "HARDENED ", delay_range=0.1)
    t2 = time.time()
    print(f"    ({t2-t1:.1f}s)")

    print("\nDone.")
