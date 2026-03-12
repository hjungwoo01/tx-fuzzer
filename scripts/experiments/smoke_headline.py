#!/usr/bin/env python3
"""Quick smoke test for headline case studies."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.experiments.case_studies import (
    run_sqlite_narrow_dirty_case,
    replay_sqlite_narrow_dirty,
)

dsn = "file:smoketest?mode=memory&cache=shared"

print("=== SQLite Headline Smoke Test ===")
print()

# Buggy variant, all strategies
for strat in ["stress", "random", "heuristic"]:
    cr = run_sqlite_narrow_dirty_case(
        dsn=dsn, strategy=strat, duration_s=5, max_trials=50,
        delay_ms=10.0, rep=0, control=False,
        shared_cache=True, read_uncommitted=True,
    )
    print(f"  {strat:12s}: bug={cr.bug_found!s:5s}  ttf={cr.time_to_first_s}  trials={cr.total_trials}")

# Fixed variant (control)
cr = run_sqlite_narrow_dirty_case(
    dsn=dsn, strategy="heuristic", duration_s=5, max_trials=50,
    delay_ms=10.0, rep=0, control=True,
    shared_cache=True, read_uncommitted=False,
)
print(f"  {'fixed/heur':12s}: bug={cr.bug_found!s:5s}  trials={cr.total_trials}")

# Quick replay
print()
print("Replay buggy 5x...")
r = replay_sqlite_narrow_dirty(dsn, n_replays=5, delay_ms=10.0, shared_cache=True, read_uncommitted=True)
print(f"  buggy: {r['bug_count']}/{r['total']}")

print("Replay fixed 5x...")
r = replay_sqlite_narrow_dirty(dsn, n_replays=5, delay_ms=10.0, shared_cache=True, read_uncommitted=False)
print(f"  fixed: {r['bug_count']}/{r['total']}")

print()
print("DONE")
