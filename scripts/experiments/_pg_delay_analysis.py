#!/usr/bin/env python3
"""Deeper PG analysis: what delay patterns find the bug?"""
import sys, random, time
sys.path.insert(0, ".")
from scripts.experiments.tune_heuristic_vs_random import (
    run_pg_tunable_trial, HeuristicConfig, BASELINE_CONFIG,
    _get_pg_trace_and_antideps, build_tunable_candidates,
    select_delays_weighted_spread,
)
from scripts.discover_update_conflict_out import (
    setup_schema, reset_tables, run_concurrent_trial,
)

dsn = "postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable"
setup_schema(dsn)

# Analysis: run many random trials and capture which delays led to bugs
rng = random.Random(42)
bugs = []
non_bugs = []

all_eids = [
    "foo_begin", "foo_select", "foo_insert", "foo_commit",
    "bar_begin", "bar_insert", "bar_select", "bar_commit",
    "trouble_begin", "trouble_update", "trouble_abort",
]

start = time.monotonic()
for i in range(200):
    delays = {}
    for eid in all_eids:
        delays[eid] = rng.uniform(0, 0.100)
    
    reset_tables(dsn)
    result = run_concurrent_trial(dsn, variant="update", delays=delays)
    
    if result.bug_found:
        bugs.append(delays.copy())
        print(f"  BUG at trial {i+1}")
    else:
        non_bugs.append(delays.copy())

elapsed = time.monotonic() - start
print(f"\n{len(bugs)}/{len(bugs)+len(non_bugs)} bugs found in {elapsed:.1f}s")

# Analyze which delays differentiate bugs from non-bugs
if bugs:
    print("\nMean delays when BUG found vs NOT found:")
    print(f"  {'Event':<20} {'Bug mean':>10} {'Non-bug mean':>12} {'Diff':>8}")
    for eid in all_eids:
        bug_vals = [d.get(eid, 0) for d in bugs]
        nonbug_vals = [d.get(eid, 0) for d in non_bugs[:len(bugs)*5]]  # sample
        bug_mean = sum(bug_vals) / len(bug_vals)
        nonbug_mean = sum(nonbug_vals) / len(nonbug_vals) if nonbug_vals else 0
        diff = bug_mean - nonbug_mean
        print(f"  {eid:<20} {bug_mean*1000:>8.1f}ms {nonbug_mean*1000:>10.1f}ms {diff*1000:>7.1f}ms")
else:
    print("No bugs found to analyze.")
