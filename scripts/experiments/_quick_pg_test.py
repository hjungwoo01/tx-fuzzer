#!/usr/bin/env python3
"""Quick sanity test: does the spread mode find PG bugs?"""
import sys, random, time
sys.path.insert(0, ".")
from scripts.experiments.tune_heuristic_vs_random import (
    run_pg_tunable_trial, HeuristicConfig, BASELINE_CONFIG,
)
from scripts.discover_update_conflict_out import setup_schema

dsn = "postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable"
setup_schema(dsn)

# Test spread mode (default baseline)
cfg = BASELINE_CONFIG
rng = random.Random(42)

bugs = 0
start = time.monotonic()
for i in range(20):
    bug, delays = run_pg_tunable_trial(dsn, "heuristic", rng, cfg, 0.0)
    if bug:
        bugs += 1
        if bugs == 1:
            print(f"  First bug at trial {i+1}, delays: {len(delays)} events")
            for eid, d in sorted(delays.items()):
                print(f"    {eid}: {d:.3f}s")

elapsed = time.monotonic() - start
print(f"Spread mode: {bugs}/20 bugs in {elapsed:.1f}s ({bugs/20:.0%})")

# Test random for comparison
rng2 = random.Random(42)
bugs2 = 0
start2 = time.monotonic()
for i in range(20):
    bug, delays = run_pg_tunable_trial(dsn, "random", rng2, cfg, 0.0)
    if bug:
        bugs2 += 1
        if bugs2 == 1:
            print(f"  First random bug at trial {i+1}")
elapsed2 = time.monotonic() - start2
print(f"Random mode: {bugs2}/20 bugs in {elapsed2:.1f}s ({bugs2/20:.0%})")
