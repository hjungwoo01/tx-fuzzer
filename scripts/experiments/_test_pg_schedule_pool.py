"""Verify schedule_pool can find PG bug by pushing distractor delays high."""
from __future__ import annotations
import sys, random, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.experiments.run_final_eval import (
    _pg_uco_trial_with_delays_hard,
    PG_UCO_EVENTS, PG_UCO_EVENTS_HARD,
    N_DISTRACTORS,
)

DSN = "postgres://postgres:postgres@localhost:54323/postgres"
N = 200

# Simulate schedule_pool: distractor begin delays pushed to max (100ms),
# real event delays randomized
rng = random.Random(123)

print(f"=== PG HARDENED with distractors pushed late ({N} trials) ===", flush=True)

# Test: distractors start late (begin=100ms), real events random 0-100ms
bugs_late = 0
t0 = time.time()
for i in range(N):
    delays = {}
    # Real event delays: random 0-100ms
    for e in PG_UCO_EVENTS:
        delays[e] = rng.uniform(0, 0.1)
    # Distractor delays: begin=100ms (start after real sessions), hold=0, commit=0
    for j in range(N_DISTRACTORS):
        delays[f"distractor{j}_begin"] = 0.1    # distractors start late
        delays[f"distractor{j}_hold"] = 0.0
        delays[f"distractor{j}_commit"] = 0.0
    r = _pg_uco_trial_with_delays_hard(DSN, delays)
    if r.get("bug_found"):
        bugs_late += 1
        if bugs_late == 1:
            print(f"  first bug at trial {i+1}", flush=True)
t1 = time.time()
print(f"Distractors late (begin=100ms): {bugs_late}/{N} ({bugs_late/N*100:.1f}%)  [{t1-t0:.1f}s]", flush=True)

# Also test: distractors begin=50ms (moderate)
bugs_mid = 0
t0 = time.time()
for i in range(N):
    delays = {}
    for e in PG_UCO_EVENTS:
        delays[e] = rng.uniform(0, 0.1)
    for j in range(N_DISTRACTORS):
        delays[f"distractor{j}_begin"] = 0.05
        delays[f"distractor{j}_hold"] = 0.0
        delays[f"distractor{j}_commit"] = 0.0
    r = _pg_uco_trial_with_delays_hard(DSN, delays)
    if r.get("bug_found"):
        bugs_mid += 1
        if bugs_mid == 1:
            print(f"  first bug at trial {i+1}", flush=True)
t1 = time.time()
print(f"Distractors mid (begin=50ms): {bugs_mid}/{N} ({bugs_mid/N*100:.1f}%)  [{t1-t0:.1f}s]", flush=True)
