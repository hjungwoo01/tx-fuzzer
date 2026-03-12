"""Quick test: PG SHARE-lock distractors - original vs hardened, random delays."""
from __future__ import annotations
import sys, random, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.experiments.run_final_eval import (
    _pg_uco_trial_with_delays, _pg_uco_trial_with_delays_hard,
    PG_UCO_EVENTS, PG_UCO_EVENTS_HARD,
    _uco_setup_schema,
)

DSN = "postgres://postgres:postgres@localhost:54323/postgres"
N = 100

rng = random.Random(42)

print(f"=== PG UCO SHARE-lock distractor test ({N} trials) ===", flush=True)

# Original (no distractors) with random 0-100ms
t0 = time.time()
bugs_o = 0
for i in range(N):
    _uco_setup_schema(DSN)
    delays = {e: rng.uniform(0, 0.1) for e in PG_UCO_EVENTS}
    r = _pg_uco_trial_with_delays(DSN, delays)
    if r.get("bug_found"):
        bugs_o += 1
        if bugs_o == 1:
            print(f"  ORIG first bug at trial {i+1}", flush=True)
t1 = time.time()
print(f"PG ORIGINAL + random: {bugs_o}/{N} ({bugs_o/N*100:.1f}%)  [{t1-t0:.1f}s]", flush=True)

# Hardened (SHARE lock distractors) with random 0-100ms
bugs_h = 0
for i in range(N):
    delays = {e: rng.uniform(0, 0.1) for e in PG_UCO_EVENTS_HARD}
    r = _pg_uco_trial_with_delays_hard(DSN, delays)
    if r.get("bug_found"):
        bugs_h += 1
        if bugs_h == 1:
            print(f"  HARD first bug at trial {i+1}", flush=True)
t2 = time.time()
print(f"PG HARDENED + random: {bugs_h}/{N} ({bugs_h/N*100:.1f}%)  [{t2-t1:.1f}s]", flush=True)

print(f"Drop: {bugs_o/N*100:.1f}% -> {bugs_h/N*100:.1f}%", flush=True)
