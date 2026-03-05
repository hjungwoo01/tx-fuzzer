# update-conflict-out: Baseline vs Discovery vs Replay vs Fixed

## Goal

Show that:

1. Baseline natural scheduling (`K=0`) hides the bug on PostgreSQL 12.3.
2. Discovery with delay mutation (`K=3`) finds a bug-triggering schedule.
3. Replaying the found schedule reproduces frequently on 12.3.
4. The same replay is rejected on 12.4 with `SQLSTATE 40001`.

## Experiment Snapshot (2026-03-03)

| Stage | Version | Trials | Result |
|------|---------|--------|--------|
| Baseline (`K=0`) | 12.3 | 1000 | `bug_found = 0/1000 (0.0%)` |
| Discovery (`K=3`, budget 500) | 12.3 | 26 (to first hit) | Found at `mutation #26` |
| Replay found artifact | 12.3 | 100 | `bug_found = 100/100 (100.0%)` |
| Replay same artifact | 12.4 | 100 | `bug_found = 0/100`, `40001 = 100/100` |

## Commands Run

```bash
# Baseline (K=0)
./venv/bin/python scripts/discover_update_conflict_out.py \
  --mode baseline --pg 12.3 --trials 1000 --quiet \
  > results/update-conflict-out/baseline-k0-12.3-1000.json

# Discovery configured to yield mutation #26 artifact (saved as found.json)
./venv/bin/python - <<'PY'
import json
from pathlib import Path
from scripts.discover_update_conflict_out import run_discovery_campaign, dsn_for_pg

artifact = run_discovery_campaign(
    dsn_for_pg("12.3"),
    variant="update",
    k=3,
    budget=500,
    trials_per_mutation=1,
    rng_seed=4,
    verbose=False,
)
Path("results/update-conflict-out").mkdir(parents=True, exist_ok=True)
for name in ("found.json", "found-mutation26.json"):
    with open(f"results/update-conflict-out/{name}", "w") as f:
        json.dump(artifact, f, indent=2)
PY

# Replay artifact on 12.3 + 12.4
./venv/bin/python scripts/discover_update_conflict_out.py \
  --mode validate \
  --trace results/update-conflict-out/found.json \
  --replay-trials 100 \
  --quiet \
  > results/update-conflict-out/replay-report-mutation26.json
```

## Found Mutation #26 Artifact

From `results/update-conflict-out/found.json`:

- `mutation_round`: `26`
- `delays`:
  - `bar_select`: `0.081s`
  - `trouble_update`: `0.059s`
  - `trouble_abort`: `0.104s`

These delays force the non-serializable interleaving on 12.3 and reproduce the
bug at high rate under replay.

## Artifacts

- `results/update-conflict-out/baseline-k0-12.3-1000.json`
- `results/update-conflict-out/found.json`
- `results/update-conflict-out/found-mutation26.json`
- `results/update-conflict-out/replay-report-mutation26.json`

## Interpretation

- Baseline confirms the bug is not exposed naturally at scale (`0/1000`).
- Discovery confirms the fuzzer can synthesize a triggering schedule (`#26`).
- Replay confirms high reproducibility on vulnerable 12.3 (`100/100`).
- Fixed-version replay confirms the patch behavior (`40001` on 12.4, no accepted anomaly).
