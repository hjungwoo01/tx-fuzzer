# Strict Comparison: 2h Baseline vs 2h Smart Fuzzer

Date: 2026-02-21  
Scope: completed runs from:

- Baseline: `corpus_baseline/fuzz/postgres/run_2026-02-21T01-06-22Z`
- Smart (pre-fix run): `corpus_fuzzer/fuzz/postgres/run_2026-02-21T03-07-44Z`

Source summary artifact: `reports/2026-02-21_baseline_vs_smart_summary.json`  
Plot artifact: `reports/figures/2026-02-21_baseline_vs_smart_cumulative_unique_states.png`

## Strict Result Table

| Metric | Baseline (2h) | Smart Fuzzer (2h, pre-fix) |
|---|---:|---:|
| Unique states (`state_*.json`) | 67 | 1 |
| First discovery iteration | 1 | 1 |
| Last discovery iteration | 74 | 1 |
| Deadlock states | 64 | 0 |
| Success states | 3 | 1 |
| Distinct error signatures | 64 | 0 |
| Avg execution time per discovered state (s) | 10.047 | 0.543 |
| Discovery span (minutes, first to last discovery timestamp) | 116.36 | 0.00 |

Interpretation:

- This strict 2h table is a factual record of the completed runs.
- It is **not** a fair final smart-fuzzer evaluation because the smart run used the pre-fix harness/mutation path.

## Cumulative Curve

Plot: `reports/figures/2026-02-21_baseline_vs_smart_cumulative_unique_states.png`

Observed shape from completed runs:

- Baseline curve rises to 67 by iteration 74.
- Smart (pre-fix) stays flat at 1 from iteration 1 onward.

## Why Smart Underperformed in This Run

The pre-fix smart run stagnated because it effectively kept exploiting the first discovered parent trace while stall reseeding did not sufficiently drive exploration of fresh seeds.

Additional practical factor:

- Replay in `scripts/replay_litmus.py` executes events in a single sequential loop, so delay-only mutations have limited effect unless ordering changes also produce key conflicts.

## Smart Fuzzer Fixes Applied

The smart mutation engine has now been updated in `scripts/run_campaign.py`:

1. Added conflict-aware reorder mutation:
   - `mutate_reorder_smart(...)` at `scripts/run_campaign.py:306`
   - prioritizes same-key cross-client conflicts and stronger non-adjacent schedule perturbation.
2. Changed mutation weighting so reorder is primary:
   - `_mutate_trace(...)` at `scripts/run_campaign.py:750`
3. Fixed parent-selection exploration bug:
   - `_select_parent(...)` at `scripts/run_campaign.py:779`
   - smart mode now intentionally explores fresh base traces at configurable rate.
4. Added CLI control:
   - `--explore-parent-rate` at `scripts/run_campaign.py:1108`
   - applied to runner at `scripts/run_campaign.py:1136`.

## Post-Fix Smoke Validation

After applying the fixes, short smoke runs confirm expected behavior:

1. Baseline cap check (`--baseline --iterations 3`)
   - run: `corpus_baseline_capcheck/fuzz/postgres/run_2026-02-21T08-14-47Z`
   - states: 3
   - all `mutated_events` lengths: 200
2. Smart cap check (`--iterations 20 --explore-parent-rate 0.6`)
   - run: `corpus_fuzzer_capcheck/fuzz/postgres/run_2026-02-21T08-23-31Z`
   - states: 5
   - all `mutated_events` lengths: 200

This verifies:

- replay now supports true per-client concurrent execution (thread-per-client + barrier start),
- baseline and smart now run equal workload volume (200 events),
- smart no longer collapses to exactly one state in short validation.

## Re-Run Command (Patched Smart)

For a fair post-fix comparison, run smart again for 2 hours:

```bash
DATABASE_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
  /home/jay/tx-fuzzer/venv/bin/python scripts/run_campaign.py \
  --target postgres \
  --isolation "READ COMMITTED" \
  --config workloads/fuzz.yaml \
  --corpus corpus_fuzzer \
  --forever \
  --explore-parent-rate 0.60
```

Use `timeout --preserve-status 2h` around the command for the same fixed-duration protocol.
