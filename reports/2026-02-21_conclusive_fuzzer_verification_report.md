# Conclusive Verification Report: Final-State Concurrency Fuzzer

Date: 2026-02-21  
Repository: `/home/jay/tx-fuzzer`

## 1. Executive Summary

This fuzzer is designed to map the concurrency state space of transactional SQL workloads (PostgreSQL primary target, with replay backend for MySQL) and discover race-condition outcomes such as lost updates and deadlock-like liveness failures.

Its core design is now aligned with the stated philosophy:

> Use final database state as the signal. Different interleavings produce different final write values, so final-state hashing is sufficient and avoids costly intermediate snapshot probes.

Conclusive status from this verification pass:

1. Distinct-write workload shaping is implemented in trace generation (`scripts/run_random.py`) via blind-overwrite rewriting with transaction-unique IDs.
2. Coverage oracle hashing is final-state-only (or error-signature-only on failures) in `scripts/run_campaign.py`.
3. Baseline mode is implemented as `--baseline` and runs without fuzzer-injected mutation.
4. Smart mutation logic targets conflict windows (especially read-then-write on same key), not purely random delay positions.

## 2. Part 1: Code Verification of the Four Pillars

### Pillar 1: Workload Generator Uses Distinct Writes (Blind Overwrite IDs)

Verified in `scripts/run_random.py`:

- Transaction-unique write IDs: `_derive_txn_write_id(...)` in `scripts/run_random.py:70`.
- `UPDATE` rewrite to blind overwrite:
  - key extraction in `scripts/run_random.py:110`
  - rewrite in `scripts/run_random.py:173`
  - emitted SQL form: `UPDATE kv SET v=$1 WHERE k=$2` (or `WHERE k IN (...)`) in `scripts/run_random.py:188`.
- `INSERT` rewrite to blind overwrite:
  - insert key extraction in `scripts/run_random.py:145`
  - rewrite in `scripts/run_random.py:195`
  - emitted SQL form: `UPDATE kv SET v=$1 WHERE k=$2` in `scripts/run_random.py:208`.
- Rewrite application inside trace conversion at `scripts/run_random.py:236` and `scripts/run_random.py:244`.

Verification snippet result (executed during this audit):

```text
{'client_id': 0, 'sql': 'UPDATE kv SET v=$1 WHERE k=$2', 'args': [281474976710665, 5], 'type': 'invoke'}
{'client_id': 1, 'sql': 'UPDATE kv SET v=$1 WHERE k=$2', 'args': [562949953421321, 7], 'type': 'invoke'}
```

Interpretation: both commutative-style `UPDATE` and `INSERT` events are transformed into blind overwrites with transaction-unique values.

### Pillar 2: Oracle Hashes Final State Only

Verified in `scripts/run_campaign.py`:

- Final-state canonicalization:
  - row normalization `scripts/run_campaign.py:541`
  - deterministic sorting `scripts/run_campaign.py:556`.
- Hash function `output_hash(...)` at `scripts/run_campaign.py:567`:
  - success payload: only `{"final_state": ...}` at `scripts/run_campaign.py:574`
  - failure payload: only `{"error_signature": ...}` at `scripts/run_campaign.py:576`.

`execution_history`, timestamps, per-step details, and intermediate observations are not part of the hash payload.

Verification snippet result:

```text
a c1ef524dc09eacf4
b c1ef524dc09eacf4
c aaaaa43acf726b10
same_a_b True
diff_a_c True
```

Interpretation: different histories with identical final state hash identically; changed final state changes hash.

### Pillar 3: Baseline Mode Exists and Disables Fuzzer Mutation

Verified in `scripts/run_campaign.py`:

- CLI flag exists:
  - `--baseline` (alias `--baseline-no-mutate`) in `scripts/run_campaign.py:1039`.
- Baseline trace preparation removes delay marks:
  - `_prepare_baseline_trace(...)` at `scripts/run_campaign.py:682`.
- Baseline loop does not call mutation path:
  - baseline branch in `scripts/run_campaign.py:939` executes `_execute(current_trace)` directly.
- Mutation path is only used in non-baseline branch:
  - `mutated = self._mutate_trace(parent)` in `scripts/run_campaign.py:968`.

Observed corpus evidence in this repository:

```text
baseline_corpus state_files 181 with_delay 0
corpus state_files 14039 with_delay 14039
```

Interpretation: baseline records contain no injected `delay` fields; fuzzer records contain mutation markers.

### Pillar 4: Smart Mutations Target Concurrency-Critical Windows

Verified in `scripts/run_campaign.py`:

- Key-aware extraction for SQL events: `_extract_event_keys(...)` at `scripts/run_campaign.py:122`.
- Smart scheduler mutation:
  - `mutate_timing_smart(...)` at `scripts/run_campaign.py:220`
  - tracks recent reads by key and client
  - prioritizes delay targets:
    1. same-client read-then-write same key (`rmw_same_client`)
    2. any read-then-write same key across clients
    3. writes in general
    4. replayable fallback.
- Primary mutation entrypoint uses smart delay with high probability:
  - `_mutate_trace(...)` at `scripts/run_campaign.py:693`
  - smart delay attempt at `scripts/run_campaign.py:705`
  - optional reorder perturbation at `scripts/run_campaign.py:708`.

Verification sample (200 seeds on synthetic read/write trace):

```text
write_hits 189 read_hits 11
```

Interpretation: delay placement is strongly biased toward writes and RMW windows rather than uniform randomness.

## 3. Workload & State Generation

### Why Blind Overwrites with Unique IDs

Using commutative updates like `v = v + 1` collapses many interleavings into indistinguishable final outcomes. That weakens state-space observability.

The current implementation instead encodes each transaction’s write identity directly into `v`:

- each transaction gets a unique write token (`_derive_txn_write_id`) in `scripts/run_random.py:70`
- writes are normalized to blind-overwrite SQL forms in `scripts/run_random.py:188` and `scripts/run_random.py:208`.

Result: if two contenders race on the same key, final `v` directly records winner identity.

### Why Final-State Hashing (and Not Intermediate Snapshotting)

Intermediate probes inside replay loops add extra queries and synchronization overhead, which perturbs scheduling and can hide or alter real races (probe effect).

This implementation avoids using intermediate observations as oracle signal:

- replay still emits detailed `execution_history` for forensics in `scripts/replay_litmus.py:320`
- oracle only consumes canonical final state for coverage in `scripts/run_campaign.py:567`.

This preserves performance and keeps the state novelty signal tied to externally visible end-state outcomes.

## 4. Smart Mutation Strategy (Code-Level Behavior)

The mutation strategy in `scripts/run_campaign.py` is conflict-aware:

1. Parse each SQL event and infer touched keys (`_extract_event_keys`, `scripts/run_campaign.py:122`).
2. Track where reads happened per key and per client (`last_read_any`, `last_read_by_client` in `scripts/run_campaign.py:235`).
3. Prefer injecting delay before:
   - same-client write after read on same key (classic RMW hazard),
   - then any write overlapping a previously read key,
   - then writes generally.
4. Keep adjacent cross-client reorder as secondary perturbation (`mutate_reorder`, `scripts/run_campaign.py:291`).

Compared to random delay injection, this concentrates mutations around known serialization/lost-update windows.

## 5. Trace Visibility and Forensics

Each discovered unique state is persisted as a full JSON record:

- file path pattern: `.../run_<timestamp>/state_<hash>.json`
- record includes:
  - `mutated_events` (the exact replay trace)
  - per-event `delay` marker (if injected)
  - `iteration`, `hash`, `outcome_type`, `output`, `error_signature`.

Code path: record writing in `scripts/run_campaign.py:790` and `scripts/run_campaign.py:810`.

Example inspection commands:

```bash
# 1) open a state file
python - <<'PY'
import json
from pathlib import Path
p = Path("corpus/fuzz/postgres/run_2026-02-19T13-38-31Z/state_0009b13b4f2782b1.json")
rec = json.loads(p.read_text())
print("iteration:", rec["iteration"], "hash:", rec["hash"], "outcome:", rec["outcome_type"])
delay_events = [e for e in rec["mutated_events"] if isinstance(e, dict) and "delay" in e]
print("delay events:", len(delay_events))
print("first delay event:", delay_events[0] if delay_events else None)
PY
```

```bash
# 2) quickly inspect the indexed metadata for a run
head -n 5 corpus/fuzz/postgres/run_2026-02-19T13-38-31Z/states_index.jsonl
```

## 6. Evaluation Methodology: Baseline vs Smart Fuzzer

Use identical DB/isolation/workload settings for both runs. Change only mutation policy.

### Recommended experiment setup

```bash
cd /home/jay/tx-fuzzer
source venv/bin/activate
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable'
```

### A) Baseline run (no fuzzer mutation)

```bash
python scripts/run_campaign.py \
  --baseline \
  --target postgres \
  --isolation "READ COMMITTED" \
  --config workloads/fuzz.yaml \
  --iterations 5000 \
  --corpus corpus_baseline
```

### B) Smart fuzzer run

```bash
python scripts/run_campaign.py \
  --target postgres \
  --isolation "READ COMMITTED" \
  --config workloads/fuzz.yaml \
  --iterations 5000 \
  --corpus corpus_fuzzer
```

This yields two comparable corpora (`corpus_baseline/` and `corpus_fuzzer/`) for visualization.

## 7. Data Visualization: Cumulative Unique States vs Iteration

A plotting utility has been added:

- `scripts/plot_unique_states.py`

It reads campaign state files, resolves the latest `run_*` under each root, and generates a step-style cumulative curve.

### Run it

```bash
pip install pandas matplotlib

python scripts/plot_unique_states.py \
  --baseline-root corpus_baseline \
  --fuzzer-root corpus_fuzzer \
  --output reports/unique_states_curve.png
```

Fallback behavior:

- if `corpus_baseline/` does not exist, script tries `baseline_corpus/`
- if `corpus_fuzzer/` does not exist, script tries `corpus/`

### Expected graph shape

- Baseline: quick early growth from natural scheduler nondeterminism, then earlier plateau.
- Smart fuzzer: steeper initial slope and higher terminal unique-state count, due to targeted conflict-window perturbation.

## 8. Final Assessment

Against the requested 4 pillars, the code is consistent with the architecture:

1. Distinct-write blind-overwrite trace shaping: implemented.
2. Final-state-only coverage oracle: implemented.
3. Baseline no-mutation mode via `--baseline`: implemented.
4. Conflict-aware smart mutation: implemented.

This provides a clean, reproducible Baseline-vs-Smart evaluation framework centered on final-state uniqueness as the core signal.

## 9. Strict 2h Comparison (Completed Runs)

The strict comparison from completed 2-hour runs is documented in:

- `reports/2026-02-21_baseline_vs_smart_strict_comparison.md`

Associated artifacts:

- Plot: `reports/figures/2026-02-21_baseline_vs_smart_cumulative_unique_states.png`
- Machine-readable metrics: `reports/2026-02-21_baseline_vs_smart_summary.json`

Note:

- That comparison used a pre-fix smart run that plateaued at 1 state.
- Smart mutation/parent selection has since been patched in `scripts/run_campaign.py` for proper exploration; rerun smart for a fair post-fix head-to-head.

## 10. Harness Correction

A critical harness flaw was identified and corrected:

1. `scripts/replay_litmus.py` now replays with true concurrency (thread-per-client + start barrier) instead of a single sequential loop.
2. `scripts/run_campaign.py --baseline` now enforces the same trace-volume cap as smart mode (200 replayable events).
3. Smart parent selection/mutation strategy was corrected to avoid stagnation on a single discovered parent lineage.

Post-fix smoke checks are documented in:

- `reports/2026-02-21_baseline_vs_smart_strict_comparison.md`
