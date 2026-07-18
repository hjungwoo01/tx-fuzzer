# Conference-Quality Evaluation Guide

This document describes the four evaluation extensions that bring the
IsoTest evaluation up to top-venue standards (OSDI / VLDB / EuroSys).
Each extension addresses a specific reviewer concern.

| # | Concern | Script | CSV output |
|---|---------|--------|-----------|
| 1 | "How does this compare to PCT, CHESS, Jepsen, naive sleep?" | `scripts/evaluate_baselines.py` | `results/baselines/baseline_*.csv` |
| 2 | "Only 3 case studies — what about the broader bug surface?" | `scripts/experiments/extended_workloads.py` | (registers 8 new workloads) |
| 3 | "No statistical significance reported" | `scripts/evaluate_significance.py` | `<input>/significance/significance_*.csv` |
| 4 | "Doesn't show how it scales" | `scripts/evaluate_scalability.py` | `results/scalability/scalability*.csv` |
| 5 | "Hand-coded oracles aren't validated" | `scripts/validate_oracle_soundness.py` | `results/oracle_soundness/oracle_*.csv` |

The all-in-one Make target is **`make eval-conference`** (Docker).

---

## 1. External-baseline comparison

The framework now ships five external baselines on the same client-side
delay-injection substrate:

| Baseline | Definition | Citation |
|----------|-----------|----------|
| `stress` | OS-only concurrency, no delays | (default control) |
| `naive` | Uniform fixed delay on every event | Common ad-hoc tester |
| `iid_bernoulli` | Per-event Bernoulli(p=0.3) delay | RAPOS family |
| `pct` | Probabilistic Concurrency Testing — depth-`d` priority points | Burckhardt et al., **ASPLOS '10** |
| `jepsen_random` | k-of-N event delays mimicking a Jepsen random nemesis | Kingsbury (Jepsen 2014) |

These exist alongside `random`, `heuristic`, `pool(random)`,
`pool(heuristic)` — all consuming the same `BaseStrategy` API. Adding a
new baseline is one new class in `scripts/schedule_pool/base_strategy.py`.

```bash
python scripts/evaluate_baselines.py \
    --db postgres mysql sqlite \
    --workloads bank_overdraft job_queue_claim list_append \
    --duration 600 --reps 5 \
    --output-dir results/baselines
```

Outputs:

* `baseline_timeseries.csv` — raw cumulative-coverage samples.
* `baseline_summary.csv` — per (db, workload, strategy) means.
* `baseline_comparison.csv` — head-to-head deltas: each IsoTest strategy
  vs each external baseline, with state/bug ratios.

---

## 2. Expanded bug corpus

`scripts/experiments/extended_workloads.py` registers **8 additional
historical-bug workloads** (total 13 across the framework). Each is
mapped to its Adya/Berenson bug class and a published reference.

| Workload | Bug class | Isolation level | Reference |
|----------|-----------|-----------------|-----------|
| `doctors_on_call` | G2-item write skew | SI / RC | Berenson '95, Adya '99 |
| `phantom_insert` | P3 phantom | RR | MySQL #73479, PG SI |
| `lost_update_counter` | P4 lost update | RC | Berenson '95 |
| `read_skew` | A5A read skew | RC | Adya '99, Jepsen YugabyteDB |
| `unique_violation` | TOCTOU predicate | RC | Devise/Rails uniqueness |
| `inventory_oversell` | TOCTOU lost-update | RC | Shopify/Stripe incidents |
| `fk_phantom` | Referential phantom | RC | PG #14796, MySQL #74132 |
| `long_fork` | G-SIb long fork | SI | Cerone '15, Jepsen YB |

The metadata is exposed as `extended_workloads.CORPUS_METADATA` so the
paper's Table 1 can be auto-generated.

---

## 3. Statistical significance

`scripts/stats_utils.py` provides pure-Python (no scipy):

* Bootstrap percentile CI (`bootstrap_ci`)
* Two-sided Mann–Whitney U with tie correction and normal approximation
* Welch's t-test with Welch–Satterthwaite df
* Cohen's *d* and Cliff's *δ* effect sizes

`scripts/evaluate_significance.py` consumes any CSV produced by the
longitudinal/baseline runners and emits per-pair tests:

```bash
python scripts/evaluate_significance.py \
    results/baselines/baseline_timeseries.csv \
    --metric states --output results/baselines/significance
```

Outputs:

* `significance_summary.csv` — mean, 95% bootstrap CI per cell.
* `significance_pairs.csv` — pairwise MWU + Welch + Cohen + Cliff's δ.
* `significance_report.txt` — human-readable summary you can paste into
  a paper draft (e.g. *"`heuristic` finds 2.3× more states than `stress`
  (MWU U=0, p=0.003, Cliff's δ=0.78 large)"*).

---

## 4. Scalability sweep

`scripts/evaluate_scalability.py` runs a Cartesian product of:

| Axis | Default values |
|------|----------------|
| `--workers` | 2, 4, 8, 16 |
| `--scales` | 1, 2, 4 (multiplies workload row counts) |
| `--strategies` | stress, random, heuristic, pct, pool(heuristic) |

Per cell the runner records: trials, unique states, bugs, time-to-first-
bug, throughput, p50/p95 trial latency. Plotting `unique_states` vs
`n_workers` (per strategy) gives the scalability curves the paper needs.

```bash
python scripts/evaluate_scalability.py \
    --db postgres --workloads multikey_transfer \
    --workers 2 4 8 16 --scales 1 2 4 \
    --strategies stress random heuristic pct 'pool(heuristic)' \
    --duration 300 --reps 3
```

---

## 5. Oracle soundness

`scripts/validate_oracle_soundness.py` cross-validates each hand-coded
`Oracle` against an *independent replay-style validator* that uses a
different derivation pathway (re-derives expected end-state from the
operation log; flags divergence as a bug).

The script produces a confusion matrix per workload (TP, TN, FP, FN
relative to the validator) and dumps an Elle-readable EDN history for
every disagreement so a Clojure Elle invocation or human reviewer can
adjudicate.

```bash
python scripts/validate_oracle_soundness.py \
    --db sqlite \
    --workloads bank_overdraft list_append job_queue_claim \
        lost_update_counter inventory_oversell unique_violation \
    --strategies random heuristic --n-trials 200
```

Outputs:

* `oracle_confusion.csv` — per (db, workload) TP/TN/FP/FN, agreement,
  oracle precision/recall.
* `oracle_disagreements.csv` — per-trial disagreements with reasons.
* `disagreements/*.edn` — full Jepsen/Elle-format histories.

---

## One-shot orchestrator

```bash
make eval-conference          # Docker; full matrix
# OR, native equivalents:
make baselines-native
make scalability-native
make significance-native
make oracle-soundness-native
```

All targets write under `results/<axis>/`.
