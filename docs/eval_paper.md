# Paper-quality evaluation — operator's manual

`make eval-paper` is the single command that produces every CSV the paper's
Evaluation section needs. This document explains exactly what it runs, how to
configure it, what it writes, and how to interpret the outputs.

## Quickstart

```bash
# 1) Sanity-check the harness, DSNs, and time budget BEFORE committing to a long run:
make eval-paper-check

# 2) Run the eval. Writes to results/paper/<UTC-timestamp>/.
make eval-paper

# 3) Print the env-var knobs and current defaults at any time:
make eval-paper-help
```

The preflight does four things:

1. Re-runs the python test suite — guards against accidental regressions in
   `scripts/schedule_pool/oracle.py` or the workload registry.
2. Imports every eval script with `extended_workloads` registered — guards
   against a stale `evaluate_baselines.py` that doesn't see the extended
   workloads.
3. Probes each requested DB with `SELECT 1` over the configured DSN.
4. Smoke-runs each of the four eval phases for 5 seconds, end-to-end, and
   prints the planned matrix plus a wall-time estimate.

Always run the preflight first. The wall-time estimate is honest: it
multiplies (cells × per-cell duration). If the estimate is too high, dial down
the matrix (workloads / strategies / reps) before launching.

## Phases

`make eval-paper` runs four phases sequentially. Each writes its own
sub-directory under `$EVAL_OUT`.

| # | Phase                  | Script                              | Output dir         |
|---|------------------------|-------------------------------------|--------------------|
| 1 | Baselines              | `evaluate_baselines.py`             | `baselines/`       |
| 2 | Scalability sweep      | `evaluate_scalability.py`           | `scalability/`     |
| 3 | Statistical significance | `evaluate_significance.py`        | `baselines/significance/` |
| 4 | Oracle soundness       | `validate_oracle_soundness.py`      | `oracle_soundness/`|

Phase 3 derives from phase 1's `baseline_timeseries.csv` — it reads the CSV
rather than running new trials, so its cost is negligible.

## Tunable knobs

All knobs are environment variables, with the defaults shown by
`make eval-paper-help`. Any subset can be overridden inline:

```bash
EVAL_DURATION_S=86400 EVAL_REPS=3 make eval-paper             # 24h per cell, n=3
EVAL_DBS="sqlite postgres" \
  POSTGRES_DSN="postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable" \
  make eval-paper
```

| Env var                   | Default                                                             | Notes |
|---------------------------|---------------------------------------------------------------------|-------|
| `EVAL_DBS`                | `sqlite`                                                            | Space-separated DB list. Each DB needs `<DB>_DSN` env var unless `DEFAULT_DSNS` resolves. |
| `EVAL_DURATION_S`         | `3600`                                                              | Per-cell seconds for **baselines**. Long enough that coverage curves approach asymptote on `list_append` / `multikey_transfer`. |
| `EVAL_SCALE_DURATION_S`   | `600`                                                               | Per-cell seconds for **scalability**. The scalability matrix has many more cells (workers × scales × strats × reps), so per-cell duration is shorter by default. |
| `EVAL_REPS`               | `3`                                                                 | Reps per cell. **Three long-running reps are preferred over many short ones** — short reps measure noise, not the asymptotic discovery rate. |
| `EVAL_N_WORKERS`          | `4`                                                                 | Concurrent workers per trial. |
| `EVAL_WORKLOADS_BASELINE` | `bank_overdraft inventory_oversell list_append multikey_transfer`   | Used for phases 1 and 3. |
| `EVAL_WORKLOADS_SCALE`    | `bank_overdraft multikey_transfer`                                  | Used for phase 2. `multikey_transfer` is the most state-rich workload and the most informative for scaling claims. |
| `EVAL_WORKLOADS_ORACLE`   | `bank_overdraft inventory_oversell unique_violation list_append job_queue_claim lost_update_counter` | Used for phase 4. |
| `EVAL_STRATEGIES`         | `stress pct heuristic 'pool(heuristic)'`                            | Quotes around `pool(heuristic)` are required (bash treats bare `()` as a subshell). |
| `EVAL_SCALE_WORKERS`      | `2 8`                                                               | Worker counts for the scalability sweep. Add `4` for finer resolution if budget allows. |
| `EVAL_SCALE_TABLE_SCALES` | `1 2`                                                               | Table-size multipliers (`multikey_transfer` is the workload that scales meaningfully here). |
| `EVAL_OUT`                | `results/paper/<UTC-timestamp>`                                      | Resulting directory; safe to override to a fixed name for resumability. |
| `PYTHON`                  | `.venv/bin/python`                                                   | Override if your venv lives elsewhere. |
| `POSTGRES_DSN` etc.       | unset                                                                | Required when the corresponding DB is in `EVAL_DBS`. |

The default matrix yields **~65h wall time** on a single sqlite host. To go
longer for convergence study, raise `EVAL_DURATION_S`; to go faster for a
smoke run, lower it.

## Output structure

```
results/paper/<timestamp>/
├── run.log                              # tee'd stdout from the entire run
├── baselines/
│   ├── baseline_summary.csv             # mean unique-states / bugs / trials per cell
│   ├── baseline_comparison.csv          # head-to-head ratios IsoTest vs each external baseline
│   ├── baseline_timeseries.csv          # cumulative metrics sampled every EVAL sample_interval
│   └── significance/
│       ├── significance_pairs.csv       # MWU U, p, Cliff's δ for every (strategy_a, strategy_b) pair
│       ├── significance_summary.csv     # per-strategy mean and 95% CI
│       └── significance_report.txt      # human-readable summary, one workload at a time
├── scalability/
│   ├── scalability.csv                  # mean states / TTFB / throughput per (workload, strategy, n_workers, scale)
│   └── scalability_raw.csv              # one row per rep
└── oracle_soundness/
    ├── oracle_confusion.csv             # TP/TN/FP/FN, agreement, precision, recall per workload
    ├── oracle_disagreements.csv         # one row per (oracle, validator) disagreement
    └── disagreements/                   # EDN history of each disagreeing trial
```

## How to read each CSV

### `baselines/baseline_summary.csv`

One row per `(db, workload, strategy)`. Columns:

| col                    | meaning |
|------------------------|---------|
| `states_mean / std / min / max` | unique end-state count over `EVAL_REPS` runs |
| `trials_mean`          | trials executed in `EVAL_DURATION_S` seconds (high for `stress`, lower for guided strategies because they pay for delay injection) |
| `bugs_mean`            | bug count from the **patched** oracle. Compare `pool(heuristic)` vs `pct` and vs `stress` per workload. |
| `duration_s`           | actual wall time per cell (≈ `EVAL_DURATION_S`) |

The headline number you want: on `list_append` (the only workload with a
non-trivial state lattice), `pool(heuristic).states_mean` should equal or
exceed `pct.states_mean` — that's the "matches PCT" claim.

On the other workloads (`bank_overdraft`, `inventory_oversell`), all
strategies saturate at 4 states. There the meaningful comparison is
`bugs_mean` and the diversity of `(state, bug-reason)` pairs.

### `baselines/baseline_comparison.csv`

One row per `(IsoTest strategy, external baseline)` pair, per workload.
Columns include `states_ratio` (e.g., `16.000` means IsoTest discovered 16×
more states than the baseline) and `states_delta` (signed integer difference).
This is the table that produces the "$16\times$ expansion vs `stress`"
quotable.

### `baselines/significance/significance_pairs.csv`

The full pairwise non-parametric test matrix. Columns of interest:

- `mwu_p` — Mann–Whitney U p-value (rank-based, distribution-free)
- `cliffs_delta` — non-parametric effect size in $[-1, +1]$
- `cliffs_magnitude` — `negligible / small / medium / large` (Romano 2006)
- `significant_at_0.05` — `yes`/`no`

For paper claims, quote $(U, p, \delta)$ tuples per workload, especially
`pool(heuristic)` vs `stress` (should be $\delta=1.000$, $p \leq 0.0339$ at
$n=3$).

**At $n=3$ vs $n=3$ the MWU p-value floor is 0.0253**: zero rank-overlap
gives $p = 1/\binom{6}{3} \cdot 2 = 0.05$ two-tailed (slightly tighter
asymptotic; the script uses the exact distribution). With longer per-cell
runs, individual cell variance shrinks but the rank-overlap structure stays
the same, so $p$-values won't drop further at $n=3$. The way to push $p$
lower is to raise $n$.

### `scalability/scalability.csv`

One row per `(db, workload, strategy, n_workers, table_scale)`.
Columns include:

- `states_mean` — unique end-states discovered. Watch this grow with
  `n_workers` for state-rich workloads (`multikey_transfer`).
- `ttfb_mean_s` — Time-to-First-Bug. **Empty cells mean no bug surfaced**,
  either because the workload variant is fixed or the budget was too short.
  If your `EVAL_WORKLOADS_SCALE` includes only buggy variants and TTFB is
  still empty, raise `EVAL_SCALE_DURATION_S` or check that the patched
  oracle is firing (see phase 4).
- `throughput_mean` — trials/s. Watch for *graceful degradation*: throughput
  should be flat or slightly increasing in `n_workers`, not collapsing.

### `oracle_soundness/oracle_confusion.csv`

One row per `(db, workload)`. Columns:

| col                | what to look for |
|--------------------|------------------|
| `tp / tn / fp / fn`| should have `fp = 0` and `fn = 0` (the patched oracles are sound and complete vs. the log-replay validator) |
| `agreement`        | should be `1.0000` everywhere |
| `precision_oracle` | NaN-protected; `0.0000` indicates "no positives raised in this run" (i.e., the buggy variant didn't fire — separate concern) |

If `agreement < 1.0`, **stop and investigate** — the log-replay validator
detected a violation the oracle missed (or vice versa), which means a
real-world false negative is plausible. Look at
`oracle_disagreements.csv` and the matching `disagreements/*.edn` file.

## Multi-DB setup

The eval supports postgres and mysql in addition to sqlite. The framework
reads `POSTGRES_DSN` / `MYSQL_DSN` / `SQLITE_DSN` from the environment;
defaults are in `scripts/experiments/db_adapter.py:DEFAULT_DSNS` (port 5432
postgres, port 3307 mysql). Override if your containers expose different
ports.

Example with the multi-DB compose file:

```bash
docker compose -f docker-compose-multidb.yml up -d
# Confirm port mapping:
docker ps --format '{{.Names}}\t{{.Ports}}' | grep '^txf_'

EVAL_DBS="sqlite postgres mysql" \
  POSTGRES_DSN="postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable" \
  MYSQL_DSN="mysql://root:root@localhost:33080/test" \
  make eval-paper
```

The preflight will skip any DB whose DSN is unreachable rather than failing
the entire run, but **the matrix multiplies linearly with DBs**: 3 DBs at
default settings is ~195h. Plan accordingly.

## Running multiple DBs in parallel

`make eval-paper` runs the DBs listed in `$EVAL_DBS` *sequentially* inside
one Python process (`for db in args.db`). To run all three DBs concurrently
on three CPU cores, use the parallel runner:

```bash
EVAL_DBS_PARALLEL="sqlite postgres mysql" \
  POSTGRES_DSN="postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable" \
  MYSQL_DSN="mysql://root:root@localhost:33080/test" \
  make eval-paper-parallel
```

This spawns one `make eval-paper` per DB, each with its own
`EVAL_OUT=<root>/<db>/`. Output ends up like:

```
results/paper/<timestamp>_parallel/
├── sqlite/                ← full per-phase output, identical structure to a normal eval-paper run
├── postgres/
├── mysql/
├── sqlite.log             ← tee'd stdout
├── postgres.log
├── mysql.log
└── sqlite.sqlite          ← auto-namespaced per-process SQLite file (only for sqlite)
```

### Why a dedicated runner exists (concurrency gotchas)

The single-process eval is internally sequential and concurrency-safe: each
phase loops `for db ... for workload ... for strategy ... for rep` and
writes its CSVs at end-of-run. Multi-DB inside one process is also safe —
the CSVs carry a `db` column.

**Across two processes**, three things will silently corrupt your run if
not handled:

1. **CSV clobber.** Both processes write
   `<output_dir>/baseline_summary.csv` etc. at end-of-run via plain
   `csv.writer(open(path, "w"))`. With the same `--output-dir`, the second
   process's `w` mode truncates the first's output. **The parallel runner
   gives each DB its own `EVAL_OUT` directory.**

2. **SQLite default DSN is a shared file.**
   `DEFAULT_DSNS["sqlite"] = "/tmp/tx_fuzzer_multidb.sqlite"`. Two parallel
   sqlite processes targeting the default would race on schema
   `DROP TABLE … CREATE TABLE` mid-trial. **The parallel runner sets a
   per-process `SQLITE_DSN` (`<EVAL_OUT_ROOT>/<db>.sqlite`)** when no
   `SQLITE_DSN` is already set in the environment. If you set
   `SQLITE_DSN` explicitly, the runner respects it — make sure that's
   what you want.

3. **EDN disagreement file collisions.** Phase 4 writes
   `<output_dir>/disagreements/trial_<db>_<workload>_<idx>.edn` for each
   oracle/validator disagreement. Within one process, `idx` is sequential;
   if two processes both write to the same `disagreements/` dir for the
   same `(db, workload)`, indices collide. The parallel runner avoids this
   by isolating output dirs.

For postgres/mysql in parallel, each DB targets a *different* DSN, so the
underlying server is not shared between processes (assuming you wired
distinct DSNs). If you point two parallel processes at the *same*
postgres/mysql instance, the schema-setup `DROP TABLE` from one will
clobber the other's mid-trial state — that is a DB-level race that no
amount of EVAL_OUT isolation fixes. Use distinct DSNs.

### Pre-run check

`make eval-paper-check` only checks one DB at a time (whatever's in
`$EVAL_DBS`). The parallel runner runs its own DSN-reachability probe for
each DB in `$EVAL_DBS_PARALLEL` *before* spawning any work, so a wrong
postgres port fails fast instead of after 24h.

## Long runs (24h+ per cell)

For convergence study you may want each cell to run for hours. The framework
is designed for this: nothing accumulates unbounded state, and Python's GC
will reclaim per-trial memory. Recommended pattern:

```bash
# 24h per baseline cell, 1h per scalability cell, n=3 reps:
EVAL_DURATION_S=86400 \
  EVAL_SCALE_DURATION_S=3600 \
  EVAL_REPS=3 \
  EVAL_OUT=results/paper/long_24h \
  make eval-paper > results/paper/long_24h.log 2>&1 &
```

Notes:
- The default matrix at 24h baselines is `4 wls × 4 strats × 3 reps × 24h = 1152h`.
  Cut workloads or strategies before launching, or accept the multi-week
  budget.
- Coverage curves are sampled every 2s by default
  (`baseline_timeseries.csv`), so you can plot convergence post-hoc.
- For **single very long cells** (one workload, one strategy, asymptote
  study), set `EVAL_WORKLOADS_BASELINE` and `EVAL_STRATEGIES` to a
  single value each, raise `EVAL_REPS` to 3, and crank `EVAL_DURATION_S`.
  This produces a tight asymptote estimate without burning a multi-week
  matrix budget.

## Troubleshooting

**Preflight fails with "no module named scripts"**: run from the repo root.
The eval scripts use `sys.path.insert` keyed off `__file__`, but the
preflight wrapper assumes `pwd` is the repo root.

**`make eval-paper-check` reports a DB unreachable**: confirm the
container is up (`docker ps`) and the DSN points at the right port. Mac
users on Homebrew MySQL 9 may hit the `mysql_native_password` plugin
issue with the bundled CLI — pymysql is unaffected, so a Python-level
connection should still work even if the `mysql` CLI errors.

**`bugs_mean = 0` on a workload that should fire**: confirm the patched
oracle is loaded by re-running phase 4 standalone:

```bash
.venv/bin/python scripts/validate_oracle_soundness.py \
  --db sqlite --workloads bank_overdraft --n-trials 100 --reps 1 \
  --output-dir /tmp/oracle_check
```

`tp` should be > 0 on `bank_overdraft`, `inventory_oversell`,
`unique_violation`. If it isn't, the patched oracle in
`scripts/schedule_pool/oracle.py` may have been reverted.

**Significance phase shows `MWU U=NaN` or skips a comparison**:
`evaluate_significance.py` requires at least 2 strategies per workload
in the input CSV. Confirm `EVAL_STRATEGIES` has ≥ 2 entries.

**Out-of-disk during a multi-day run**: the heaviest writers are
`baseline_timeseries.csv` (one row every `sample_interval` seconds per
cell) and the EDN disagreement files in
`oracle_soundness/disagreements/`. With 100% oracle agreement (the
expected state), no EDN files are written. If you see thousands of EDN
files, that's a regression — investigate before continuing.

**Resuming after interruption**: this version of `make eval-paper` is not
checkpointed. If a run is interrupted, the partial CSVs in
`$EVAL_OUT/baselines/` etc. are kept (each phase writes atomically at
end), but a re-run will overwrite. To resume from a specific phase,
invoke its script directly with the same args the Makefile would use —
see `scripts/evaluate_baselines.py --help` and the Makefile recipes.

## What "publishable" looks like

A run that tells the paper's intended story should produce, at minimum:

- **Phase 1**: `pool(heuristic).states_mean ≥ pct.states_mean` on
  `list_append` and `multikey_transfer`. Both should exceed
  `stress.states_mean` by ≥ $5\times$.
- **Phase 1**: `bugs_mean > 0` for every cell where the workload is in
  `{bank_overdraft, inventory_oversell, unique_violation}` (these are the
  three the patched oracle catches reliably on sqlite). On postgres/mysql
  with weaker isolation, `list_append` and `lost_update_counter` should
  also fire.
- **Phase 2**: `pool(heuristic).states_mean` grows super-linearly in
  `n_workers` on `multikey_transfer` (e.g., 3 → 15 → 232 at workers
  2/4/8); `stress` grows sub-linearly.
- **Phase 3**: `pool(heuristic) vs stress` reaches Cliff's $\delta=1.000$,
  `mwu_p ≤ 0.0339` on every workload. Effect-size magnitude is `large`.
- **Phase 4**: every row in `oracle_confusion.csv` has `agreement = 1.0000`,
  `fp = 0`, `fn = 0`. Workloads where the buggy variant fires should have
  `tp > 0`; where it does not, `tn = total`.

If any of these break, the run is not publishable as-is and the divergence
is itself the next thing to investigate.
