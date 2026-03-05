# Known Bugs Benchmark Suite

This suite benchmarks two approaches on known PostgreSQL concurrency anomalies:

- `tool`: adaptive schedule mutation (the system-under-test runner).
- `baseline`: naive fixed concurrent script replay.

These cases are **known anomalies/limitations under weaker isolation**, not claims of
PostgreSQL engine defects.

## Cases

Manifest: `bench/known_bugs/manifest.yaml`

- `write_skew_rr`: classic write skew under `REPEATABLE READ`.
- `lost_update_rc`: naive read-modify-write lost update under `READ COMMITTED`.
- `serializable_write_skew`: expected `40001` guard behavior under `SERIALIZABLE`.

Each case is self-contained under:
`bench/known_bugs/<bug_name>/{schema.sql,setup.sql,workload.yaml,checker.py,README.md}`

## Start PostgreSQL (Docker)

If you do not already have local PostgreSQL:

```bash
docker run --name txfuzz-pg \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  -d postgres:16
```

If container already exists:

```bash
docker start txfuzz-pg
```

Set DSN:

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
```

## Run Full Benchmark

Convenience command:

```bash
./bench/run_known_bugs.sh --seeds 30 --max-trials 40 --time-budget 30
```

If runtime is too high on your machine, run `--seeds 20` and report that caveat in results.

Equivalent module command:

```bash
python -m bench.known_bugs.run benchmark --seeds 30 --max-trials 40 --time-budget 30
```

Artifacts are written under:
`bench/known_bugs/artifacts/benchmark_<timestamp>/`

Required outputs:

- `results.csv`
- `summary.md`
- `plots/*.png` (ECDF, when matplotlib is available)
- per-seed runner artifacts with `config.json`, `trials/`, `histories/`, `detection.json`

## Run One Bug Manually

Tool runner:

```bash
python -m bench.known_bugs.run tool --bug write_skew_rr --seed 7 --max-trials 20 --time-budget 15
```

Baseline runner:

```bash
python -m bench.known_bugs.baseline_runner --bug write_skew_rr --seed 7 --max-trials 20 --time-budget 15
```

## Manual Reproduction (single trial)

One deterministic baseline attempt for `lost_update_rc`:

```bash
python -m bench.known_bugs.baseline_runner \
  --bug lost_update_rc \
  --seed 1 \
  --max-trials 1 \
  --time-budget 30
```

Inspect trial artifact JSON under the run directory.

## Statistical Method

Primary metric:

- Time-to-first-detection per seed under fixed budget.

Secondary metrics:

- Trials-to-first-detection.
- Detection rate within budget.

Stat test and effect size:

- Mann–Whitney U on time-to-first-detection.
- Cliff's delta (positive means tool is faster).

Censoring rule:

- For hypothesis testing, undetected runs are encoded as `+infinity`.
- For descriptive moments/plots, undetected runs are right-censored at the time budget.

## Notes

- Every trial resets DB state by applying each case's `schema.sql` + `setup.sql`.
- Isolation level is explicitly set per transaction in each case runner.
- Concurrency is real: two concurrent client threads synchronize at a barrier and then race under per-step delays.
