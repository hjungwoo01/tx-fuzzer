# Known-bugs benchmark suite

This benchmark suite is an auxiliary path for fixed, named anomaly cases. It is
useful for regression comparisons, but it is not the repository’s main
schedule-exploration workflow.

## What it compares

- the tool runner
- a simpler baseline runner

## Entry points

```bash
./bench/run_known_bugs.sh --seeds 30 --max-trials 40 --time-budget 30
python -m bench.known_bugs.run benchmark --seeds 30 --max-trials 40 --time-budget 30
```

## Cases

- `write_skew_rr`
- `lost_update_rc`
- `serializable_write_skew`

Each case directory contains its own schema, setup SQL, workload, checker, and
short case README.
