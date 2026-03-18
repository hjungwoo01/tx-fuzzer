# Known Bugs Benchmark Summary

Method notes:
- Mann–Whitney U and Cliff's delta use +infinity for undetected runs.
- Median/p90/mean/std below are computed on times censored at the run time budget.

## lost_update_rc

| runner | n | detections | rate | median_s | p90_s | mean_s | std_s | median_trials | p90_trials |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tool | 30 | 30 | 1.000 | 0.1850 | 0.2832 | 0.1998 | 0.0694 | 3.0000 | 4.0000 |
| baseline | 30 | 0 | 0.000 | 30.0000 | 30.0000 | 30.0000 | 0.0000 | 40.0000 | 40.0000 |

- Mann–Whitney U (time-to-first, +inf censoring): U=0.000, p=0.000000
- Cliff's delta (positive favors tool): 1.0000

## serializable_write_skew

| runner | n | detections | rate | median_s | p90_s | mean_s | std_s | median_trials | p90_trials |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tool | 30 | 30 | 1.000 | 0.1550 | 0.2550 | 0.1769 | 0.0703 | 2.0000 | 4.1000 |
| baseline | 30 | 0 | 0.000 | 30.0000 | 30.0000 | 30.0000 | 0.0000 | 40.0000 | 40.0000 |

- Mann–Whitney U (time-to-first, +inf censoring): U=0.000, p=0.000000
- Cliff's delta (positive favors tool): 1.0000

## write_skew_rr

| runner | n | detections | rate | median_s | p90_s | mean_s | std_s | median_trials | p90_trials |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| tool | 30 | 30 | 1.000 | 0.1565 | 0.2626 | 0.1819 | 0.0750 | 2.0000 | 4.1000 |
| baseline | 30 | 0 | 0.000 | 30.0000 | 30.0000 | 30.0000 | 0.0000 | 40.0000 | 40.0000 |

- Mann–Whitney U (time-to-first, +inf censoring): U=0.000, p=0.000000
- Cliff's delta (positive favors tool): 1.0000

## Plots

- bench/known_bugs/artifacts/benchmark_2026-02-25T15-19-26Z/plots/lost_update_rc_ecdf.png
- bench/known_bugs/artifacts/benchmark_2026-02-25T15-19-26Z/plots/serializable_write_skew_ecdf.png
- bench/known_bugs/artifacts/benchmark_2026-02-25T15-19-26Z/plots/write_skew_rr_ecdf.png
