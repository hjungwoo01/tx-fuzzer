### Iteration 1 Summary
- **History:** 30,060 committed txns, but 0 dependency edges (WR/WW/RW) → Elle saw only isolated reads.
- **Detected:** No cycles or anomalies; dependency density stayed at 0.0.
- **Observation:** Write transactions repeatedly failed because the workload targeted numeric keys while the schema stores text keys (`k1`..`k4`), so updates never overlapped.
- **Action Items:** Align key domain with seeded rows, boost writer bias, and raise concurrency to create actual write/read conflicts in the next iteration.

### Elle Check
- `make check HIST=runs/iter_01/history.edn` → **valid** (no anomalies detected).
