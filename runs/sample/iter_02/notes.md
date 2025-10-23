### Iteration 2 Summary
- **History:** 20,617 committed txns with 41,221 dependency edges (WR=7,164 / WW=13,446 / RW=20,611); edges/txn ≈ 2.00, graph density ≈ 1.0e-4.
- **Hot Keys:** Elle key pressure highlights `1` (13,714 edges), `2` (14,001), `3` (13,506) — contention is well balanced across the three-key focus set.
- **Near Cycles:** Ten truncated near-cycle paths, all pivoting on early transactions (`txn_1..txn_13`) and especially key `1`, indicating we are close to forming rw→wr loops but need tighter scheduling.
- **Observations:** Eliminating the nonexistent `ts` column fixed write failures. However, Elle's register checker aborted because distinct transactions wrote the same `v` (random collisions), so values need to be globally unique going forward.
- **Next Steps:** Encode writes with a monotonic value (e.g., `now_ns`) to satisfy Elle, shorten `barrier_every` to 2, and bias scheduling so the conflicting writer/reader pairs overlap more aggressively in the next workload.

### Elle Check
- `make check HIST=runs/iter_02/history.edn` failed: `Key 1 had value ... written by more than one op`. No anomaly verdict yet — rerun after ensuring unique write values.
