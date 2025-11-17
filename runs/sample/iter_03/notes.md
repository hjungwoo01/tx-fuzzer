### Iteration 3 Summary
- **History:** 18,415 committed txns with 36,820 dependency edges (WR=5,516 / WW=12,892 / RW=18,412); edges/txn ≈ 2.00, density ≈ 1.1e-4.
- **Hot Keys:** Pressure remains evenly split (key `1`: 12,430 edges, `2`: 12,255, `3`: 12,135) after switching writes to monotonic `now_ns` values.
- **Near Cycles:** Ten truncated rw→wr candidates, now spanning all three keys (e.g., txn_1→txn_13→txn_17 on key 2, txn_2→txn_4→txn_6 on key 3). We're closer to closing cycles thanks to tighter barriers.
- **Observations:** Using `now_ns` eliminated Elle collisions and preserved high contention. Still no confirmed cycles despite heavy overlap; early txns dominate near-cycle reports, implying we may need to stagger start jitter and lengthen client runtime to desynchronize commit order.

### Elle Check
- `make check HIST=runs/iter_03/history.edn` → **valid** (no anomalies detected).

### Next Steps
- Introduce slight per-client jitter and extend duration to 4s to diversify scheduling while keeping barriers at 2.
- Add a second read in the writer txn (post-update audit of a different key) to create more rw edges crossing keys, aiming to close cycles.
- Consider lifting read-only mix reduction further (e.g., 0.25) if rw edges plateau next round.
