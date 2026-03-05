# write_skew_rr

Known anomaly scenario (not a PostgreSQL engine defect): classic write skew under
PostgreSQL `REPEATABLE READ` (snapshot isolation behavior).

- Invariant: at least one row in `on_call` must stay `TRUE`.
- Anomaly signal: both transactions commit and final `on_call_count == 0`.

The benchmark runner executes two concurrent clients with controlled per-step
sleeps and validates final state via `checker.py`.
