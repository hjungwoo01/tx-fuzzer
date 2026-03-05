# lost_update_rc

Known anomaly scenario (not a PostgreSQL engine defect): naive read-modify-write
lost update under `READ COMMITTED`.

- Initial state: `counters(1) = 0`.
- Both clients attempt `value = read_value + 1`.
- Anomaly signal: both commit and final value is `1` (expected serial result is `2`).

This models application logic that does arithmetic client-side without
`SELECT ... FOR UPDATE` or atomic SQL updates.
