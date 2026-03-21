# `serializable_write_skew`

Auxiliary control case for the write-skew pattern under `SERIALIZABLE`.

Expected behavior:

- the database prevents the anomaly
- a serialization failure or equivalent protection appears instead of a bug

This case belongs to the `bench/known_bugs/` benchmark suite, not the main
schedule-pool evaluation path.
