# `write_skew_rr`

Auxiliary benchmark case for classic write skew under PostgreSQL
`REPEATABLE READ`.

Signal:

- both transactions commit
- final invariant is violated

This case belongs to the `bench/known_bugs/` benchmark suite, not the main
schedule-pool evaluation path.
