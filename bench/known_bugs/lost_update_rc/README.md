# `lost_update_rc`

Auxiliary benchmark case for naive read-modify-write lost update under
`READ COMMITTED`.

Signal:

- both clients commit
- final counter/value reflects a lost update

This case belongs to the `bench/known_bugs/` benchmark suite, not the main
schedule-pool evaluation path.
