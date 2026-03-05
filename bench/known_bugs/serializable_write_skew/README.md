# serializable_write_skew

Control case for the same write-skew application pattern under
`SERIALIZABLE` isolation.

- Expected signal: at least one transaction aborts with SQLSTATE `40001`
  (`serialization_failure`).
- This is expected behavior, not a bug.

The benchmark uses this case to measure how quickly each runner reaches the
conflict window that triggers SSI protection.
