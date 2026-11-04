# Example ScheduleArtifacts

Each JSON file is a `ScheduleArtifact` produced by the tool itself
(`isotest explore` followed by `isotest minimize`).  It records the minimal
delay schedule, the workload and variant, the number of workers, the
injection mode, the baseline stagger, the database version it was found on,
and a statement-level evidence trace of one failing execution.

| File | DB | Variant | Finding | Category | Pauses |
|------|----|---------|---------|----------|--------|
| `bank_overdraft_sqlite.json` | SQLite | buggy | Phantom success: a worker reports a withdrawal the database rejected (`BUSY`) | swallowed_error | 1 |
| `bank_overdraft_postgres.json` | PostgreSQL 16 | buggy | Overdraft: `balance = -60` | isolation_anomaly | 1 |
| `job_queue_claim_postgres.json` | PostgreSQL 16 | buggy | Double claim: two workers own one job | isolation_anomaly | 1 |
| `doctors_on_call_postgres.json` | PostgreSQL 16 | buggy | Write skew (G2-item): no doctor on call | isolation_anomaly | 1 |
| `fk_phantom_naive_fix_postgres.json` | PostgreSQL 16 | naive_fix | Orphan child row (referential phantom) | isolation_anomaly | 2 |

The same `bank_overdraft` code and the same single pause give two different
findings.  SQLite refuses the stale write, but the application ignores the
error.  PostgreSQL READ COMMITTED accepts the write and the balance goes
negative.  The `fk_phantom` artifact needs two pauses: it is a depth-2 bug
that a single-pause sweep cannot reach.

## Replay

```bash
python -m isotest show   examples/repros/bank_overdraft_sqlite.json            # no database needed
python -m isotest replay examples/repros/bank_overdraft_sqlite.json --control  # stdlib only
python -m isotest replay examples/repros/doctors_on_call_postgres.json --control
make replay-examples                                                           # all of them (CI)
```

`--control` replays the artifact's own variant (expect: bug reproduced on
every run) and the `fixed` variant under the same schedule (expect: clean).
The exit code is 0 only if both expectations hold, so an artifact works as a
regression test.  On noisy CI runners use `--min-rate 0.9`, which tolerates
rare timing misses in *reproducing* the bug.  The clean expectation stays
strict.

Validation at commit time (PostgreSQL 16.15, SQLite 3.53): every artifact
reproduced 30/30 on its own variant with its `fixed` control clean 0/30.  It
held again (20/20 and 0/20) with every CPU core saturated.

## Legacy artifacts (`legacy/`)

These three files predate the artifact format v2.  They were hand-written:
they have no recorded state hash, worker count, or injection mode, and they
use the historical `start_skew` semantics.  They still load, and the
backward-compatibility tests use them.  They are not reliable reproductions:
on PostgreSQL 16 `job_queue_double_claim_postgres.json` reproduced 0/20.
`bank_overdraft_postgres.json` "reproduced" only through an oracle false
positive that has since been fixed.  `lost_append_mysql.json` has not been
re-validated.

## Generate your own

```bash
python -m isotest explore  --workload list_append --db postgres
python -m isotest minimize results/isotest/list_append_postgres.json
```
