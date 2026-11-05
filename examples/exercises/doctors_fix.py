"""
Exercise: fix the doctors-on-call write skew so it survives *every* schedule.

Invariant: at least one doctor stays on call.  Each worker tries to take
doctor ``(worker_id % 2) + 1`` off call, but only if someone else is still on.

The starting point below is the classic "one atomic conditional UPDATE" fix.
It looks right, and it passes on SQLite (which serializes all writers), but
on PostgreSQL READ COMMITTED the tool breaks it with a single pause.  Your
job: make it hold.

Attack your fix (exit code 0 = it held; run from the repository root):

    python -m isotest sweep --workload doctors_on_call --db postgres \\
        --variant examples/exercises/doctors_fix.py:doctors_fix --expect clean

    python -m isotest explore --workload doctors_on_call --db postgres --workers 3 \\
        --variant examples/exercises/doctors_fix.py:doctors_fix --expect clean --trials 300

    python -m isotest replay examples/repros/doctors_on_call_postgres.json \\
        --variant examples/exercises/doctors_fix.py:doctors_fix

Contract: ``doctors_fix(adapter, worker_id) -> dict`` whose "outcome" is
"went_off_call", "stayed_on_call", or "error".  ``adapter`` offers
``begin(level)``, ``exec(sql, params) -> result`` (``.rows``, ``.rowcount``,
``.error_category.value`` -- "OK" on success), ``commit()``, ``rollback()``,
and ``db_name`` ("postgres" | "mysql" | "sqlite").

Hints (try without them first):
  1. Which rows does the guard *read*?  Which rows does the UPDATE *lock*?
  2. Under READ COMMITTED a statement only waits for rows it writes or locks.
  3. ``SELECT ... FOR UPDATE`` -- or SERIALIZABLE plus a retry on SQLSTATE 40001.

A reference solution is ``doctors_fixed`` in
scripts/experiments/extended_workloads.py (no peeking until you have tried).
"""


def doctors_fix(adapter, worker_id):
    my_id = (worker_id % 2) + 1
    sqlite = adapter.db_name == "sqlite"
    param = "?" if sqlite else "%s"
    on, off = ("1", "0") if sqlite else ("TRUE", "FALSE")

    adapter.begin("READ COMMITTED")
    # TODO: this guard reads rows the UPDATE never locks.  Make it hold.
    r = adapter.exec(
        f"UPDATE doctors SET on_call = {off} "
        f"WHERE id = {param} AND (SELECT COUNT(*) FROM doctors WHERE on_call = {on}) > 1",
        (my_id,),
    )
    if r.error_category.value != "OK":
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "doctor": my_id}
    if r.rowcount == 0:
        adapter.rollback()
        return {"outcome": "stayed_on_call", "doctor": my_id}
    cr = adapter.commit()
    if cr.error_category.value != "OK":
        return {"outcome": "error", "error": cr.error_category.value, "doctor": my_id}
    return {"outcome": "went_off_call", "doctor": my_id}
