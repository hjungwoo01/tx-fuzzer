"""
Tests for the workshop upgrade: statement-level injection, the shared trial
executor, categorized oracles, artifact metadata, EDN escaping, schedule
minimization, timeline rendering, and the `python -m isotest` CLI.

SQLite tests always run (stdlib only).  PostgreSQL regression tests run when
a server is reachable (POSTGRES_DSN / DATABASE_URL or localhost:5432/54326)
and are skipped otherwise.
"""

from __future__ import annotations

import json
import os
import sys
import importlib
import importlib.util
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from scripts.experiments.db_adapter import ExecResult, checked_isolation_level  # noqa: E402
from scripts.experiments.error_taxonomy import ErrorCategory  # noqa: E402
from scripts.experiments.micro_workloads import MICRO_WORKLOADS  # noqa: E402

importlib.import_module("scripts.experiments.extended_workloads")  # registers 8 more workloads
from scripts.schedule_pool.elle_logger import ElleLogger, _edn_value  # noqa: E402
from scripts.schedule_pool.execution import execute_trial  # noqa: E402
from scripts.schedule_pool.injection import (  # noqa: E402
    HarnessError,
    InjectionMode,
    InstrumentedAdapter,
    StrictAdapter,
    TraceEvent,
    WorkerPlan,
)
from scripts.schedule_pool.minimize import ddmin, minimize_schedule  # noqa: E402
from scripts.schedule_pool.oracle import LongForkOracle, OverdraftOracle  # noqa: E402
from scripts.schedule_pool.replay import assess_replay  # noqa: E402
from scripts.schedule_pool.schedule import Schedule, ScheduleArtifact  # noqa: E402
from scripts.schedule_pool.timeline import Style, render_plan, render_timeline  # noqa: E402
from scripts.schedule_pool.types import (  # noqa: E402
    BugCategory,
    DelayPoint,
    OracleVerdict,
    ReplayResult,
    ReplayVerification,
)


def sqlite_dsn() -> str:
    return os.path.join(tempfile.mkdtemp(prefix="isotest_test_"), "t.sqlite")


def _pg_dsn():
    if importlib.util.find_spec("psycopg2") is None:
        return None
    from isotest.cli import resolve_dsn, server_version
    dsn = resolve_dsn("postgres")
    try:
        server_version("postgres", dsn)
    except Exception:
        return None
    return dsn


PG_DSN = _pg_dsn()
needs_pg = pytest.mark.skipif(PG_DSN is None, reason="no reachable PostgreSQL server")


@pytest.fixture(scope="module")
def pg_dsn():
    """A private, auto-dropped schema so tests never collide with other runs."""
    from isotest import cli
    dsn = cli.sandbox_dsn("postgres", PG_DSN)
    yield dsn
    cli.cleanup_sandboxes([dsn])


# ---------------------------------------------------------------------------
# Injection proxy (no database: fake adapter + recorded sleeps)
# ---------------------------------------------------------------------------

class FakeAdapter:
    db_name = "fake"

    def __init__(self, fail_sql: str = ""):
        self.calls = []
        self._fail_sql = fail_sql

    def begin(self, isolation_level=""):
        self.calls.append("BEGIN")

    def exec(self, sql, params=None):
        self.calls.append(sql)
        if self._fail_sql and sql.startswith(self._fail_sql):
            return ExecResult(error_category=ErrorCategory.BUSY, error_msg="database is locked")
        return ExecResult(rows=[(100,)] if sql.startswith("SELECT") else [], rowcount=1)

    def commit(self):
        self.calls.append("COMMIT")
        return ExecResult()

    def rollback(self):
        self.calls.append("ROLLBACK")
        return ExecResult()


def _proxy(schedule, mode, worker=0, fail_sql=""):
    sleeps = []
    inner = FakeAdapter(fail_sql)
    plan = WorkerPlan.from_schedule(schedule, worker)
    clock = iter(range(0, 10_000)).__next__
    p = InstrumentedAdapter(inner, worker, plan, InjectionMode.parse(mode),
                            clock=lambda: float(clock()), sleep=sleeps.append)
    return p, inner, sleeps


def _txn(p):
    p.begin("READ COMMITTED")
    p.exec("SELECT balance FROM accounts")
    p.exec("UPDATE accounts SET balance = 0")
    p.exec("INSERT INTO log VALUES (1)")
    p.commit()


def test_step_mode_pauses_exactly_where_named():
    sched = Schedule([
        DelayPoint("s0_t0_q0_begin", 5.0),
        DelayPoint("s0_t0_q2_exec", 30.0),      # before the UPDATE
        DelayPoint("s0_t0_q4_commit", 7.0),     # commit (q index irrelevant)
        DelayPoint("s1_t0_q2_exec", 99.0),      # other worker: ignored
    ])
    p, inner, sleeps = _proxy(sched, "step")
    _txn(p)
    assert sleeps == [0.005, 0.030, 0.007]
    by_event = {e.event: e for e in p.trace}
    assert by_event["s0_t0_q2_exec"].delay_ms == 30.0
    assert by_event["s0_t0_q2_exec"].sql.startswith("UPDATE")
    assert by_event["s0_t0_q1_exec"].result == "100"
    assert by_event["s0_t0_q4_commit"].delay_ms == 7.0
    assert inner.calls[0] == "BEGIN" and inner.calls[-1] == "COMMIT"


def test_start_skew_mode_injects_nothing_inside_the_transaction():
    sched = Schedule([DelayPoint("s0_t0_q2_exec", 30.0), DelayPoint("s0_t0_q4_commit", 7.0)])
    p, _inner, sleeps = _proxy(sched, "start_skew")
    _txn(p)
    assert sleeps == []
    plan = WorkerPlan.from_schedule(Schedule([
        DelayPoint("s0_t0_q0_begin", 4.0), DelayPoint("s0_t0_q1_exec", 10.0),
        DelayPoint("s0_t0_q2_exec", 25.0), DelayPoint("s0_t0_q3_commit", 50.0)]), 0)
    # historical semantics: begin + max(exec); commit ignored
    assert plan.start_skew_ms() == 29.0


def test_worker_plan_ignores_malformed_and_zero_delays():
    plan = WorkerPlan.from_schedule(Schedule([
        DelayPoint("garbage", 10.0), DelayPoint("s0_t0_q1_exec", 0.0),
        DelayPoint("s0_t0_q1_exec_extra", 3.0)]), 0)
    assert plan.execs == {} and plan.begin == {} and plan.commit == {}


def test_proxy_records_errors_and_delegates_attributes():
    p, _inner, _ = _proxy(Schedule.empty(), "step", fail_sql="UPDATE")
    _txn(p)
    statuses = {e.event: e.status for e in p.trace}
    assert statuses["s0_t0_q2_exec"] == "BUSY"
    assert p.db_name == "fake"


def test_strict_adapter_turns_failed_reads_into_harness_errors():
    s = StrictAdapter(FakeAdapter(fail_sql="SELECT COUNT"), workload="w")
    assert s.exec("SELECT x FROM t").rows == [(100,)]
    with pytest.raises(HarnessError, match="canonical-state read failed"):
        s.exec("SELECT COUNT(*) FROM withdrawals WHERE success = 1")


def test_injection_mode_parse():
    assert InjectionMode.parse("STEP") is InjectionMode.STEP
    with pytest.raises(ValueError):
        InjectionMode.parse("sideways")


# ---------------------------------------------------------------------------
# Shared executor on real SQLite
# ---------------------------------------------------------------------------

def test_step_mode_honors_commit_and_summed_exec_delays_on_sqlite():
    wl = MICRO_WORKLOADS["bank_overdraft"]
    dsn = sqlite_dsn()
    commit = Schedule([DelayPoint(f"s{i}_t0_q4_commit", 120.0) for i in range(2)])
    two = Schedule([DelayPoint("s0_t0_q1_exec", 60.0), DelayPoint("s0_t0_q2_exec", 60.0)])
    tr_c = execute_trial(wl, db="sqlite", dsn=dsn, schedule=commit, n_workers=2, mode="step")
    tr_e = execute_trial(wl, db="sqlite", dsn=dsn, schedule=two, n_workers=2, mode="step")
    assert tr_c.elapsed_ms >= 110, "commit delays must be honored in step mode"
    assert tr_e.elapsed_ms >= 110, "same-worker exec delays must add up in step mode"
    legacy = execute_trial(wl, db="sqlite", dsn=dsn, schedule=commit, n_workers=2,
                           mode="start_skew")
    assert legacy.elapsed_ms < 100, "start_skew keeps the historical (commit-ignored) behavior"


def test_single_pause_deterministically_triggers_sqlite_swallowed_error():
    wl = MICRO_WORKLOADS["bank_overdraft"]
    dsn = sqlite_dsn()
    sched = Schedule([DelayPoint("s0_t0_q2_exec", 40.0)])
    results = [execute_trial(wl, db="sqlite", dsn=dsn, schedule=sched, n_workers=2,
                             mode="step", stagger_ms=10.0) for _ in range(3)]
    assert all(r.bug_found for r in results)
    assert {r.bug_category for r in results} == {BugCategory.SWALLOWED_ERROR}
    assert any(e.status == "BUSY" for e in results[0].trace)
    fixed = [execute_trial(wl, db="sqlite", dsn=dsn, schedule=sched, n_workers=2, mode="step",
                           stagger_ms=10.0, variant="fixed") for _ in range(3)]
    assert not any(r.bug_found for r in fixed)


def test_serial_baseline_is_clean_for_every_workload_on_sqlite():
    dsn = sqlite_dsn()
    for name, wl in sorted(MICRO_WORKLOADS.items()):
        tr = execute_trial(wl, db="sqlite", dsn=dsn, schedule=Schedule.empty(),
                           n_workers=2, mode="step", stagger_ms=15.0)
        assert not tr.bug_found, f"{name}: oracle fires in serial execution: {tr.bug_reason}"


def test_fixed_variants_are_clean_when_racing_on_sqlite():
    # Regression for the control-sweep false positives (unique_violation's
    # missing UNIQUE constraint; long_fork's unsound oracle).
    dsn = sqlite_dsn()
    for name, wl in sorted(MICRO_WORKLOADS.items()):
        for _ in range(2):
            tr = execute_trial(wl, db="sqlite", dsn=dsn, schedule=Schedule.empty(),
                               n_workers=4, mode="step", variant="fixed")
            assert not tr.bug_found, f"{name} fixed variant flagged: {tr.bug_reason}"


class _SlowWorkload:
    """Duck-typed workload whose worker 1 overruns the trial timeout."""
    name = "slow"

    def setup_sql(self, db):
        return ["CREATE TABLE IF NOT EXISTS t (x INTEGER)"]

    def run_buggy(self, adapter, worker_id):
        if worker_id == 1:
            time.sleep(0.6)
        return {"outcome": "done", "worker": f"w{worker_id}"}

    run_fixed = run_buggy

    def canonical(self, adapter):
        adapter.begin()
        r = adapter.exec("SELECT COUNT(*) FROM t")
        adapter.commit()
        return {"n": r.rows[0][0]}

    def oracle(self, outcomes, state):
        return False, "ok"

    def canonical_hash(self, state):
        return json.dumps(state, sort_keys=True)


def test_executor_labels_timed_out_workers_instead_of_reading_mid_transaction():
    tr = execute_trial(_SlowWorkload(), db="sqlite", dsn=sqlite_dsn(), schedule=Schedule.empty(),
                       n_workers=2, mode="step", worker_timeout_s=0.1)
    assert tr.timed_out
    assert tr.outcomes[1]["outcome"] == "timeout"
    assert tr.outcomes[0]["outcome"] == "done"


# ---------------------------------------------------------------------------
# Workload model, oracles, verdict categories
# ---------------------------------------------------------------------------

def test_workload_variants_and_variant_specific_setup():
    uv = MICRO_WORKLOADS["unique_violation"]
    assert any("UNIQUE" in s.upper() for s in uv.setup_for("sqlite", "fixed"))
    assert not any("UNIQUE" in s.upper() for s in uv.setup_for("sqlite", "buggy"))
    doc = MICRO_WORKLOADS["doctors_on_call"]
    assert doc.variants == ["buggy", "fixed", "naive_fix"]
    with pytest.raises(KeyError):
        doc.variant_fn("nope")


def test_oracle_verdict_backward_compatible_and_categorized():
    assert OracleVerdict(True, "x").category == BugCategory.ISOLATION_ANOMALY
    assert OracleVerdict(False, "ok", BugCategory.SWALLOWED_ERROR).category == ""
    od = OverdraftOracle()
    assert od.check([], {"balance": -60, "successful_withdrawals": 2}).category == \
        BugCategory.ISOLATION_ANOMALY
    phantom = od.check([{"outcome": "withdrawn"}] * 2, {"balance": 20, "successful_withdrawals": 1})
    assert phantom.category == BugCategory.SWALLOWED_ERROR and "Phantom success" in phantom.reason
    unreported = od.check([], {"balance": 20, "successful_withdrawals": 1})
    assert unreported.category == BugCategory.OUTCOME_MISMATCH


def _obs(a, b):
    return {"outcome": "observed", "observed_order": [a, b]}


def test_long_fork_oracle_requires_incomparable_views():
    lf = LongForkOracle()
    # Different prefixes of ONE commit order are fine (this used to be flagged).
    assert not lf.check([_obs(0, 0), _obs(1, 0), _obs(1, 1)], {}).bug_found
    # a-but-not-b vs b-but-not-a cannot come from a single order.
    v = lf.check([_obs(1, 0), _obs(0, 1)], {})
    assert v.bug_found and "incomparable" in v.reason


def test_isolation_level_whitelist():
    assert checked_isolation_level(" read   committed ") == "READ COMMITTED"
    for bad in ("SERIALIZABLE; DROP TABLE users", "IMMEDIATE", ""):
        with pytest.raises(ValueError):
            checked_isolation_level(bad)


# ---------------------------------------------------------------------------
# Artifact schema
# ---------------------------------------------------------------------------

def test_artifact_records_reproducibility_metadata(tmp_path):
    art = ScheduleArtifact(db="sqlite", workload="bank_overdraft",
                           schedule=Schedule([DelayPoint("s0_t0_q2_exec", 20.0)]),
                           bug_found=True, bug_reason="x", bug_category=BugCategory.SWALLOWED_ERROR,
                           n_workers=2, injection_mode="step", stagger_ms=9.0,
                           evidence_trace=[{"worker": 0, "event": "s0_t0_q0_begin",
                                            "phase": "begin", "sql": "", "delay_ms": 0.0,
                                            "start_ms": 0.1, "end_ms": 0.2, "status": "OK"}])
    p = art.save(tmp_path / "a.json")
    back = ScheduleArtifact.load(p)
    assert (back.n_workers, back.injection_mode, back.stagger_ms, back.format_version) == \
        (2, "step", 9.0, 2)
    assert back.bug_category == BugCategory.SWALLOWED_ERROR
    assert TraceEvent.from_dict(back.evidence_trace[0]).event == "s0_t0_q0_begin"


def test_legacy_artifacts_load_with_legacy_semantics():
    for f in sorted((ROOT / "examples" / "repros" / "legacy").glob("*.json")):
        art = ScheduleArtifact.load(f)
        assert art.format_version == 1
        assert art.injection_mode == "start_skew"
        assert art.n_workers is None


def test_shipped_examples_are_complete_v2_artifacts():
    shipped = sorted((ROOT / "examples" / "repros").glob("*.json"))
    assert shipped, "expected generated example artifacts in examples/repros/"
    for f in shipped:
        art = ScheduleArtifact.load(f)
        assert art.format_version == 2 and art.n_workers and art.injection_mode == "step", f.name
        assert art.discovered_state_hash and art.evidence_trace and art.replay_rate_buggy, f.name


# ---------------------------------------------------------------------------
# EDN escaping
# ---------------------------------------------------------------------------

def test_edn_strings_are_escaped_and_round_trip(tmp_path):
    edn_format = pytest.importorskip("edn_format")
    err = 'relation "accounts" does not exist\nLINE 1: x \\ y'
    path = tmp_path / "h.edn"
    with ElleLogger(path) as log:
        log.log_fail(process=0, f="txn", value={"weird key!": 1.5, "ok": [1, "two"]}, error=err)
    parsed = edn_format.loads(path.read_text().strip())
    assert parsed[edn_format.Keyword("error")] == err
    assert sorted(str(k) for k in parsed.keys()) == sorted(
        [":type", ":f", ":process", ":time", ":wall", ":value", ":error"])
    assert _edn_value(float("nan")) == "##NaN"


# ---------------------------------------------------------------------------
# Minimizer (pure)
# ---------------------------------------------------------------------------

def _needs(**mins):
    def test(s: Schedule) -> bool:
        d = {e.event: e.delay_ms for e in s.entries}
        return all(d.get(k, 0.0) >= v for k, v in mins.items())
    return test


def test_minimizer_finds_one_minimal_subset_and_thresholds():
    sched = Schedule([DelayPoint("a", 40.0), DelayPoint("b", 20.0),
                      DelayPoint("c", 30.0), DelayPoint("d", 5.0)])
    res = minimize_schedule(sched, _needs(a=10.0, b=5.0), margin=2.0)
    kept = {e.event: e.delay_ms for e in res.schedule.entries}
    assert set(kept) == {"a", "b"}
    assert res.thresholds_ms == {"a": 10.0, "b": 5.0}
    assert kept == {"a": 20.0, "b": 10.0}   # 2x margin, capped at the original
    assert res.depth == 2


def test_minimizer_edge_cases():
    sched = Schedule([DelayPoint("a", 10.0)])
    assert minimize_schedule(sched, lambda s: True).reproduces_without_delays
    nope = minimize_schedule(sched, lambda s: False)
    assert nope.schedule == sched and "does not reproduce" in nope.log[0]


def test_ddmin_is_one_minimal_for_random_monotone_predicates():
    import random
    rng = random.Random(1)
    for _ in range(30):
        universe = [DelayPoint(f"e{i}", 10.0) for i in range(rng.randint(2, 9))]
        need = set(rng.sample([p.event for p in universe], rng.randint(1, len(universe))))

        def test(points, need=need):
            return need <= {p.event for p in points}
        out = ddmin(universe, test)
        assert {p.event for p in out} == need


# ---------------------------------------------------------------------------
# Replay assessment + timeline
# ---------------------------------------------------------------------------

def _ver(bugs, n, expected="", hashes=None):
    hashes = hashes or ["h"] * n
    replays = [ReplayResult(i, expected, hashes[i], hashes[i] == expected, i < bugs, "", 1.0)
               for i in range(n)]
    return ReplayVerification("id", expected, n, sum(r.match for r in replays),
                              0.0, bugs, bugs / n, len(set(hashes)), replays)


def test_assess_replay_statuses():
    assert assess_replay(_ver(10, 10), expect_bug=True)[0] == "REPRODUCED"
    assert assess_replay(_ver(3, 10), expect_bug=True)[0] == "FLAKY"
    assert assess_replay(_ver(0, 10), expect_bug=True)[0] == "NOT_REPRODUCED"
    assert assess_replay(_ver(0, 10), expect_bug=False)[0] == "CLEAN"
    assert assess_replay(_ver(1, 10), expect_bug=False)[0] == "BROKEN"
    # No recorded hash: judged on determinism, never a misleading "0% match".
    msg = assess_replay(_ver(10, 10, expected=""), expect_bug=True)[1]
    assert "deterministic" in msg and "0%" not in msg


def _trace():
    return [
        TraceEvent(0, "s0_t0_q0_begin", "begin", "", 0.0, 0.1, 0.2, "OK"),
        TraceEvent(0, "s0_t0_q1_exec", "exec", "SELECT balance FROM accounts", 0.0, 0.2, 0.3, "OK", "100"),
        TraceEvent(1, "s1_t0_q0_begin", "begin", "", 0.0, 10.0, 10.1, "OK"),
        TraceEvent(0, "s0_t0_q2_exec", "exec", "UPDATE accounts SET balance = 0", 20.0, 20.4, 20.5, "BUSY"),
    ]


def test_timeline_shows_pauses_errors_and_verdict():
    text = render_timeline(_trace(), 2, style=Style(unicode=True), width=120,
                           verdict="Phantom success", verdict_is_bug=True)
    lines = text.splitlines()
    pause_i = next(i for i, l in enumerate(lines) if "pause 20.0 ms" in l)
    update_i = next(i for i, l in enumerate(lines) if "UPDATE" in l)
    w1_i = next(i for i, l in enumerate(lines) if l.strip().startswith("10.0 ms"))
    assert pause_i < w1_i < update_i, "pause starts before w1 begins; UPDATE resumes after"
    assert "BUSY" in lines[update_i] and "BUG: Phantom success" in text


def test_timeline_ascii_mode_is_pure_ascii():
    text = render_timeline(_trace(), 2, style=Style(unicode=False), width=100)
    assert text.isascii()
    plan = render_plan([DelayPoint("s0_t0_q2_exec", 20.0)], 2, {"s0_t0_q2_exec": "UPDATE x"},
                       style=Style(unicode=False))
    assert "w1: (no pauses)" in plan and "before UPDATE x" in plan


# ---------------------------------------------------------------------------
# CLI end-to-end on SQLite (stdlib only)
# ---------------------------------------------------------------------------

def _cli(*argv):
    from isotest.cli import main
    return main(list(argv))


def test_cli_explore_minimize_show_replay_on_sqlite(tmp_path, capsys):
    out_dir = tmp_path / "out"
    assert _cli("explore", "--db", "sqlite", "--workload", "bank_overdraft",
                "--trials", "80", "--out-dir", str(out_dir)) == 0
    art = out_dir / "bank_overdraft_sqlite.json"
    assert art.exists()
    assert _cli("minimize", str(art), "--replays", "2", "--validate", "5") == 0
    mini = ScheduleArtifact.load(art.with_suffix(".min.json"))
    orig = ScheduleArtifact.load(art)
    assert 1 <= mini.delay_count <= orig.delay_count
    assert mini.evidence_trace and mini.n_workers == 2 and mini.injection_mode == "step"
    capsys.readouterr()
    assert _cli("--ascii", "show", str(art.with_suffix(".min.json"))) == 0   # offline
    assert "BUG:" in capsys.readouterr().out
    assert _cli("replay", str(art.with_suffix(".min.json")), "--control", "--n", "5") == 0


def test_cli_dry_runs_need_no_database(tmp_path, capsys):
    assert _cli("explore", "--dry-run", "--db", "postgres",
                "--dsn", "postgres://u:secret@nowhere.invalid:1/x") == 0
    out = capsys.readouterr().out
    assert "secret" not in out and "***" in out
    assert _cli("lab", "--dry-run") == 0


def test_cli_list_json_and_usage_errors(capsys):
    assert _cli("--json", "list") == 0
    names = {w["workload"] for w in json.loads(capsys.readouterr().out)["workloads"]}
    assert {"bank_overdraft", "doctors_on_call"} <= names
    assert _cli("replay", "/nonexistent/artifact.json") == 2
    assert _cli("explore", "--workload", "no_such_workload", "--dry-run") == 2


def test_cli_selfcheck_subset_on_sqlite():
    assert _cli("selfcheck", "--db", "sqlite", "--workloads", "bank_overdraft",
                "unique_violation", "long_fork", "--trials", "2") == 0


# ---------------------------------------------------------------------------
# PostgreSQL regressions (skipped without a server)
# ---------------------------------------------------------------------------

@needs_pg
def test_pg_fixed_variants_clean_regression(pg_dsn):
    # bank_overdraft: canonical `success = 1` on BOOLEAN flagged 100% of trials;
    # doctors/fk: the old "fixes" let write skew / orphans through.
    for name in ("bank_overdraft", "doctors_on_call", "fk_phantom", "unique_violation"):
        wl = MICRO_WORKLOADS[name]
        for _ in range(3):
            tr = execute_trial(wl, db="postgres", dsn=pg_dsn, schedule=Schedule.empty(),
                               n_workers=4, mode="step", variant="fixed")
            assert not tr.bug_found, f"{name}: {tr.bug_reason}"


@needs_pg
def test_pg_single_pause_overdraft_and_naive_fix_is_broken(pg_dsn):
    bank = MICRO_WORKLOADS["bank_overdraft"]
    pause = Schedule([DelayPoint("s0_t0_q2_exec", 60.0)])
    tr = execute_trial(bank, db="postgres", dsn=pg_dsn, schedule=pause, n_workers=2,
                       mode="step", stagger_ms=20.0)
    assert tr.bug_found and tr.bug_category == BugCategory.ISOLATION_ANOMALY
    assert "Overdraft" in tr.bug_reason
    doc = MICRO_WORKLOADS["doctors_on_call"]
    commit_pause = Schedule([DelayPoint("s0_t0_q2_commit", 60.0)])
    naive = execute_trial(doc, db="postgres", dsn=pg_dsn, schedule=commit_pause, n_workers=2,
                          mode="step", stagger_ms=20.0, variant="naive_fix")
    fixed = execute_trial(doc, db="postgres", dsn=pg_dsn, schedule=commit_pause, n_workers=2,
                          mode="step", stagger_ms=20.0, variant="fixed")
    assert naive.bug_found and "Write skew" in naive.bug_reason
    assert not fixed.bug_found


@needs_pg
def test_pg_sandbox_isolates_concurrent_sessions():
    """Two sessions with fixed table names must not see each other's data."""
    from isotest import cli
    from scripts.experiments.db_adapter import make_adapter
    a_dsn = cli.sandbox_dsn("postgres", PG_DSN)
    b_dsn = cli.sandbox_dsn("postgres", PG_DSN)
    try:
        assert a_dsn != b_dsn
        a, b = make_adapter("postgres", a_dsn), make_adapter("postgres", b_dsn)
        try:
            a.setup(["CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)",
                     "INSERT INTO accounts VALUES (1, 100)"])
            b.setup(["CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)",
                     "INSERT INTO accounts VALUES (1, -1)"])
            assert a.exec("SELECT balance FROM accounts").rows == [(100,)]
            assert b.exec("SELECT balance FROM accounts").rows == [(-1,)]
        finally:
            a.close()
            b.close()
    finally:
        # Only this test's sandboxes: the module-scoped pg_dsn fixture's
        # schema must survive for the tests that run after this one.
        cli.cleanup_sandboxes([a_dsn, b_dsn])
    probe = make_adapter("postgres", PG_DSN)
    try:
        left = probe.exec("SELECT COUNT(*) FROM information_schema.schemata "
                          "WHERE schema_name = %s OR schema_name = %s",
                          (a_dsn.rsplit("%3D", 1)[-1], b_dsn.rsplit("%3D", 1)[-1])).rows[0][0]
    finally:
        probe.close()
    assert left == 0, "sandboxes must be dropped on cleanup"


# ---------------------------------------------------------------------------
# User-supplied variants (path/to/file.py:function)
# ---------------------------------------------------------------------------

_USER_BUGGY = """
def withdraw(adapter, worker_id):
    # Same check-then-act as the bundled buggy variant, errors ignored.
    adapter.begin("READ COMMITTED")
    r = adapter.exec("SELECT balance FROM accounts WHERE id = 1")
    if r.rows and r.rows[0][0] >= 80:
        adapter.exec("UPDATE accounts SET balance = balance - ? WHERE id = 1", (80,))
        adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) "
                     "VALUES (1, 80, ?, 1)", (f"w{worker_id}",))
        adapter.commit()
        return {"outcome": "withdrawn", "worker": f"w{worker_id}"}
    adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) "
                 "VALUES (1, 80, ?, 0)", (f"w{worker_id}",))
    adapter.commit()
    return {"outcome": "insufficient", "worker": f"w{worker_id}"}
"""

_USER_FIXED = """
def withdraw(adapter, worker_id):
    w = f"w{worker_id}"
    adapter.begin("READ COMMITTED")
    r = adapter.exec("UPDATE accounts SET balance = balance - 80 WHERE id = 1 AND balance >= 80")
    if r.error_category.value != "OK":
        adapter.rollback()
        return {"outcome": "error", "error": r.error_category.value, "worker": w}
    ok = r.rowcount == 1
    i = adapter.exec("INSERT INTO withdrawals (account_id, amount, worker, success) "
                     "VALUES (1, 80, ?, ?)", (w, 1 if ok else 0))
    if i.error_category.value != "OK":
        adapter.rollback()
        return {"outcome": "error", "error": i.error_category.value, "worker": w}
    c = adapter.commit()
    if c.error_category.value != "OK":
        return {"outcome": "error", "error": c.error_category.value, "worker": w}
    return {"outcome": "withdrawn" if ok else "insufficient", "worker": w}
"""


def test_user_variant_from_file_is_attacked_on_sqlite(tmp_path):
    buggy = tmp_path / "mine_buggy.py"
    buggy.write_text(_USER_BUGGY)
    fixed = tmp_path / "mine_fixed.py"
    fixed.write_text(_USER_FIXED)
    common = ["sweep", "--db", "sqlite", "--workload", "bank_overdraft", "--expect", "clean"]
    assert _cli(*common, "--variant", f"{buggy}:withdraw") == 1     # broken
    assert _cli(*common, "--variant", f"{fixed}:withdraw") == 0     # holds
    assert _cli(*common, "--variant", f"{fixed}:no_such_fn") == 2   # usage error
    assert _cli("explore", "--db", "sqlite", "--workload", "bank_overdraft", "--trials", "40",
                "--variant", f"{fixed}:withdraw", "--expect", "clean",
                "--out-dir", str(tmp_path / "o")) == 0


def test_exercise_starter_is_loadable_and_broken_only_where_expected():
    starter = ROOT / "examples" / "exercises" / "doctors_fix.py"
    # SQLite serializes writers, so the naive starter holds there ...
    assert _cli("sweep", "--db", "sqlite", "--workload", "doctors_on_call", "--expect", "clean",
                "--variant", f"{starter}:doctors_fix") == 0


@needs_pg
def test_exercise_starter_is_broken_on_postgres():
    starter = ROOT / "examples" / "exercises" / "doctors_fix.py"
    # ... but not under PostgreSQL READ COMMITTED.
    assert _cli("sweep", "--db", "postgres", "--workload", "doctors_on_call", "--expect", "clean",
                "--variant", f"{starter}:doctors_fix") == 1


def test_replay_min_rate_tolerates_rare_misses_but_never_a_dirty_control(monkeypatch):
    from isotest import cli
    from scripts.experiments.micro_runner import TrialResult

    def fake_results(pattern):
        return [TrialResult(outcomes=[], canonical_state={}, canonical_hash="h",
                            bug_found=b, bug_reason="Double claim!" if b else "ok",
                            elapsed_ms=1.0) for b in pattern]

    patterns = {"buggy": [True] * 9 + [False], "fixed": [False] * 9 + [True]}
    monkeypatch.setattr(cli, "run_n", lambda cfg, sched, n, variant=None, on_each=None:
                        fake_results(patterns[variant]))
    art = ScheduleArtifact(db="sqlite", workload="job_queue_claim", bug_found=True,
                           schedule=Schedule([DelayPoint("s0_t0_q2_exec", 10.0)]),
                           n_workers=2, injection_mode="step")
    cfg = cli.RunConfig(workload="job_queue_claim", db="sqlite", dsn="unused", mode="start_skew")
    out = cli.Out(json_mode=True)
    ok_strict, _ = cli.replay_report(art, cfg, out, n=10, variants=["buggy"], show_timeline=False)
    ok_tolerant, rows = cli.replay_report(art, cfg, out, n=10, variants=["buggy"],
                                          show_timeline=False, min_rate=0.9)
    assert not ok_strict and ok_tolerant and rows[0]["status"] == "FLAKY"
    ok_control, rows = cli.replay_report(art, cfg, out, n=10, variants=["fixed"],
                                         show_timeline=False, min_rate=0.1)
    assert not ok_control and rows[0]["status"] == "BROKEN"


def test_top_level_parser_does_not_abbreviate_subcommand_options():
    # On Python <= 3.11 the top-level parser prefix-matched `--n` (a `replay`
    # option) against --no-color/--no-sandbox and exited with "ambiguous
    # option".  Newer Pythons do not, so assert the guard itself too.
    from isotest.cli import build_parser
    parser = build_parser()
    assert parser.allow_abbrev is False
    args = parser.parse_args(["replay", "a.json", "--n", "5", "--min-rate", "0.9"])
    assert (args.n, args.min_rate) == (5, 0.9)


def test_serial_runs_are_verified_from_the_trace():
    # A "serial" run whose transactions overlapped (latency spike longer than
    # the stagger) must be retried with a longer stagger, never counted.
    from isotest import cli
    from scripts.experiments.micro_runner import TrialResult

    def ev(w, a, b):
        return TraceEvent(w, f"s{w}_t0_q0_begin", "begin", "", 0.0, a, b, "OK")

    assert not cli.transactions_overlapped([ev(0, 0, 3), ev(1, 10, 12)])
    assert cli.transactions_overlapped([ev(0, 0, 15), ev(1, 10, 12)])

    seen = []

    class Fake:
        stagger_ms = 10.0
        workload, db = "w", "sqlite"

        def run(self, schedule, variant=None, stagger_ms=None):
            seen.append(stagger_ms)
            overlap = stagger_ms < 40          # spikes until the stagger reaches 40 ms
            trace = [ev(0, 0, 25), ev(1, stagger_ms, stagger_ms + 2)]
            return TrialResult([], {}, "h", overlap, "Lost update!" if overlap else "ok", 1.0,
                               trace=trace)

    results, retries = cli.run_serial(Fake(), 1)
    assert seen == [10.0, 20.0, 40.0] and retries == 2
    assert [r.bug_found for r in results] == [False]


@needs_pg
def test_pg_serial_check_does_not_blame_overlap_on_the_harness(pg_dsn):
    # Reproduces the CI failure: with a too-short stagger the lost_update
    # counter "fails in serial execution" because the transactions overlap.
    from isotest import cli
    cfg = cli.RunConfig(workload="lost_update_counter", db="postgres", dsn=pg_dsn,
                        n_workers=2, mode="step", stagger_ms=1.0)
    naive = [cfg.run(Schedule.empty()) for _ in range(5)]
    assert all(cli.transactions_overlapped(t.trace) for t in naive)
    serial, retries = cli.run_serial(cfg, 5, max_retries=6)
    assert retries > 0 and not any(t.bug_found for t in serial)
