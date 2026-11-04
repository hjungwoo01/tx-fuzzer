"""
isotest -- find, explain, and pin down transaction-isolation bugs by
controlling *when* each SQL statement runs.

    python -m isotest list                         # workloads and anomaly classes
    python -m isotest doctor                       # is my machine ready?
    python -m isotest lab                          # guided 8-step workshop run
    python -m isotest explore  --workload bank_overdraft --db sqlite
    python -m isotest minimize results/isotest/bank_overdraft_sqlite.json
    python -m isotest show     results/isotest/bank_overdraft_sqlite.min.json
    python -m isotest replay   results/isotest/bank_overdraft_sqlite.min.json --control
    python -m isotest sweep    --workload doctors_on_call --db postgres --variant naive_fix
    python -m isotest selfcheck --db sqlite --adversarial

SQLite needs nothing beyond Python's standard library.  PostgreSQL needs
``psycopg2-binary``; MySQL needs ``pymysql``.

Exit codes: 0 = expectation met, 1 = expectation not met (bug missing, fix
broken, harness problem found), 2 = usage / environment error.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import secrets
import socket
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from isotest import __version__  # noqa: E402
from scripts.schedule_pool.event_id import get_workload_event_ids  # noqa: E402
from scripts.schedule_pool.execution import execute_trial  # noqa: E402
from scripts.schedule_pool.injection import (  # noqa: E402
    HarnessError,
    InjectionMode,
    TraceEvent,
    describe_events,
    discover_events,
)
from scripts.schedule_pool.minimize import minimize_schedule  # noqa: E402
from scripts.schedule_pool.mutator import MutationConfig, ScheduleMutator  # noqa: E402
from scripts.schedule_pool.pool import PoolConfig, SchedulePool  # noqa: E402
from scripts.schedule_pool.retention import RetentionPolicy  # noqa: E402
from scripts.schedule_pool.base_strategy import HeuristicStrategy  # noqa: E402
from scripts.schedule_pool.replay import assess_replay  # noqa: E402
from scripts.schedule_pool.schedule import Schedule, ScheduleArtifact  # noqa: E402
from scripts.schedule_pool.timeline import Style, render_plan, render_timeline  # noqa: E402
from scripts.schedule_pool.types import (  # noqa: E402
    BugCategory,
    DelayPoint,
    ReplayResult,
    ReplayVerification,
)

EXIT_OK, EXIT_FAIL, EXIT_ENV = 0, 1, 2
DBS = ("sqlite", "postgres", "mysql")
DEFAULT_OUT = ROOT / "results" / "isotest"
_EVENT_RE = re.compile(r"^s(\d+)_t(\d+)_q(\d+)_([a-z]+)$")

# Candidate DSNs tried in order when no --dsn / environment variable is set:
# a native install first, then the ports published by docker-compose files.
PG_CANDIDATES = (
    "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
    "postgres://postgres:postgres@localhost:54326/postgres?sslmode=disable",  # compose pg16_2
)
MYSQL_CANDIDATES = (
    "mysql://root:root@localhost:33080/test",  # docker-compose-multidb.yml mysql8_0_34
    "mysql://root:root@localhost:3307/test",   # docker-compose.yml mysql-litmus
)
DB_HINTS = {
    "postgres": "start one with:  docker compose -f docker-compose-multidb.yml up -d pg16_2"
                "   (or set POSTGRES_DSN=postgres://user:pass@host:port/db)",
    "mysql": "start one with:  docker compose -f docker-compose-multidb.yml up -d mysql8_0_34"
             "   (or set MYSQL_DSN=mysql://user:pass@host:port/db)",
}


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

class UsageError(Exception):
    """A problem with the command line or environment (exit code 2)."""


class Out:
    """Human output unless --json; progress lines only on a TTY."""

    def __init__(self, json_mode: bool = False, style: Optional[Style] = None) -> None:
        self.json_mode = json_mode
        self.style = style or Style.detect()
        self._tty = sys.stdout.isatty()
        self._progress_len = 0

    def say(self, msg: str = "") -> None:
        if self.json_mode:
            return
        self._clear_progress()
        print(msg, flush=True)

    def progress(self, msg: str) -> None:
        # Plain carriage return + padding (no ANSI erase codes): works on
        # legacy Windows consoles as well as every POSIX terminal.
        if self.json_mode or not self._tty:
            return
        msg = msg[:160]
        sys.stdout.write("\r" + msg + " " * max(0, self._progress_len - len(msg)))
        sys.stdout.flush()
        self._progress_len = len(msg)

    def _clear_progress(self) -> None:
        if self._progress_len:
            sys.stdout.write("\r" + " " * self._progress_len + "\r")
            sys.stdout.flush()
            self._progress_len = 0

    def emit_json(self, payload: Dict[str, Any]) -> None:
        if self.json_mode:
            print(json.dumps(payload, indent=2, default=str))

    def heading(self, msg: str) -> None:
        self.say("")
        self.say(self.style.bold(msg))


def redact(dsn: str) -> str:
    """Hide the password in a URL-style DSN before printing it."""
    try:
        u = urlparse(dsn)
    except ValueError:
        return dsn
    if not u.password:
        return dsn
    netloc = u.netloc.replace(f":{u.password}@", ":***@", 1)
    return urlunparse(u._replace(netloc=netloc))


def _tcp_reachable(dsn: str, timeout: float = 1.0) -> bool:
    u = urlparse(dsn)
    try:
        with socket.create_connection((u.hostname or "localhost", u.port or 5432), timeout=timeout):
            return True
    except OSError:
        return False


def private_sqlite_dsn() -> str:
    """A fresh SQLite file inside a new private (0700) temp directory."""
    return os.path.join(tempfile.mkdtemp(prefix="isotest_"), "isotest.sqlite")


def resolve_dsn(db: str, explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit
    if db == "sqlite":
        return os.environ.get("SQLITE_DSN") or private_sqlite_dsn()
    if db == "postgres":
        env = os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL")
        if env:
            return env
        for cand in PG_CANDIDATES:
            if _tcp_reachable(cand):
                return cand
        return PG_CANDIDATES[0]
    if db == "mysql":
        env = os.environ.get("MYSQL_DSN") or os.environ.get("MYSQL_URL")
        if env:
            return env
        for cand in MYSQL_CANDIDATES:
            if _tcp_reachable(cand):
                return cand
        return MYSQL_CANDIDATES[0]
    raise UsageError(f"unknown database {db!r} (choose from {', '.join(DBS)})")


# ---------------------------------------------------------------------------
# Per-session sandboxes: concurrent users of one server must not collide
# ---------------------------------------------------------------------------
#
# Every workload uses fixed table names (accounts, doctors, ...).  Two runs
# against the same PostgreSQL database -- two terminals, a CI job and a
# laptop, or thirty workshop attendees sharing one hosted server -- would
# DROP and rewrite each other's tables mid-trial.  Each CLI session therefore
# works in a private schema (PostgreSQL, via search_path) or database
# (MySQL) that is dropped on exit.

_SANDBOX_ENABLED = True
_SANDBOXES: List[Tuple[str, str, str]] = []  # (db, base dsn, namespace)


def _with_namespace(db: str, dsn: str, name: str) -> str:
    if db == "postgres":
        if "://" not in dsn:  # libpq key=value form
            return f"{dsn} options='-csearch_path={name}'"
        u = urlparse(dsn)
        q = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True) if k != "options"]
        q.append(("options", f"-csearch_path={name}"))
        return urlunparse(u._replace(query=urlencode(q)))
    u = urlparse(dsn)
    return urlunparse(u._replace(path="/" + name))


def sandbox_dsn(db: str, dsn: str, out: Optional["Out"] = None) -> str:
    """Return a DSN bound to a fresh private schema/database for this session."""
    if db not in ("postgres", "mysql") or not _SANDBOX_ENABLED:
        return dsn
    from scripts.experiments.db_adapter import make_adapter
    name = f"isotest_{os.getpid()}_{secrets.token_hex(3)}"
    try:
        admin = make_adapter(db, dsn)
        try:
            admin.setup([f"CREATE SCHEMA {name}" if db == "postgres" else f"CREATE DATABASE {name}"])
        finally:
            admin.close()
    except Exception as e:
        if out is not None:
            first = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
            out.say(out.style.yellow(
                f"  note: could not create a private {'schema' if db == 'postgres' else 'database'} "
                f"({first}); using the shared default -- concurrent users may interfere"))
        return dsn
    _SANDBOXES.append((db, dsn, name))
    return _with_namespace(db, dsn, name)


def cleanup_sandboxes() -> None:
    from scripts.experiments.db_adapter import make_adapter
    while _SANDBOXES:
        db, dsn, name = _SANDBOXES.pop()
        try:
            a = make_adapter(db, dsn)
            try:
                a.setup([f"DROP SCHEMA IF EXISTS {name} CASCADE" if db == "postgres"
                         else f"DROP DATABASE IF EXISTS {name}"])
            finally:
                a.close()
        except Exception:
            pass  # best effort; names are prefixed isotest_ for manual cleanup


def server_version(db: str, dsn: str) -> str:
    """Server version string, e.g. 'PostgreSQL 16.15' or 'SQLite 3.45.1'."""
    if db == "sqlite":
        import sqlite3
        return f"SQLite {sqlite3.sqlite_version}"
    from scripts.experiments.db_adapter import make_adapter
    a = make_adapter(db, dsn)
    try:
        r = a.exec("SELECT version()" if db == "postgres" else "SELECT VERSION()")
        v = str(r.rows[0][0]) if r.rows else "?"
    finally:
        a.close()
    if db == "postgres":
        return " ".join(v.split()[:2])
    return f"MySQL {v}"


def check_db(db: str, dsn: str) -> str:
    """Connect once; raise UsageError with a helpful hint if we cannot."""
    driver = {"postgres": "psycopg2", "mysql": "pymysql"}.get(db)
    if driver:
        try:
            __import__(driver)
        except ImportError:
            pkg = "psycopg2-binary" if db == "postgres" else "pymysql"
            raise UsageError(f"{db} needs the {driver} driver:  pip install {pkg}") from None
    try:
        return server_version(db, dsn)
    except Exception as e:  # driver-specific connection errors
        hint = DB_HINTS.get(db, "")
        raise UsageError(f"cannot connect to {db} at {redact(dsn)}: "
                         f"{str(e).strip().splitlines()[0] if str(e).strip() else e}\n  {hint}") from None


def load_registry():
    from scripts.experiments.micro_workloads import MICRO_WORKLOADS
    from scripts.experiments import extended_workloads  # noqa: F401  (registers 8 more)
    return MICRO_WORKLOADS, extended_workloads.CORPUS_METADATA


def get_workload(name: str):
    wls, _ = load_registry()
    if name not in wls:
        raise UsageError(f"unknown workload {name!r}; run `python -m isotest list`")
    return wls[name]


def ensure_variant(wl: Any, variant: str) -> str:
    """Resolve ``variant``; ``path/to/file.py:function`` loads user code.

    A user variant is any function ``fn(adapter, worker_id) -> outcome dict``
    with the same contract as the bundled ``run_buggy`` / ``run_fixed``.  It
    runs against the bundled workload's schema, seed data, canonical state,
    and oracle -- so attendees can attack *their own* fix.  The spec string
    itself is the variant name, so artifacts found with it can be replayed
    (from the same working directory) without extra flags.
    """
    if variant in wl.variants or ":" not in variant:
        if variant not in wl.variants:
            raise UsageError(f"workload {wl.name!r} has no variant {variant!r} "
                             f"(available: {', '.join(wl.variants)}, or path/to/file.py:function)")
        return variant
    import importlib
    import importlib.util
    target, func = variant.rsplit(":", 1)
    try:
        if target.endswith(".py"):
            path = Path(target)
            if not path.exists():
                raise UsageError(f"variant file not found: {target}")
            spec = importlib.util.spec_from_file_location(f"isotest_user_{path.stem}", path)
            module = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(module)
        else:
            module = importlib.import_module(target)
    except UsageError:
        raise
    except Exception as e:
        raise UsageError(f"could not load variant {variant!r}: {type(e).__name__}: {e}") from None
    fn = getattr(module, func, None)
    if not callable(fn):
        raise UsageError(f"{target} has no function {func!r}")
    wl.extra_variants[variant] = fn
    return variant


def event_space(workload: str, n_workers: int, trace: Iterable[TraceEvent]) -> List[str]:
    """Static event ids for the workload, refined by what a probe run reached.

    Commit/rollback delays match on (worker, txn, phase) regardless of the
    query index, so keep a single representative per terminal phase.
    """
    discovered = discover_events(trace)
    static = get_workload_event_ids(workload, n_workers)
    terminal: Dict[Tuple[int, int, str], str] = {}
    out = set()
    for e in list(discovered) + list(static):
        m = _EVENT_RE.match(e)
        if not m:
            continue
        w, t, ph = int(m.group(1)), int(m.group(2)), m.group(4)
        if w >= n_workers:
            continue
        if ph in ("commit", "rollback"):
            key = (w, t, ph)
            if key in terminal:
                continue
            terminal[key] = e
        out.add(e)
    return sorted(out)


# ---------------------------------------------------------------------------
# Run configuration
# ---------------------------------------------------------------------------

@dataclass
class RunConfig:
    workload: str
    db: str
    dsn: str
    n_workers: int = 2
    variant: str = "buggy"
    mode: str = "step"
    stagger_ms: float = 0.0
    timeout_s: float = 15.0
    db_version: str = ""

    @property
    def wl(self):
        return get_workload(self.workload)

    def run(self, schedule: Schedule, *, variant: Optional[str] = None,
            stagger_ms: Optional[float] = None, elle: Any = None):
        return execute_trial(
            self.wl,
            db=self.db,
            dsn=self.dsn,
            schedule=schedule,
            variant=variant or self.variant,
            n_workers=self.n_workers,
            mode=self.mode,
            stagger_ms=self.stagger_ms if stagger_ms is None else stagger_ms,
            worker_timeout_s=self.timeout_s,
            elle_logger=elle,
        )

    def describe(self) -> str:
        return (f"{self.workload} on {self.db_version or self.db} "
                f"({self.n_workers} workers, {self.mode} mode, stagger {self.stagger_ms:.1f} ms)")


@dataclass
class Calibration:
    stagger_ms: float
    events: List[str]
    labels: Dict[str, str]
    max_txn_ms: float
    serial_bug: str = ""   # non-empty if the bug fired in serial execution


def calibrate(cfg: RunConfig, variant: Optional[str] = None) -> Calibration:
    """Probe one serial execution: event space, labels, and a safe stagger.

    The stagger (start offset between consecutive workers) must exceed one
    transaction's duration so that, without pauses, execution is serial.

    Delay points are positional (the q-th statement of a transaction), so
    the event space belongs to one *variant*: a fix that issues different
    statements has different events.  Always calibrate the variant you
    are about to attack.
    """
    probe = cfg.run(Schedule.empty(), stagger_ms=50.0, variant=variant)
    durations = []
    for w in range(cfg.n_workers):
        evs = [e for e in probe.trace if e.worker == w]
        if evs:
            durations.append(max(e.end_ms for e in evs) - min(e.start_ms for e in evs))
    max_dur = max(durations or [1.0])
    stagger = float(max(8, min(250, math.ceil(4 * max_dur + 2))))
    return Calibration(
        stagger_ms=stagger,
        events=event_space(cfg.workload, cfg.n_workers, probe.trace),
        labels=describe_events(probe.trace),
        max_txn_ms=max_dur,
        serial_bug=probe.bug_reason if probe.bug_found else "",
    )


def config_from_args(args: argparse.Namespace, *, connect: bool = True) -> RunConfig:
    wl = get_workload(args.workload)
    variant = ensure_variant(wl, getattr(args, "variant", None) or "buggy")
    dsn = resolve_dsn(args.db, getattr(args, "dsn", None))
    cfg = RunConfig(
        workload=args.workload, db=args.db, dsn=dsn,
        n_workers=args.workers, variant=variant,
        mode=InjectionMode.parse(getattr(args, "mode", "step")).value,
        timeout_s=getattr(args, "timeout", 15.0),
    )
    if connect:
        cfg.db_version = check_db(cfg.db, cfg.dsn)
        cfg.dsn = sandbox_dsn(cfg.db, cfg.dsn)
    return cfg


def config_from_artifact(art: ScheduleArtifact, args: argparse.Namespace,
                         out: Out, *, connect: bool = True) -> RunConfig:
    if art.workload not in load_registry()[0]:
        raise UsageError(f"artifact workload {art.workload!r} is not registered")
    ensure_variant(get_workload(art.workload), art.variant)
    if art.n_workers is None:
        out.say(out.style.yellow(
            "  note: legacy artifact (format 1) does not record n_workers; assuming 4 "
            "and the historical start_skew injection mode"))
    db = getattr(args, "db", None) or art.db
    cfg = RunConfig(
        workload=art.workload, db=db,
        dsn=resolve_dsn(db, getattr(args, "dsn", None)),
        n_workers=art.n_workers or 4, variant=art.variant,
        mode=art.injection_mode or "start_skew", stagger_ms=art.stagger_ms,
        timeout_s=getattr(args, "timeout", 15.0),
    )
    if connect:
        cfg.db_version = check_db(cfg.db, cfg.dsn)
        cfg.dsn = sandbox_dsn(cfg.db, cfg.dsn, out)
    return cfg


def load_artifact(path: str) -> ScheduleArtifact:
    p = Path(path)
    if not p.exists():
        raise UsageError(f"artifact not found: {p}")
    try:
        return ScheduleArtifact.load(p)
    except (ValueError, KeyError, TypeError) as e:
        raise UsageError(f"not a valid ScheduleArtifact: {p}: {e}") from None


def trace_dicts(tr) -> List[Dict[str, Any]]:
    return [e.to_dict() for e in tr.trace]


def run_n(cfg: RunConfig, schedule: Schedule, n: int, variant: Optional[str] = None,
          on_each=None) -> List[Any]:
    results = []
    for i in range(n):
        tr = cfg.run(schedule, variant=variant)
        results.append(tr)
        if on_each:
            on_each(i, tr)
    return results


def verification_of(results: Sequence[Any], art_id: str, expected_hash: str) -> ReplayVerification:
    replays = [ReplayResult(
        replay_idx=i, expected_hash=expected_hash, actual_hash=tr.canonical_hash,
        match=(tr.canonical_hash == expected_hash), bug_found=tr.bug_found,
        bug_reason=tr.bug_reason, elapsed_ms=tr.elapsed_ms) for i, tr in enumerate(results)]
    n = len(replays)
    hm = sum(r.match for r in replays)
    bugs = sum(r.bug_found for r in replays)
    return ReplayVerification(
        artifact_id=art_id, expected_hash=expected_hash, total_replays=n,
        hash_match_count=hm, hash_match_rate=hm / max(n, 1),
        bug_replay_count=bugs, bug_replay_rate=bugs / max(n, 1),
        unique_hashes=len({r.actual_hash for r in replays}), replays=replays)


def _timeline(out: Out, trace: Sequence[Any], n_workers: int, verdict: str = "",
              is_bug: Optional[bool] = None) -> str:
    return render_timeline(trace, n_workers, style=out.style, verdict=verdict, verdict_is_bug=is_bug)


def _rate(k: int, n: int) -> str:
    return f"{k}/{n}"


def _status_color(out: Out, status: str) -> str:
    good = {"REPRODUCED", "CLEAN", "PASS"}
    bad = {"NOT_REPRODUCED", "BROKEN", "FAIL", "FLAKY"}
    if status in good:
        return out.style.green(status)
    if status in bad:
        return out.style.red(status)
    return status


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_list(args: argparse.Namespace, out: Out) -> int:
    wls, meta = load_registry()
    rows = []
    for name in sorted(wls):
        wl = wls[name]
        m = meta.get(name)
        rows.append({
            "workload": name,
            "bug_class": getattr(m, "bug_class", ""),
            "isolation": getattr(m, "isolation_level", ""),
            "variants": wl.variants,
            "workers": getattr(m, "recommended_workers", 2),
            "reference": getattr(m, "historical_ref", ""),
        })
    if out.json_mode:
        out.emit_json({"workloads": rows})
        return EXIT_OK
    out.say(f"{'workload':20s} {'anomaly class':26s} {'isolation':16s} variants")
    out.say("-" * 86)
    for r in rows:
        out.say(f"{r['workload']:20s} {r['bug_class'][:26]:26s} {r['isolation'][:16]:16s} "
                f"{', '.join(r['variants'])}")
    out.say("")
    out.say("Each workload has a buggy and a fixed transaction; some also ship a")
    out.say("plausible-but-wrong `naive_fix`.  Try:  python -m isotest lab")
    return EXIT_OK


def cmd_doctor(args: argparse.Namespace, out: Out) -> int:
    import sqlite3
    checks: List[Dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str, required: bool) -> None:
        checks.append({"check": name, "ok": ok, "detail": detail, "required": required})

    py_ok = sys.version_info >= (3, 9)
    add("python", py_ok, sys.version.split()[0] + ("" if py_ok else " (need >= 3.9)"), True)
    try:
        dsn = private_sqlite_dsn()
        con = sqlite3.connect(dsn)
        mode = con.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        con.close()
        add("sqlite", True, f"SQLite {sqlite3.sqlite_version} (journal_mode={mode})", True)
    except Exception as e:
        add("sqlite", False, str(e), True)
    wanted = set(args.db or [])
    for db, driver, pkg in (("postgres", "psycopg2", "psycopg2-binary"), ("mysql", "pymysql", "pymysql")):
        required = db in wanted
        try:
            mod = __import__(driver)
            add(f"{db} driver", True, f"{driver} {getattr(mod, '__version__', '')}".strip(), required)
            have_driver = True
        except ImportError:
            add(f"{db} driver", False, f"missing -> pip install {pkg}", required)
            have_driver = False
        dsn = resolve_dsn(db, args.dsn if (args.dsn and len(wanted) == 1 and db in wanted) else None)
        if not have_driver:
            add(f"{db} server", False, "skipped (no driver)", required)
            continue
        try:
            v = server_version(db, dsn)
            add(f"{db} server", True, f"{v} at {redact(dsn)}", required)
        except Exception as e:
            first = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
            add(f"{db} server", False, f"not reachable at {redact(dsn)} ({first}); {DB_HINTS[db]}", required)
    enc = (getattr(sys.stdout, "encoding", "") or "").lower()
    add("terminal", True, f"encoding={enc or '?'}; use --ascii if boxes look garbled", False)
    out_dir = Path(args.out_dir)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        probe = out_dir / ".write_test"
        probe.write_text("ok")
        probe.unlink()
        add("output dir", True, str(out_dir), True)
    except OSError as e:
        add("output dir", False, f"{out_dir}: {e}", True)

    failed_required = [c for c in checks if c["required"] and not c["ok"]]
    if out.json_mode:
        out.emit_json({"checks": checks, "ready": not failed_required})
    else:
        for c in checks:
            mark = out.style.green("ok  ") if c["ok"] else (
                out.style.red("FAIL") if c["required"] else out.style.yellow("--  "))
            out.say(f"  [{mark}] {c['check']:16s} {c['detail']}")
        out.say("")
        if failed_required:
            out.say(out.style.red("Not ready: fix the FAIL lines above."))
        else:
            dbs = ", ".join(["sqlite"] + sorted(d for d in ("postgres", "mysql")
                                                  if any(c["check"] == f"{d} server" and c["ok"] for c in checks)))
            out.say(out.style.green(f"Ready. Usable databases: {dbs}.  Next: python -m isotest lab"))
    return EXIT_FAIL if failed_required else EXIT_OK


def _mutation_config(stagger: float) -> MutationConfig:
    s = max(1.0, stagger)
    return MutationConfig(
        max_per_event_delay_ms=4 * s, max_total_delay_ms=8 * s, max_delay_count=6,
        add_delay_min_ms=0.5 * s, add_delay_max_ms=3 * s,
        jitter_max_ms=max(0.5, 0.2 * s), mutations_per_step=2,
    )


def explore(cfg: RunConfig, out: Out, *, trials: int, seconds: Optional[float], seed: int,
            replays: int, min_rate: float, out_dir: Path, full: bool,
            stagger_override: Optional[float] = None, elle_path: Optional[str] = None,
            calibration: Optional[Calibration] = None,
            hint_on_miss: bool = True) -> Dict[str, Any]:
    cal = calibration or calibrate(cfg)
    cfg.stagger_ms = cal.stagger_ms if stagger_override is None else stagger_override
    out.say(f"isotest explore: {cfg.describe()}")
    out.say(f"  event space: {len(cal.events)} delay points; serial txn ~{cal.max_txn_ms:.1f} ms")
    if cal.serial_bug:
        out.say(out.style.yellow(
            f"  warning: the oracle fires in SERIAL execution ({cal.serial_bug}); that is not "
            f"a concurrency bug -- check the workload/oracle (python -m isotest selfcheck)"))

    rng = random.Random(seed)
    pool = SchedulePool(config=PoolConfig())
    retention = RetentionPolicy(pool)
    mutator = ScheduleMutator(cal.events, _mutation_config(cfg.stagger_ms), rng)
    base = HeuristicStrategy(k=2, min_delay_ms=0.5 * cfg.stagger_ms, max_delay_ms=3 * cfg.stagger_ms)
    elle = None
    if elle_path:
        from scripts.schedule_pool.elle_logger import ElleLogger
        elle = ElleLogger(elle_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    states: set = set()
    found: List[ScheduleArtifact] = []
    best: Optional[ScheduleArtifact] = None
    n_trials = 0
    t_start = time.monotonic()
    try:
        while n_trials < trials:
            if seconds is not None and time.monotonic() - t_start >= seconds:
                break
            parent = pool.sample(rng, workload=cfg.workload, db=cfg.db)
            parent_sched = parent.schedule if parent else base.seed(cal.events, rng)
            cand = mutator.mutate(parent_sched)
            tr = cfg.run(cand, elle=elle)
            n_trials += 1
            states.add(tr.canonical_hash)
            rec = ScheduleArtifact(
                parent_schedule_id=parent.schedule_id if parent else None,
                db=cfg.db, workload=cfg.workload, variant=cfg.variant,
                strategy="schedule_pool(heuristic)", seed=seed, schedule=cand,
                discovered_state_hash=tr.canonical_hash, bug_found=tr.bug_found,
                bug_reason=tr.bug_reason, bug_category=tr.bug_category,
                n_workers=cfg.n_workers, injection_mode=cfg.mode,
                stagger_ms=cfg.stagger_ms, db_version=cfg.db_version,
            )
            retention.accept(rec)
            if tr.bug_found:
                hits = 1 + sum(cfg.run(cand).bug_found for _ in range(max(0, replays - 1)))
                rec.replay_rate_buggy = hits / max(1, replays)
                rec.evidence_trace = trace_dicts(tr)
                path = out_dir / f"{cfg.workload}_{cfg.db}_{rec.schedule_id}.json"
                rec.save(path)
                found.append(rec)
                best = max(found, key=lambda a: (a.replay_rate_buggy or 0.0,
                                                 -a.delay_count, -a.total_delay_ms))
                out.say(f"  [{n_trials:4d}] {out.style.red('BUG')} ({tr.bug_category}) "
                        f"{tr.bug_reason}  replayed {hits}/{replays}")
                if not full and (rec.replay_rate_buggy or 0) >= min_rate:
                    break
            out.progress(f"  [{n_trials:4d}/{trials}] states={len(states)} bugs={len(found)} "
                         f"pool={pool.size}")
    finally:
        if elle:
            elle.close()

    elapsed = time.monotonic() - t_start
    summary: Dict[str, Any] = {
        "workload": cfg.workload, "db": cfg.db, "db_version": cfg.db_version,
        "n_workers": cfg.n_workers, "mode": cfg.mode, "stagger_ms": cfg.stagger_ms,
        "trials": n_trials, "elapsed_s": round(elapsed, 2),
        "trials_per_s": round(n_trials / elapsed, 1) if elapsed > 0 else None,
        "unique_states": len(states), "bug_artifacts": len(found), "pool_size": pool.size,
        "best_artifact": None,
    }
    if best is not None:
        stable = out_dir / f"{cfg.workload}_{cfg.db}.json"
        best.save(stable)
        summary["best_artifact"] = str(stable)
        summary["best"] = {"schedule_id": best.schedule_id, "bug_reason": best.bug_reason,
                           "bug_category": best.bug_category,
                           "replay_rate": best.replay_rate_buggy,
                           "delay_points": best.delay_count,
                           "total_delay_ms": round(best.total_delay_ms, 1)}
        out.say("")
        out.say(out.style.bold(f"  FOUND after {n_trials} trials ({elapsed:.1f} s): "
                               f"{best.bug_reason}"))
        out.say(f"    category: {best.bug_category} | replay {best.replay_rate_buggy:.0%} | "
                f"{best.delay_count} delay points, {best.total_delay_ms:.1f} ms total")
        out.say(f"    saved:    {_rel(stable)}")
        out.say(f"    next:     python -m isotest minimize {_rel(stable)}")
    else:
        out.say(f"  no bug in {n_trials} trials ({elapsed:.1f} s); {len(states)} distinct end states."
                + ("  Try --trials 300 or --workers 3." if hint_on_miss else ""))
    return summary


def _rel(p: Path) -> str:
    try:
        return str(Path(p).resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(p)


def cmd_explore(args: argparse.Namespace, out: Out) -> int:
    if args.dry_run:
        cfg = config_from_args(args, connect=False)
        events = get_workload_event_ids(cfg.workload, cfg.n_workers)
        rng = random.Random(args.seed)
        stagger = args.stagger_ms or 10.0
        mut = ScheduleMutator(events, _mutation_config(stagger), rng)
        base = HeuristicStrategy(k=2, min_delay_ms=0.5 * stagger, max_delay_ms=3 * stagger)
        samples = [mut.mutate(base.seed(events, rng)) for _ in range(3)]
        plan = {
            "workload": cfg.workload, "db": cfg.db, "dsn": redact(cfg.dsn),
            "n_workers": cfg.n_workers, "variant": cfg.variant, "mode": cfg.mode,
            "budget_trials": args.trials, "budget_seconds": args.seconds,
            "static_event_space": events,
            "sample_candidates": [s.to_list() for s in samples],
            "note": "dry run: no database connection was made; stagger is calibrated at run time",
        }
        if out.json_mode:
            out.emit_json(plan)
            return EXIT_OK
        out.say(f"DRY RUN -- explore {cfg.workload} on {cfg.db} ({redact(cfg.dsn)})")
        out.say(f"  workers={cfg.n_workers} variant={cfg.variant} mode={cfg.mode} "
                f"budget={args.trials} trials" + (f" / {args.seconds}s" if args.seconds else ""))
        out.say(f"  static event space ({len(events)}): {', '.join(events)}")
        out.say("  first candidate schedules the search would try (seeded):")
        for s in samples:
            out.say(render_plan(s.entries, cfg.n_workers, style=out.style))
            out.say("")
        out.say("  (calibration probes one serial run to measure txn time and refine events)")
        return EXIT_OK
    cfg = config_from_args(args)
    summary = explore(cfg, out, trials=args.trials, seconds=args.seconds, seed=args.seed,
                      replays=args.replays, min_rate=args.min_rate, out_dir=Path(args.out_dir),
                      full=args.full and args.expect != "clean", stagger_override=args.stagger_ms,
                      elle_path=args.elle, hint_on_miss=args.expect != "clean")
    out.emit_json(summary)
    if args.expect == "clean":
        if summary["best_artifact"]:
            out.say(out.style.red(f"  FAIL: expected {cfg.variant} to survive, but a schedule broke it."))
            return EXIT_FAIL
        out.say(out.style.green(f"  PASS: {cfg.variant} survived {summary['trials']} explored schedules."))
        return EXIT_OK
    return EXIT_OK if summary["best_artifact"] else EXIT_FAIL


def replay_report(art: ScheduleArtifact, cfg: RunConfig, out: Out, *, n: int,
                  variants: Sequence[str], show_timeline: bool,
                  min_rate: float = 1.0) -> Tuple[bool, List[Dict[str, Any]]]:
    """Replay each variant ``n`` times and judge it.

    A variant expected to show the bug passes if it reproduced in at least
    ``min_rate`` of the replays (1.0 = every replay).  A variant expected to
    be clean is always strict: a single oracle hit fails it, so a regressed
    fix can never hide behind a tolerance.
    """
    rows = []
    all_ok = True
    for v in variants:
        expect_bug = (v == art.variant and art.bug_found)
        expected_hash = (art.discovered_state_hash or "") if v == art.variant else ""

        def tick(i: int, tr: Any) -> None:
            out.progress(f"  {v}: replay {i + 1}/{n} ...")

        results = run_n(cfg, art.schedule, n, variant=v, on_each=tick)
        ver = verification_of(results, art.schedule_id, expected_hash)
        scheduled = art.schedule.total_delay_ms
        applied = min(sum(e.delay_ms for e in tr.trace) for tr in results) if results else 0.0
        status, detail = assess_replay(ver, expect_bug=expect_bug)
        if expect_bug:
            ok = status == "REPRODUCED" or (
                status == "FLAKY" and ver.bug_replay_rate >= min_rate)
            if ok and status == "FLAKY":
                detail += f" (accepted: rate >= --min-rate {min_rate:g})"
        else:
            ok = status == "CLEAN"
        all_ok &= ok
        cats = sorted({tr.bug_category for tr in results if tr.bug_found})
        reasons = sorted({tr.bug_reason for tr in results if tr.bug_found})
        unexercised = cfg.mode == InjectionMode.STEP.value and applied + 0.05 < scheduled
        rows.append({"variant": v, "expect_bug": expect_bug, "status": status, "ok": ok,
                     "bug_replays": ver.bug_replay_count, "replays": n,
                     "unique_end_states": ver.unique_hashes, "detail": detail,
                     "categories": cats, "reasons": reasons[:3],
                     "pause_ms_scheduled": round(scheduled, 2),
                     "pause_ms_applied": round(applied, 2)})
        verdict = "expect bug" if expect_bug else "expect clean"
        out.say(f"  {v:10s} ({verdict:12s}) {_status_color(out, status):>14s}  {detail}")
        for r in reasons[:2]:
            out.say(f"  {'':10s}   e.g. {r}")
        if unexercised:
            out.say(out.style.yellow(
                f"  {'':10s}   warning: only {applied:.1f} of {scheduled:.1f} ms of scheduled pauses "
                f"were reached -- {v} issues different statements, so this replay barely tests it. "
                f"Attack it directly: python -m isotest sweep --workload {art.workload} "
                f"--db {cfg.db} --variant {v} --workers {cfg.n_workers} --expect clean"))
        if show_timeline:
            pick = next((tr for tr in results if tr.bug_found), results[-1])
            out.say(_timeline(out, pick.trace, cfg.n_workers,
                              verdict=pick.bug_reason, is_bug=pick.bug_found))
            out.say("")
    return all_ok, rows


def cmd_replay(args: argparse.Namespace, out: Out) -> int:
    art = load_artifact(args.artifact)
    cfg = config_from_artifact(art, args, out, connect=not args.dry_run)
    wl = cfg.wl
    if args.variant:
        variants = [args.variant]
    else:
        variants = [art.variant] + (["fixed"] if args.control and art.variant != "fixed" else [])
    variants = [ensure_variant(wl, v) for v in variants]
    out.say(f"isotest replay: artifact {art.schedule_id} -- {cfg.describe()}")
    out.say(f"  finding: {art.bug_reason or '(none recorded)'}"
            + (f"  [{art.bug_category}]" if art.bug_category else ""))
    out.say(render_plan(art.schedule.entries, cfg.n_workers,
                        describe_events(TraceEvent.from_dict(d) for d in art.evidence_trace),
                        style=out.style))
    if args.dry_run:
        out.say(f"  DRY RUN: would replay {args.n}x each of: {', '.join(variants)}")
        out.emit_json({"artifact": art.schedule_id, "variants": variants, "n": args.n,
                       "dry_run": True})
        return EXIT_OK
    ok, rows = replay_report(art, cfg, out, n=args.n, variants=variants,
                             show_timeline=args.timeline, min_rate=args.min_rate)
    out.say("")
    out.say(out.style.green("PASS: every expectation held.") if ok else
            out.style.red("FAIL: at least one expectation did not hold (see above)."))
    out.emit_json({"artifact": art.schedule_id, "workload": art.workload, "db": cfg.db,
                   "db_version": cfg.db_version, "ok": ok, "results": rows})
    return EXIT_OK if ok else EXIT_FAIL


def minimize_artifact(art: ScheduleArtifact, cfg: RunConfig, out: Out, *, replays: int,
                      validate: int, margin: float) -> Tuple[ScheduleArtifact, Dict[str, Any]]:
    want_cat = art.bug_category

    def test(s: Schedule) -> bool:
        for _ in range(replays):
            tr = cfg.run(s)
            if not tr.bug_found or (want_cat and tr.bug_category != want_cat):
                return False
        return True

    marks: List[str] = []

    def on_test(s: Schedule, ok: bool) -> None:
        marks.append("+" if ok else ".")
        out.progress(f"  testing candidates ({replays} runs each): {''.join(marks)[-60:]}")

    t0 = time.monotonic()
    res = minimize_schedule(art.schedule, test, margin=margin, on_test=on_test)
    elapsed = time.monotonic() - t0
    for line in res.log:
        out.say(f"  {line}")

    final = cfg.run(res.schedule)
    hits = sum(tr.bug_found for tr in [final] + run_n(cfg, res.schedule, max(0, validate - 1)))
    m = ScheduleArtifact(
        parent_schedule_id=art.schedule_id, db=cfg.db, workload=art.workload,
        variant=art.variant, strategy="minimized", seed=art.seed, schedule=res.schedule,
        discovered_state_hash=final.canonical_hash, bug_found=final.bug_found,
        bug_reason=final.bug_reason or art.bug_reason, bug_category=final.bug_category or art.bug_category,
        replay_rate_buggy=hits / max(1, validate), retention_reason="minimized",
        n_workers=cfg.n_workers, injection_mode=cfg.mode, stagger_ms=cfg.stagger_ms,
        db_version=cfg.db_version or art.db_version, evidence_trace=trace_dicts(final),
    )
    stats = {"original_points": art.delay_count, "minimized_points": m.delay_count,
             "original_total_ms": round(art.total_delay_ms, 1),
             "minimized_total_ms": round(m.total_delay_ms, 1),
             "thresholds_ms": res.thresholds_ms, "tests_run": res.tests_run,
             "cache_hits": res.cache_hits, "elapsed_s": round(elapsed, 2),
             "validation": f"{hits}/{validate}",
             "reproduces_without_delays": res.reproduces_without_delays}
    return m, stats


def cmd_minimize(args: argparse.Namespace, out: Out) -> int:
    art = load_artifact(args.artifact)
    if not art.bug_found:
        raise UsageError("artifact does not record a bug; nothing to minimize")
    if args.dry_run:
        cfg = config_from_artifact(art, args, out, connect=False)
        n = art.delay_count
        out.say(f"DRY RUN -- minimize {art.schedule_id} ({n} delay points, "
                f"{art.total_delay_ms:.1f} ms) on {cfg.db}")
        out.say(f"  ddmin needs at most ~{max(1, n * n)} candidate tests (usually far fewer), "
                f"then <= {int(math.log2(max(2.0, art.total_delay_ms)))} halvings per kept point;")
        out.say(f"  each test re-runs the schedule {args.replays}x and passes only if every run "
                f"reproduces the same bug category ({art.bug_category or 'any'}).")
        return EXIT_OK
    cfg = config_from_artifact(art, args, out)
    out.say(f"isotest minimize: {art.schedule_id} -- {cfg.describe()}")
    out.say(f"  start: {art.delay_count} delay points, {art.total_delay_ms:.1f} ms total; "
            f"finding: {art.bug_reason}")
    m, stats = minimize_artifact(art, cfg, out, replays=args.replays, validate=args.validate,
                                 margin=args.margin)
    dest = Path(args.out) if args.out else Path(args.artifact).with_suffix(".min.json")
    m.save(dest)
    out.say("")
    out.say(out.style.bold(f"  minimal schedule: {stats['original_points']} -> "
                           f"{stats['minimized_points']} delay point(s), "
                           f"{stats['original_total_ms']} -> {stats['minimized_total_ms']} ms total"))
    out.say(render_plan(m.schedule.entries, cfg.n_workers,
                        describe_events(TraceEvent.from_dict(d) for d in m.evidence_trace),
                        style=out.style))
    out.say(f"  validated {stats['validation']} replays; {stats['tests_run']} candidate tests "
            f"({stats['cache_hits']} cached) in {stats['elapsed_s']} s")
    out.say("")
    out.say(_timeline(out, m.evidence_trace, cfg.n_workers, verdict=m.bug_reason, is_bug=m.bug_found))
    out.say("")
    out.say(f"  saved: {_rel(dest)}")
    out.say(f"  next:  python -m isotest replay {_rel(dest)} --control")
    stats["artifact"] = str(dest)
    out.emit_json(stats)
    ok = m.bug_found and (m.replay_rate_buggy or 0) >= args.min_rate
    return EXIT_OK if ok else EXIT_FAIL


def cmd_show(args: argparse.Namespace, out: Out) -> int:
    art = load_artifact(args.artifact)
    cfg = config_from_artifact(art, args, out, connect=args.run)
    trace = [TraceEvent.from_dict(d) for d in art.evidence_trace]
    verdict, is_bug = art.bug_reason, art.bug_found
    if args.run:
        tr = cfg.run(art.schedule)
        trace, verdict, is_bug = tr.trace, tr.bug_reason, tr.bug_found
    if out.json_mode:
        out.emit_json({"artifact": art.to_dict(), "trace": [t.to_dict() for t in trace]})
        return EXIT_OK
    out.say(f"artifact {art.schedule_id}: {art.workload} on {art.db_version or art.db} "
            f"({cfg.n_workers} workers, {cfg.mode} mode, stagger {cfg.stagger_ms:.1f} ms)")
    if art.bug_found:
        rate = f"{art.replay_rate_buggy:.0%}" if art.replay_rate_buggy is not None else "n/a"
        out.say(f"finding: [{art.bug_category or 'unclassified'}] {art.bug_reason}  (replay rate {rate})")
    if art.bug_category == BugCategory.SWALLOWED_ERROR:
        out.say(out.style.yellow(
            "  note: the database rejected a conflicting write and the application ignored the "
            "error -- an error-handling bug, not an isolation anomaly."))
    out.say("delay plan:")
    out.say(render_plan(art.schedule.entries, cfg.n_workers, describe_events(trace), style=out.style))
    if not trace:
        out.say("(no recorded trace in this artifact; re-run with --run to execute it once)")
        return EXIT_OK
    out.say("")
    out.say("interleaving" + (" (fresh run):" if args.run else " (recorded evidence):"))
    out.say(_timeline(out, trace, cfg.n_workers, verdict=verdict, is_bug=is_bug))
    return EXIT_OK


def sweep(cfg: RunConfig, cal: Calibration, *, pause_ms: float, trials: int,
          variant: str, out: Out, include_baseline: bool = True) -> List[Dict[str, Any]]:
    """Depth-1 exhaustive search: one pause at each event, from a serial baseline."""
    rows = []
    cases: List[Tuple[str, Schedule]] = []
    if include_baseline:
        cases.append(("(no pause)", Schedule.empty()))
    cases += [(e, Schedule([DelayPoint(e, pause_ms)])) for e in cal.events]
    for i, (label, sched) in enumerate(cases):
        out.progress(f"  sweep {i + 1}/{len(cases)}: {label}")
        results = run_n(cfg, sched, trials, variant=variant)
        bugs = [tr for tr in results if tr.bug_found]
        rows.append({"event": label, "label": cal.labels.get(label, ""),
                     "bugs": len(bugs), "trials": trials,
                     "category": bugs[0].bug_category if bugs else "",
                     "reason": bugs[0].bug_reason if bugs else ""})
    return rows


def cmd_sweep(args: argparse.Namespace, out: Out) -> int:
    cfg = config_from_args(args)
    cal = calibrate(cfg)
    cfg.stagger_ms = args.stagger_ms or cal.stagger_ms
    pause = args.pause_ms or 3 * cfg.stagger_ms
    out.say(f"isotest sweep: {cfg.describe()} -- variant {cfg.variant}")
    out.say(f"  one {pause:.1f} ms pause at each of {len(cal.events)} events, "
            f"{args.trials} run(s) each, from a serial baseline")
    rows = sweep(cfg, cal, pause_ms=pause, trials=args.trials, variant=cfg.variant, out=out)
    out.say("")
    out.say(f"  {'pause at':18s} {'before':40s} {'bug':>5s}  finding")
    for r in rows:
        mark = out.style.red(f"{r['bugs']}/{r['trials']}") if r["bugs"] else f"{r['bugs']}/{r['trials']}"
        out.say(f"  {r['event']:18s} {r['label'][:40]:40s} {mark:>5s}  {r['reason'][:70]}")
    triggering = [r for r in rows if r["bugs"]]
    out.say("")
    out.say(f"  {len(triggering)} of {len(rows)} single-pause schedules trigger the oracle.")
    out.emit_json({"workload": cfg.workload, "db": cfg.db, "variant": cfg.variant,
                   "pause_ms": pause, "stagger_ms": cfg.stagger_ms, "rows": rows})
    if args.expect == "clean":
        return EXIT_FAIL if triggering else EXIT_OK
    if args.expect == "bug":
        return EXIT_OK if triggering else EXIT_FAIL
    return EXIT_OK


def cmd_stress(args: argparse.Namespace, out: Out) -> int:
    cfg = config_from_args(args)
    cal = calibrate(cfg)
    jitter = args.jitter_ms or 4 * cal.stagger_ms
    rng = random.Random(args.seed)
    out.say(f"isotest stress: {cfg.workload} on {cfg.db_version} ({cfg.n_workers} workers, "
            f"{cfg.variant}) -- no targeted pauses; random arrival jitter 0-{jitter:.0f} ms")
    hits = []
    for i in range(args.trials):
        sched = Schedule([DelayPoint(f"s{w}_t0_q0_begin", rng.uniform(0, jitter))
                          for w in range(cfg.n_workers)])
        tr = cfg.run(sched, stagger_ms=0.0)
        if tr.bug_found:
            hits.append(tr)
        out.progress(f"  trial {i + 1}/{args.trials}: bugs so far {len(hits)}")
    k, n = len(hits), args.trials
    out.say(f"  oracle fired in {k}/{n} runs ({k / n:.0%})"
            + (f" -- on average one failing CI run in {n / k:.1f}" if k else
               " -- this suite would stay green"))
    if hits and args.timeline:
        out.say(_timeline(out, hits[0].trace, cfg.n_workers, verdict=hits[0].bug_reason, is_bug=True))
    out.emit_json({"workload": cfg.workload, "db": cfg.db, "variant": cfg.variant,
                   "jitter_ms": jitter, "bugs": k, "trials": n})
    return EXIT_OK


def cmd_selfcheck(args: argparse.Namespace, out: Out) -> int:
    wls, _ = load_registry()
    names = args.workloads or sorted(wls)
    dbs = args.db or ["sqlite"]
    rows = []
    for db in dbs:
        dsn = resolve_dsn(db, args.dsn if len(dbs) == 1 else None)
        try:
            version = check_db(db, dsn)
        except UsageError as e:
            out.say(out.style.red(f"  {db}: {e}"))
            rows.append({"db": db, "workload": "*", "ok": False, "problems": [str(e)]})
            continue
        dsn = sandbox_dsn(db, dsn, out)
        out.say(f"isotest selfcheck on {version}: serial runs and fixed variants must be clean")
        for name in names:
            if name not in wls:
                raise UsageError(f"unknown workload {name!r}; run `python -m isotest list`")
            cfg = RunConfig(workload=name, db=db, dsn=dsn, n_workers=args.workers,
                            mode="step", timeout_s=args.timeout, db_version=version)
            problems: List[str] = []
            row = {"db": db, "workload": name, "serial": "", "control": "", "adversarial": ""}
            try:
                cal = calibrate(cfg)
                cfg.stagger_ms = cal.stagger_ms
                serial = run_n(cfg, Schedule.empty(), args.trials)
                k = sum(tr.bug_found for tr in serial)
                row["serial"] = _rate(k, args.trials)
                if k:
                    problems.append("buggy variant fails in SERIAL execution: "
                                    + next(t.bug_reason for t in serial if t.bug_found))
                control = [cfg.run(Schedule.empty(), variant="fixed", stagger_ms=0.0)
                           for _ in range(args.trials)]
                k = sum(tr.bug_found for tr in control)
                row["control"] = _rate(k, args.trials)
                if k:
                    problems.append("fixed variant fails with all workers racing: "
                                    + next(t.bug_reason for t in control if t.bug_found))
                if args.adversarial:
                    cal_fixed = calibrate(cfg, variant="fixed")
                    cfg.stagger_ms = max(cal.stagger_ms, cal_fixed.stagger_ms)
                    srows = sweep(cfg, cal_fixed, pause_ms=3 * cfg.stagger_ms, trials=1,
                                  variant="fixed", out=out, include_baseline=False)
                    bad = [r for r in srows if r["bugs"]]
                    row["adversarial"] = f"{len(bad)}/{len(srows)}"
                    if bad:
                        problems.append(f"fixed variant broken by a single pause at {bad[0]['event']}: "
                                        f"{bad[0]['reason']}")
            except HarnessError as e:
                problems.append(f"HARNESS ERROR: {e}")
            except Exception as e:  # keep checking the other workloads
                problems.append(f"{type(e).__name__}: {str(e).strip().splitlines()[0] if str(e).strip() else ''}")
            row["ok"] = not problems
            row["problems"] = problems
            rows.append(row)
            status = out.style.green("ok") if not problems else out.style.red("PROBLEM")
            adv = f" adversarial(fixed) {row['adversarial']:>6s}" if args.adversarial else ""
            out.say(f"  {name:20s} serial(buggy) {row['serial']:>4s}  race(fixed) {row['control']:>4s}"
                    f"{adv}  {status}")
            for p in problems:
                out.say(f"      - {p}")
    bad = [r for r in rows if not r.get("ok")]
    out.say("")
    out.say(out.style.green("selfcheck passed.") if not bad else
            out.style.red(f"selfcheck found {len(bad)} problem(s)."))
    out.emit_json({"ok": not bad, "rows": rows})
    return EXIT_OK if not bad else EXIT_FAIL


def cmd_lab(args: argparse.Namespace, out: Out) -> int:
    steps = [
        ("1. Serial baseline", "the buggy code passes when requests do not overlap"),
        ("2. Stress", "random arrival jitter: does a normal concurrent test catch it?"),
        ("3. Explore", "schedule search finds an interleaving that breaks the invariant"),
        ("4. Minimize", "delta debugging: the fewest, shortest pauses that still break it"),
        ("5. Explain", "the interleaving, statement by statement"),
        ("6. Verify the fix", "replay the exact schedule against buggy and fixed code"),
        ("7. Challenge", "is the plausible 'naive' fix enough? (workloads that ship one)"),
        ("8. Regression test", "pin the artifact in CI"),
    ]
    if args.dry_run:
        out.say(f"DRY RUN -- isotest lab on {args.db} with workload {args.workload}")
        for title, what in steps:
            out.say(f"  {title:22s} {what}")
        return EXIT_OK

    def pause() -> None:
        if args.pause and sys.stdin.isatty():
            input(out.style.dim("  [enter] next step "))

    cfg = config_from_args(args)
    out_dir = Path(args.out_dir) / "lab"
    out.say(out.style.bold(f"isotest lab -- {cfg.workload} on {cfg.db_version}"))
    meta = load_registry()[1].get(cfg.workload)
    if meta is not None:
        out.say(f"  anomaly class: {meta.bug_class} (typical level: {meta.isolation_level}); "
                f"cf. {meta.historical_ref}")
    cal = calibrate(cfg)
    cfg.stagger_ms = cal.stagger_ms

    out.heading(f"{steps[0][0]}: {steps[0][1]}")
    out.say(f"  $ python -m isotest selfcheck --workloads {cfg.workload} --db {cfg.db}"
            f"   # 5 runs, no pauses, workers start {cfg.stagger_ms:.0f} ms apart")
    serial = run_n(cfg, Schedule.empty(), 5)
    k = sum(tr.bug_found for tr in serial)
    out.say(f"  oracle fired in {k}/5 serial runs" + (" -- clean, as expected." if not k else
                                                     out.style.red(" -- unexpected! run selfcheck.")))
    out.say(_timeline(out, serial[0].trace, cfg.n_workers, verdict=serial[0].bug_reason,
                      is_bug=serial[0].bug_found))
    pause()

    out.heading(f"{steps[1][0]}: {steps[1][1]}")
    out.say(f"  $ python -m isotest stress --workload {cfg.workload} --db {cfg.db} --trials 30")
    st_args = argparse.Namespace(**{**vars(args), "trials": 30, "jitter_ms": None, "seed": args.seed,
                                    "timeline": False, "variant": "buggy"})
    cmd_stress(st_args, out)
    pause()

    out.heading(f"{steps[2][0]}: {steps[2][1]}")
    out.say(f"  $ python -m isotest explore --workload {cfg.workload} --db {cfg.db}")
    summary = explore(cfg, out, trials=args.trials, seconds=None, seed=args.seed, replays=5,
                      min_rate=0.8, out_dir=out_dir, full=False, calibration=cal)
    if not summary["best_artifact"]:
        out.say(out.style.red("  explore found nothing within the budget; try --trials 400"))
        return EXIT_FAIL
    art_path = Path(summary["best_artifact"])
    art = ScheduleArtifact.load(art_path)
    pause()

    out.heading(f"{steps[3][0]}: {steps[3][1]}")
    out.say(f"  $ python -m isotest minimize {_rel(art_path)}")
    m, stats = minimize_artifact(art, cfg, out, replays=3, validate=10, margin=2.0)
    min_path = art_path.with_suffix(".min.json")
    m.save(min_path)
    out.say(out.style.bold(f"  {stats['original_points']} -> {stats['minimized_points']} delay point(s); "
                           f"{stats['original_total_ms']} -> {stats['minimized_total_ms']} ms; "
                           f"validated {stats['validation']}"))
    out.say(render_plan(m.schedule.entries, cfg.n_workers,
                        describe_events(TraceEvent.from_dict(d) for d in m.evidence_trace), style=out.style))
    pause()

    out.heading(f"{steps[4][0]}: {steps[4][1]}")
    out.say(f"  $ python -m isotest show {_rel(min_path)}")
    out.say(_timeline(out, m.evidence_trace, cfg.n_workers, verdict=m.bug_reason, is_bug=m.bug_found))
    if m.bug_category == BugCategory.SWALLOWED_ERROR:
        out.say(out.style.yellow(
            "  The database refused the stale write (see the error), but the code never checks "
            "exec() results and reports success anyway.  Re-run with --db postgres to see a true "
            "isolation anomaly from the same code."))
    pause()

    def attack(variant: str) -> bool:
        """Depth-1 sweep of a variant's *own* statements; True if it holds."""
        out.say(f"  $ python -m isotest sweep --workload {cfg.workload} --db {cfg.db} "
                f"--variant {variant} --workers {cfg.n_workers} --expect clean")
        cal_v = calibrate(cfg, variant=variant)
        rows = sweep(cfg, cal_v, pause_ms=3 * cfg.stagger_ms, trials=2, variant=variant,
                     out=out, include_baseline=False)
        broken = [r for r in rows if r["bugs"]]
        if broken:
            out.say(out.style.red(f"  {variant}: BROKEN by a single pause at {broken[0]['event']} "
                                  f"(before {broken[0]['label'] or broken[0]['event']}) -- "
                                  f"{broken[0]['reason']}"))
        else:
            out.say(out.style.green(f"  {variant}: survived all {len(rows)} single-pause attacks"))
        return not broken

    out.heading(f"{steps[5][0]}: {steps[5][1]}")
    out.say(f"  $ python -m isotest replay {_rel(min_path)} --control")
    ok, _rows = replay_report(m, cfg, out, n=10, variants=["buggy", "fixed"], show_timeline=False)
    out.say("  A replay is a regression check, not a proof: delay points name the q-th statement,")
    out.say("  so also attack the fix's own statements, one pause at a time:")
    ok = attack("fixed") and ok
    pause()

    out.heading(f"{steps[6][0]}: {steps[6][1]}")
    if "naive_fix" in cfg.wl.variants:
        out.say(f"  $ python -m isotest replay {_rel(min_path)} --variant naive_fix")
        replay_report(m, cfg, out, n=10, variants=["naive_fix"], show_timeline=False)
        out.say("  The bug's own schedule does not break it ... but that schedule pauses statements")
        out.say("  naive_fix does not have.  Attack naive_fix directly:")
        if not attack("naive_fix"):
            out.say("  Read its docstring: the guard reads rows that the UPDATE never locks.")
    else:
        out.say(f"  {cfg.workload} ships no naive_fix.  Try the challenge workload:")
        out.say("  $ python -m isotest lab --db postgres --workload doctors_on_call")
    pause()

    out.heading(f"{steps[7][0]}: {steps[7][1]}")
    out.say("  Commit the minimized artifact and run in CI (exit code 0 = bug still reproduces")
    out.say("  on the buggy variant AND the fixed variant stays clean under the same schedule;")
    out.say("  --min-rate tolerates rare timing misses on noisy runners, never a dirty fix):")
    out.say(f"  $ python -m isotest replay {_rel(min_path)} --control --n 20 --min-rate 0.9")
    out.say("")
    out.say(out.style.green("lab complete.") if ok else
            out.style.red("lab finished, but the fix check did not pass -- investigate above."))
    return EXIT_OK if ok else EXIT_FAIL


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _add_target(p: argparse.ArgumentParser, *, workload_default: Optional[str] = "bank_overdraft",
                workers_default: int = 2) -> None:
    p.add_argument("--workload", "-w", default=workload_default,
                   help="workload name (see `isotest list`); default %(default)s")
    p.add_argument("--db", choices=DBS, default="sqlite", help="database; default %(default)s")
    p.add_argument("--dsn", help="connection string (default: env POSTGRES_DSN / MYSQL_DSN / "
                                 "SQLITE_DSN, then local candidates)")
    p.add_argument("--workers", type=int, default=workers_default,
                   help="concurrent sessions; default %(default)s")
    p.add_argument("--timeout", type=float, default=15.0,
                   help="per-trial worker timeout in seconds; default %(default)s")


def _add_artifact_target(p: argparse.ArgumentParser) -> None:
    p.add_argument("artifact", help="path to a ScheduleArtifact JSON file")
    p.add_argument("--db", choices=DBS, help="override the artifact's database kind")
    p.add_argument("--dsn", help="connection string for the database")
    p.add_argument("--timeout", type=float, default=15.0,
                   help="per-trial worker timeout in seconds; default %(default)s")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m isotest",
        description="Find, explain, and pin down transaction-isolation bugs by controlling "
                    "*when* each SQL statement runs.",
        epilog="Start with:  python -m isotest doctor  &&  python -m isotest lab",
        # Python <= 3.11 prefix-matches *every* --option against the top-level
        # parser, including subcommand options: `replay x.json --n 5` failed
        # with "ambiguous option: --n could match --no-color, --no-sandbox".
        allow_abbrev=False,
    )
    parser.add_argument("--version", action="version", version=f"isotest {__version__}")
    parser.add_argument("--json", action="store_true", help="machine-readable output only")
    parser.add_argument("--ascii", action="store_true", help="ASCII-only timeline glyphs")
    parser.add_argument("--no-color", action="store_true", help="disable ANSI colors")
    parser.add_argument("--no-sandbox", action="store_true",
                        help="use the database's default schema instead of a private, "
                             "auto-dropped one (PostgreSQL/MySQL)")
    sub = parser.add_subparsers(dest="command", required=True, metavar="command")

    p = sub.add_parser("list", help="workloads, anomaly classes, and variants")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("doctor", help="check Python, drivers, and database reachability")
    p.add_argument("--db", choices=DBS, action="append",
                   help="treat this database as required (repeatable)")
    p.add_argument("--dsn", help="connection string to test (with a single --db)")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT))
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("explore", help="search schedules for a bug; save a replayable artifact")
    _add_target(p)
    p.add_argument("--variant", default="buggy",
                   help="buggy | fixed | naive_fix | path/to/file.py:function; default %(default)s")
    p.add_argument("--mode", default="step", choices=[m.value for m in InjectionMode],
                   help="delay injection mode; default %(default)s")
    p.add_argument("--trials", type=int, default=120, help="trial budget; default %(default)s")
    p.add_argument("--seconds", type=float, help="optional wall-clock budget")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--replays", type=int, default=5,
                   help="replays to measure each finding's reliability; default %(default)s")
    p.add_argument("--min-rate", type=float, default=0.8,
                   help="stop at the first finding with at least this replay rate")
    p.add_argument("--full", action="store_true", help="use the whole budget (coverage run)")
    p.add_argument("--expect", choices=["bug", "clean"], default="bug",
                   help="exit 0 if a bug is found (bug, default) / if none is found within "
                        "the budget (clean: validating a fix; stops at the first breaking schedule)")
    p.add_argument("--stagger-ms", type=float, help="override the calibrated baseline stagger")
    p.add_argument("--elle", metavar="PATH", help="also write an Elle/Jepsen EDN history")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT))
    p.add_argument("--dry-run", action="store_true", help="print the plan; no database needed")
    p.set_defaults(func=cmd_explore)

    p = sub.add_parser("replay", help="re-run an artifact; --control also checks the fixed variant")
    _add_artifact_target(p)
    p.add_argument("--n", type=int, default=10, help="replays per variant; default %(default)s")
    p.add_argument("--control", action="store_true",
                   help="also replay the fixed variant under the same schedule (expect clean)")
    p.add_argument("--variant", help="replay only this variant (fixed, naive_fix, file.py:fn)")
    p.add_argument("--timeline", action="store_true", help="print the interleaving per variant")
    p.add_argument("--min-rate", type=float, default=1.0,
                   help="fraction of replays that must reproduce the bug (default 1.0; CI on "
                        "noisy runners: 0.9).  Clean expectations are always strict.")
    p.add_argument("--dry-run", action="store_true", help="print the plan; no database needed")
    p.set_defaults(func=cmd_replay)

    p = sub.add_parser("minimize", help="shrink an artifact to the fewest/shortest pauses")
    _add_artifact_target(p)
    p.add_argument("--replays", type=int, default=3,
                   help="runs per candidate test (all must reproduce); default %(default)s")
    p.add_argument("--validate", type=int, default=10, help="final validation replays")
    p.add_argument("--margin", type=float, default=2.0,
                   help="keep delays at margin x the smallest passing value; default %(default)s")
    p.add_argument("--min-rate", type=float, default=0.8)
    p.add_argument("--out", help="output path (default: <artifact>.min.json)")
    p.add_argument("--dry-run", action="store_true", help="print the plan; no database needed")
    p.set_defaults(func=cmd_minimize)

    p = sub.add_parser("show", help="explain an artifact: delay plan + interleaving timeline")
    _add_artifact_target(p)
    p.add_argument("--run", action="store_true", help="execute once and show a fresh trace")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("sweep", help="depth-1 exhaustive search: one pause at every event")
    _add_target(p)
    p.add_argument("--variant", default="buggy")
    p.add_argument("--mode", default="step", choices=[m.value for m in InjectionMode])
    p.add_argument("--pause-ms", type=float, help="pause length (default 3x stagger)")
    p.add_argument("--stagger-ms", type=float)
    p.add_argument("--trials", type=int, default=2, help="runs per schedule; default %(default)s")
    p.add_argument("--expect", choices=["bug", "clean"],
                   help="exit 1 unless some (bug) / no (clean) schedule triggers the oracle")
    p.set_defaults(func=cmd_sweep)

    p = sub.add_parser("stress", help="baseline: random arrival jitter, no targeted pauses")
    _add_target(p)
    p.add_argument("--variant", default="buggy")
    p.add_argument("--mode", default="step", choices=[m.value for m in InjectionMode])
    p.add_argument("--trials", type=int, default=30)
    p.add_argument("--jitter-ms", type=float, help="max random start offset (default 4x stagger)")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--timeline", action="store_true", help="print the first failing interleaving")
    p.set_defaults(func=cmd_stress)

    p = sub.add_parser("selfcheck", help="harness health: serial runs and fixed variants must be clean")
    p.add_argument("--db", choices=DBS, action="append", help="database(s); default sqlite")
    p.add_argument("--dsn", help="connection string (with a single --db)")
    p.add_argument("--workloads", nargs="+", help="subset of workloads (default: all)")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--timeout", type=float, default=15.0)
    p.add_argument("--adversarial", action="store_true",
                   help="also attack each fixed variant with every single-pause schedule")
    p.set_defaults(func=cmd_selfcheck)

    p = sub.add_parser("lab", help="guided workshop run-through (8 steps)")
    _add_target(p)
    p.add_argument("--trials", type=int, default=200, help="explore budget; default %(default)s")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--pause", action="store_true", help="wait for Enter between steps (presenting)")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT))
    p.add_argument("--dry-run", action="store_true", help="print the steps; no database needed")
    p.set_defaults(func=cmd_lab, mode="step", variant="buggy")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    style = Style.detect(force_ascii=args.ascii, force_color=False if args.no_color else None)
    out = Out(json_mode=args.json, style=style)
    global _SANDBOX_ENABLED
    _SANDBOX_ENABLED = not args.no_sandbox
    try:
        return args.func(args, out)
    except UsageError as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"isotest: error: {e}", file=sys.stderr)
        return EXIT_ENV
    except HarnessError as e:
        print(f"isotest: harness error (not a finding): {e}", file=sys.stderr)
        return EXIT_ENV
    except KeyboardInterrupt:
        print("\nisotest: interrupted", file=sys.stderr)
        return 130
    finally:
        cleanup_sandboxes()
