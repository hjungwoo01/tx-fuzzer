from __future__ import annotations

import importlib.util
import json
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

try:
    import psycopg2
except Exception:  # pragma: no cover - import check for CLI environments
    psycopg2 = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parent
DEFAULT_DSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"


@dataclass
class RunnerSummary:
    bug: str
    runner: str
    seed: int
    detected: bool
    detection_reason: str
    time_to_first_s: Optional[float]
    trials_to_first: Optional[int]
    trials_executed: int
    time_budget_s: float
    max_trials: int
    artifact_dir: str

    def to_row(self) -> Dict[str, Any]:
        row = asdict(self)
        if row["time_to_first_s"] is not None:
            row["time_to_first_s"] = round(float(row["time_to_first_s"]), 6)
        return row


class HistoryRecorder:
    def __init__(self) -> None:
        self._start = time.monotonic()
        self._lock = threading.Lock()
        self._events: List[Dict[str, Any]] = []

    def log(
        self,
        *,
        client: str,
        event: str,
        sql: str = "",
        status: str = "ok",
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "t_s": round(time.monotonic() - self._start, 6),
            "client": client,
            "event": event,
            "status": status,
        }
        if sql:
            payload["sql"] = sql
        if details:
            payload["details"] = details
        with self._lock:
            self._events.append(payload)

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._events)


def resolve_dsn(explicit: Optional[str]) -> str:
    return explicit or os.getenv("DATABASE_URL") or DEFAULT_DSN


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)


def write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def load_manifest(path: Optional[Path] = None) -> Dict[str, Any]:
    manifest_path = path or (ROOT / "manifest.yaml")
    data = read_yaml(manifest_path)
    bugs = data.get("bugs")
    if not isinstance(bugs, list) or not bugs:
        raise ValueError(f"Manifest has no bug entries: {manifest_path}")
    return data


def resolve_bug_entry(manifest: Dict[str, Any], bug_name: str) -> Dict[str, Any]:
    for entry in manifest.get("bugs", []):
        if entry.get("name") == bug_name:
            return entry
    raise KeyError(f"Unknown bug name: {bug_name}")


def bug_dir(entry: Dict[str, Any]) -> Path:
    rel = str(entry.get("path", entry["name"])).strip()
    return (ROOT / rel).resolve()


def load_checker_module(case_dir: Path):
    checker_path = case_dir / "checker.py"
    if not checker_path.exists():
        raise FileNotFoundError(f"Checker not found: {checker_path}")
    module_name = f"known_bugs_checker_{case_dir.name}"
    spec = importlib.util.spec_from_file_location(module_name, checker_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load checker: {checker_path}")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        missing = str(exc).split("'")[1] if "'" in str(exc) else str(exc)
        raise RuntimeError(
            f"Missing Python dependency {missing!r} while loading {checker_path}. "
            "Install requirements (e.g. `pip install -r requirements.txt`)."
        ) from exc
    if not hasattr(module, "run_trial") or not hasattr(module, "check_trial"):
        raise AttributeError(
            f"Checker must export run_trial(...) and check_trial(...): {checker_path}"
        )
    return module


def apply_schema_and_setup(dsn: str, schema_sql: Path, setup_sql: Path) -> None:
    if psycopg2 is None:
        raise RuntimeError(
            "psycopg2 is required. Install dependencies, e.g. `pip install psycopg2-binary`."
        )
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql.read_text(encoding="utf-8"))
            cur.execute(setup_sql.read_text(encoding="utf-8"))
    finally:
        conn.close()


def sqlstate_from_exception(exc: BaseException) -> Optional[str]:
    code = getattr(exc, "pgcode", None)
    if code:
        return str(code)
    diag = getattr(exc, "diag", None)
    if diag is not None:
        state = getattr(diag, "sqlstate", None)
        if state:
            return str(state)
    return None


def normalize_schedule(schedule_ms: List[List[float]]) -> List[List[float]]:
    out: List[List[float]] = []
    for row in schedule_ms:
        out.append([max(0.0, float(v)) for v in row])
    return out


def clamp_ms(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def relative_case_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)
