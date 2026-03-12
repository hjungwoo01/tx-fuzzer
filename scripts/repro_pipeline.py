#!/usr/bin/env python3
"""
Repro Pipeline — unified concurrency-bug discovery + minimization + replay.

Usage:
  python scripts/repro_pipeline.py --db sqlite  --pack sqlite_narrow_dirty --mode smoke
  python scripts/repro_pipeline.py --db mysql   --pack mysql_rr_phantom    --mode quick
  python scripts/repro_pipeline.py --db postgres --pack pg_update_conflict_out --mode quick
  python scripts/repro_pipeline.py --list-packs
  python scripts/repro_pipeline.py --db sqlite --pack sqlite_narrow_dirty --mode quick --replay-only

Pipeline steps:
  1. Setup schema
  2. Discovery: run trials with stress/random/heuristic strategies
  3. Replay verification: 20x on buggy config
  4. Control verification: 20x on fixed config
  5. Generate report artifacts
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import pack registry — this triggers registration of all headline packs
from scripts.repro_pipeline.bug_pack import (
    BugPack, PackConfig, TrialResult, ReplayResult,
    SMOKE_CONFIG, QUICK_CONFIG, LONG_CONFIG,
    get_pack, list_packs,
)
import scripts.repro_pipeline.packs  # noqa: F401 — side-effect: registers packs
from scripts.repro_pipeline.template_runner import run_template_discovery


STRATEGIES = ["stress", "random", "heuristic"]


def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def run_discovery_phase(
    pack: BugPack,
    dsn: str,
    cfg: PackConfig,
    output_dir: Path,
) -> List[Dict[str, Any]]:
    """Run discovery for all strategies × reps.

    Returns list of per-run result dicts.
    """
    results: List[Dict[str, Any]] = []
    for strat in STRATEGIES:
        for rep in range(cfg.reps):
            label = f"{pack.id}/{strat}/rep{rep+1}"
            print(f"  Discovery: {label}")
            t0 = time.monotonic()
            disc = pack.run_discovery(dsn, strat, cfg, rep=rep)
            elapsed = time.monotonic() - t0

            status = "BUG" if disc["bug_found"] else "clean"
            ttf = disc.get("time_to_first_s")
            ttf_str = f"{ttf:.2f}s" if ttf is not None else "-"
            print(f"    {status} (ttf={ttf_str}, trials={disc['total_trials']}, elapsed={elapsed:.1f}s)")

            result = {
                "pack": pack.id,
                "db": pack.db,
                "strategy": strat,
                "rep": rep,
                "bug_found": disc["bug_found"],
                "time_to_first_s": disc.get("time_to_first_s"),
                "trials_to_first": disc.get("trials_to_first"),
                "total_trials": disc["total_trials"],
                "total_time_s": disc["total_time_s"],
                "control": False,
            }
            results.append(result)

            # Save artifact if bug found
            if disc["bug_found"] and disc.get("found_result"):
                artifact_dir = _ensure_dir(output_dir / "artifacts" / pack.id)
                artifact = {
                    "pack": pack.id,
                    "strategy": strat,
                    "rep": rep,
                    "bug_found": True,
                    "reason": disc["found_result"].reason,
                    "delays_used": disc["found_result"].delays_used,
                    "extra": disc["found_result"].extra,
                    "time_to_first_s": disc["time_to_first_s"],
                }
                apath = artifact_dir / f"discovery_{strat}_rep{rep}.json"
                apath.write_text(json.dumps(artifact, indent=2, default=str), encoding="utf-8")

    return results


def run_replay_phase(
    pack: BugPack,
    cfg: PackConfig,
    output_dir: Path,
) -> Dict[str, Any]:
    """Run 20x replay on buggy + control configs.

    Returns dict with buggy_replay and control_replay results.
    """
    n = cfg.replay_count
    result: Dict[str, Any] = {}

    # Buggy replay
    buggy_dsn = pack.buggy_dsn()
    print(f"  Replay buggy: {n}x on {pack.id}")
    if pack.is_available(buggy_dsn):
        try:
            pack.setup(buggy_dsn)
        except Exception:
            pass
        buggy_replay = pack.run_replay(buggy_dsn, n)
        print(f"    buggy: {buggy_replay.bug_count}/{buggy_replay.total}")
        result["buggy_replay"] = {
            "bug_count": buggy_replay.bug_count,
            "total": buggy_replay.total,
            "details": buggy_replay.details,
        }

        # Save artifact
        artifact_dir = _ensure_dir(output_dir / "artifacts" / pack.id)
        (artifact_dir / "replay_buggy.json").write_text(
            json.dumps(result["buggy_replay"], indent=2, default=str),
            encoding="utf-8",
        )
    else:
        print(f"    SKIP buggy replay (not available)")
        result["buggy_replay"] = {"bug_count": 0, "total": 0, "skipped": True}

    # Control replay
    control_dsn = pack.control_dsn()
    print(f"  Replay control: {n}x on {pack.id}")
    if pack.is_available(control_dsn):
        try:
            pack.setup(control_dsn)
        except Exception:
            pass
        if hasattr(pack, "run_control_replay"):
            ctrl_replay = pack.run_control_replay(control_dsn, n)
        else:
            ctrl_replay = pack.run_replay(control_dsn, n)
        print(f"    control: {ctrl_replay.bug_count}/{ctrl_replay.total}")
        result["control_replay"] = {
            "bug_count": ctrl_replay.bug_count,
            "total": ctrl_replay.total,
            "details": ctrl_replay.details,
        }

        (artifact_dir / "replay_control.json").write_text(
            json.dumps(result["control_replay"], indent=2, default=str),
            encoding="utf-8",
        )
    else:
        print(f"    SKIP control replay (not available)")
        result["control_replay"] = {"bug_count": 0, "total": 0, "skipped": True}

    return result


def generate_pack_report(
    pack: BugPack,
    discovery_results: List[Dict[str, Any]],
    replay_results: Dict[str, Any],
    output_dir: Path,
) -> None:
    """Generate per-pack markdown report."""
    lines = [
        f"# Repro Pack: {pack.id}",
        "",
        f"**DB**: {pack.db}",
        f"**Description**: {pack.description}",
        "",
        "## Discovery Results",
        "",
        "| Strategy | Rep | Bug Found | TTF (s) | Trials | Total Time |",
        "|----------|----:|-----------|--------:|-------:|-----------:|",
    ]

    for r in discovery_results:
        ttf = f"{r['time_to_first_s']:.2f}" if r["time_to_first_s"] is not None else "-"
        lines.append(
            f"| {r['strategy']} | {r['rep']} | "
            f"{'YES' if r['bug_found'] else 'no'} | {ttf} | "
            f"{r['total_trials']} | {r['total_time_s']:.1f}s |"
        )

    # Success rates by strategy
    lines.extend(["", "### Success Rates", ""])
    for strat in STRATEGIES:
        strat_results = [r for r in discovery_results if r["strategy"] == strat]
        found = sum(1 for r in strat_results if r["bug_found"])
        total = len(strat_results)
        rate = f"{found}/{total}" if total > 0 else "N/A"
        pct = f"{100*found/total:.0f}%" if total > 0 else "N/A"
        lines.append(f"- **{strat}**: {rate} ({pct})")

        if found > 0:
            ttfs = [r["time_to_first_s"] for r in strat_results if r["bug_found"] and r["time_to_first_s"] is not None]
            if ttfs:
                median_ttf = sorted(ttfs)[len(ttfs)//2]
                lines.append(f"  - Median TTF: {median_ttf:.2f}s")

    # Replay
    lines.extend(["", "## Replay Verification", ""])
    br = replay_results.get("buggy_replay", {})
    cr = replay_results.get("control_replay", {})
    lines.append(f"- **Buggy replay**: {br.get('bug_count', 'N/A')}/{br.get('total', 'N/A')}")
    lines.append(f"- **Control replay**: {cr.get('bug_count', 'N/A')}/{cr.get('total', 'N/A')}")

    if cr.get("bug_count", 0) > 0:
        lines.append("")
        lines.append("**WARNING**: False positives detected in control!")

    report_path = output_dir / "artifacts" / pack.id / "report.md"
    _ensure_dir(report_path.parent)
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Wrote {report_path}")


def generate_summary(
    all_results: List[Dict[str, Any]],
    all_replays: Dict[str, Dict[str, Any]],
    output_dir: Path,
) -> None:
    """Generate top-level summary.csv and report.md."""
    # CSV
    csv_path = output_dir / "summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "pack", "db", "strategy", "rep", "bug_found",
            "time_to_first_s", "trials_to_first", "total_trials",
            "total_time_s", "control",
        ])
        for r in all_results:
            w.writerow([
                r["pack"], r["db"], r["strategy"], r["rep"], r["bug_found"],
                r.get("time_to_first_s", ""), r.get("trials_to_first", ""),
                r["total_trials"], f"{r['total_time_s']:.2f}", r["control"],
            ])
    print(f"Wrote {csv_path}")

    # Summary markdown
    lines = [
        "# Repro Pipeline — Summary Report",
        "",
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}",
        "",
        "## Discovery Results",
        "",
        "| Pack | DB | Strategy | Reps | Success Rate | Median TTF |",
        "|------|-----|----------|-----:|--------------|------------|",
    ]

    packs_seen = sorted(set(r["pack"] for r in all_results))
    for pack_id in packs_seen:
        for strat in STRATEGIES:
            strat_results = [
                r for r in all_results
                if r["pack"] == pack_id and r["strategy"] == strat and not r["control"]
            ]
            if not strat_results:
                continue
            found = sum(1 for r in strat_results if r["bug_found"])
            total = len(strat_results)
            rate = f"{found}/{total}"
            db = strat_results[0]["db"]
            ttfs = [r["time_to_first_s"] for r in strat_results if r["bug_found"] and r.get("time_to_first_s") is not None]
            median_ttf = f"{sorted(ttfs)[len(ttfs)//2]:.2f}s" if ttfs else "-"
            lines.append(f"| {pack_id} | {db} | {strat} | {total} | {rate} | {median_ttf} |")

    lines.extend(["", "## Replay Verification", ""])
    for pack_id in packs_seen:
        rpl = all_replays.get(pack_id, {})
        br = rpl.get("buggy_replay", {})
        cr = rpl.get("control_replay", {})
        lines.append(f"**{pack_id}**:")
        lines.append(f"- Buggy: {br.get('bug_count', 'N/A')}/{br.get('total', 'N/A')}")
        lines.append(f"- Control: {cr.get('bug_count', 'N/A')}/{cr.get('total', 'N/A')}")
        lines.append("")

    # Controls check
    lines.extend(["## Controls Check", ""])
    any_fp = False
    for pack_id in packs_seen:
        rpl = all_replays.get(pack_id, {})
        cr = rpl.get("control_replay", {})
        if cr.get("bug_count", 0) > 0:
            lines.append(f"**WARNING**: {pack_id} — {cr['bug_count']} false positive(s) in control!")
            any_fp = True
    if not any_fp:
        lines.append("All controls: 0 false positives.")

    report_path = output_dir / "report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {report_path}")


def run_pipeline(
    pack: BugPack,
    mode: str,
    output_dir: Path,
    replay_only: bool = False,
    dsn_override: Optional[str] = None,
) -> int:
    """Execute the full repro pipeline for a single pack."""
    cfg = {"smoke": SMOKE_CONFIG, "quick": QUICK_CONFIG, "long": LONG_CONFIG}[mode]
    dsn = dsn_override or pack.buggy_dsn()

    print(f"\n{'='*60}")
    print(f"Repro Pipeline: {pack.id} ({pack.db})")
    print(f"Mode: {mode} | DSN: {dsn}")
    print(f"Config: duration={cfg.duration_s}s, reps={cfg.reps}, "
          f"max_trials={cfg.max_trials}, replay={cfg.replay_count}")
    print(f"{'='*60}")

    # Check availability
    if not pack.is_available(dsn):
        print(f"ERROR: {pack.db} not available at {dsn}")
        return 1

    # Setup
    print(f"\n--- Setup ---")
    try:
        pack.setup(dsn)
        print("  Schema ready.")
    except Exception as e:
        print(f"  Setup error: {e}")
        return 1

    discovery_results: List[Dict[str, Any]] = []

    if not replay_only:
        # Discovery
        print(f"\n--- Discovery ---")
        discovery_results = run_discovery_phase(pack, dsn, cfg, output_dir)

    # Replay
    print(f"\n--- Replay Verification ---")
    replay_results = run_replay_phase(pack, cfg, output_dir)

    # Report
    print(f"\n--- Report ---")
    generate_pack_report(pack, discovery_results, replay_results, output_dir)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repro Pipeline — concurrency-bug discovery + replay CLI",
    )
    parser.add_argument("--list-packs", action="store_true",
                        help="List all registered bug packs")
    parser.add_argument("--db", choices=["postgres", "mysql", "sqlite"],
                        help="Target DB engine")
    parser.add_argument("--pack", help="Bug pack ID (e.g. sqlite_narrow_dirty)")
    parser.add_argument("--mode", choices=["smoke", "quick", "long"], default="smoke",
                        help="Run mode (default: smoke)")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Output directory")
    parser.add_argument("--dsn", default=None, help="Override DSN for buggy variant")
    parser.add_argument("--replay-only", action="store_true",
                        help="Skip discovery, only run replay verification")
    parser.add_argument("--all", action="store_true",
                        help="Run all available packs (optionally filtered by --db)")
    parser.add_argument("--template-scan", action="store_true",
                        help="Run general-template discovery (seed_gen → slicer → fuzz → minimize)")
    parser.add_argument("--n-seeds", type=int, default=10,
                        help="Number of seed traces for --template-scan (default: 10)")
    parser.add_argument("--no-minimize", action="store_true",
                        help="Skip minimization on template-scan hits")
    args = parser.parse_args()

    if args.list_packs:
        packs = list_packs(db=args.db)
        if not packs:
            print("No packs registered" + (f" for db={args.db}" if args.db else ""))
            return 0
        print(f"{'ID':<30s} {'DB':<10s} Description")
        print("-" * 80)
        for p in packs:
            print(f"{p.id:<30s} {p.db:<10s} {p.description[:50]}")
        return 0

    if args.all:
        packs = list_packs(db=args.db)
        if not packs:
            print("No packs available")
            return 1

        ts = time.strftime("%Y%m%d_%H%M%S")
        output_dir = args.output_dir or (ROOT / "results" / f"repro_pipeline_{ts}")
        _ensure_dir(output_dir)

        all_results: List[Dict[str, Any]] = []
        all_replays: Dict[str, Dict[str, Any]] = {}
        exit_code = 0

        for pack in packs:
            pack_dir = _ensure_dir(output_dir / pack.id)
            if not pack.is_available(pack.buggy_dsn()):
                print(f"\nSKIP {pack.id} (not available)")
                continue
            rc = run_pipeline(pack, args.mode, output_dir,
                              replay_only=args.replay_only,
                              dsn_override=args.dsn)
            if rc != 0:
                exit_code = 1

            # Collect results from artifacts
            artifact_dir = output_dir / "artifacts" / pack.id
            for f in sorted(artifact_dir.glob("discovery_*.json")):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    all_results.append({
                        "pack": pack.id, "db": pack.db,
                        "strategy": data.get("strategy", ""),
                        "rep": data.get("rep", 0),
                        "bug_found": data.get("bug_found", False),
                        "time_to_first_s": data.get("time_to_first_s"),
                        "trials_to_first": None,
                        "total_trials": 0,
                        "total_time_s": data.get("time_to_first_s", 0),
                        "control": False,
                    })
                except Exception:
                    pass

            # Collect replay results
            buggy_path = artifact_dir / "replay_buggy.json"
            control_path = artifact_dir / "replay_control.json"
            replays: Dict[str, Any] = {}
            if buggy_path.exists():
                replays["buggy_replay"] = json.loads(buggy_path.read_text(encoding="utf-8"))
            if control_path.exists():
                replays["control_replay"] = json.loads(control_path.read_text(encoding="utf-8"))
            all_replays[pack.id] = replays

        generate_summary(all_results, all_replays, output_dir)
        print(f"\n=== ALL DONE === (output: {output_dir})")
        return exit_code

    if args.template_scan:
        if not args.db:
            parser.error("--db is required for --template-scan")
        dsn = args.dsn
        if not dsn:
            from scripts.experiments.db_adapter import DEFAULT_DSNS
            dsn = DEFAULT_DSNS.get(args.db, "")
        cfg = {"smoke": SMOKE_CONFIG, "quick": QUICK_CONFIG, "long": LONG_CONFIG}[args.mode]
        ts = time.strftime("%Y%m%d_%H%M%S")
        output_dir = args.output_dir or (ROOT / "results" / f"template_scan_{args.db}_{ts}")
        _ensure_dir(output_dir)

        print(f"\n{'='*60}")
        print(f"Template Scan: db={args.db}")
        print(f"Mode: {args.mode} | Seeds: {args.n_seeds} | Minimize: {not args.no_minimize}")
        print(f"{'='*60}")

        for strat in STRATEGIES:
            print(f"\n--- Strategy: {strat} ---")
            result = run_template_discovery(
                db=args.db, dsn=dsn, cfg=cfg, strategy=strat,
                n_seeds=args.n_seeds, do_minimize=not args.no_minimize,
            )
            if result.bug_found:
                print(f"  BUG FOUND: {result.anomaly_type}")
                print(f"  Details: {result.details}")
                artifact = {
                    "strategy": strat,
                    "anomaly_type": result.anomaly_type,
                    "details": result.details,
                    "delays": result.delays,
                }
                if result.minimized:
                    print(f"  Minimized: {result.minimized.original_event_count} → "
                          f"{result.minimized.minimized_event_count} events, "
                          f"{result.minimized.original_delay_count} → "
                          f"{result.minimized.minimized_delay_count} delays")
                    artifact["minimized_events"] = result.minimized.minimized_event_count
                    artifact["minimized_delays"] = result.minimized.minimized_delay_count
                apath = output_dir / f"template_hit_{strat}.json"
                apath.write_text(json.dumps(artifact, indent=2, default=str), encoding="utf-8")
            else:
                print(f"  clean ({result.details})")

        print(f"\n=== DONE === (output: {output_dir})")
        return 0

    if not args.pack:
        parser.error("--pack is required (or use --list-packs / --all / --template-scan)")

    pack = get_pack(args.pack)
    ts = time.strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or (ROOT / "results" / f"repro_{pack.id}_{ts}")
    _ensure_dir(output_dir)

    return run_pipeline(pack, args.mode, output_dir,
                        replay_only=args.replay_only,
                        dsn_override=args.dsn)


if __name__ == "__main__":
    sys.exit(main())
