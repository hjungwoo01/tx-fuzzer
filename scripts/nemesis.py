from __future__ import annotations

"""
Simple fault-injection "nemesis" for tx-fuzzer, inspired by Jepsen's ChaosMonkey.

The `ChaosMonkey` randomly applies faults to the PostgreSQL container `txfuzz-pg`:
  - pause / unpause
  - kill / restart
  - disconnect / reconnect from its networks

Usage:

    python scripts/nemesis.py --faults pause,kill,disconnect --interval-min 2 --interval-max 10

You can also import `ChaosMonkey` and run it in a background thread from other scripts.
"""

import argparse
import random
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Iterable, List


DEFAULT_CONTAINER = "txfuzz-pg"


def _run(cmd: List[str], quiet: bool = False) -> bool:
    """Run a shell command. On failure: log unless quiet, return False. Returns True on success."""
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as exc:
        if not quiet:
            print(f"[ChaosMonkey] Command failed: {' '.join(cmd)} (exit={exc.returncode})", file=sys.stderr)
        return False


def _inspect_networks(container: str) -> List[str]:
    """Return a list of network names the container is attached to."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{json .NetworkSettings.Networks}}", container],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return []

    import json

    try:
        data = json.loads(result.stdout.strip() or "{}")
    except Exception:
        return []

    return list(data.keys())


@dataclass
class ChaosConfig:
    container: str = DEFAULT_CONTAINER
    faults: List[str] = None
    interval_min: float = 2.0
    interval_max: float = 10.0

    def __post_init__(self) -> None:
        if self.faults is None:
            self.faults = ["pause", "kill", "disconnect"]


class ChaosMonkey:
    """
    Randomly injects faults against a Docker container (e.g. txfuzz-pg).

    Call `run(stop_event)` in a background thread.
    """

    def __init__(self, config: ChaosConfig) -> None:
        self.config = config
        self._valid_faults = {"pause", "kill", "disconnect"}
        self.config.faults = [f for f in self.config.faults if f in self._valid_faults]
        if not self.config.faults:
            raise ValueError("ChaosMonkey requires at least one valid fault: pause, kill, disconnect")

    def _do_pause(self) -> None:
        print("[ChaosMonkey] Applying fault: pause", file=sys.stderr)
        _run(["docker", "pause", self.config.container])
        

    def _do_kill(self) -> None:
        print("[ChaosMonkey] Applying fault: kill", file=sys.stderr)
        _run(["docker", "kill", self.config.container])
        

    def _do_disconnect(self) -> None:
        print("[ChaosMonkey] Applying fault: disconnect", file=sys.stderr)
        networks = _inspect_networks(self.config.container)
        if not networks:
            print("[ChaosMonkey] No networks to disconnect from.", file=sys.stderr)
            return
        
        for net in networks:
            _run(["docker", "network", "disconnect", net, self.config.container])

    def _recover(self) -> None:
        """Best-effort recovery from any fault."""
        print("[ChaosMonkey] Recovering container...", file=sys.stderr)
        
        _run(["docker", "unpause", self.config.container], quiet=True)
        
        _run(["docker", "start", self.config.container], quiet=True)
        
        networks = _inspect_networks(self.config.container)
        if not networks:
            _run(["docker", "network", "connect", "bridge", self.config.container])

    def run(self, stop_event: threading.Event) -> None:
        """Main loop: inject faults until `stop_event` is set."""
        while not stop_event.is_set():
            fault = random.choice(self.config.faults)
            delay = random.uniform(self.config.interval_min, self.config.interval_max)

            try:
                if fault == "pause":
                    self._do_pause()
                elif fault == "kill":
                    self._do_kill()
                elif fault == "disconnect":
                    self._do_disconnect()
                else:
                    print(f"[ChaosMonkey] Unknown fault type: {fault}", file=sys.stderr)

                time.sleep(delay)
            finally:
                self._recover()

            cooldown = random.uniform(self.config.interval_min, self.config.interval_max)
            time.sleep(cooldown)


def _parse_faults(value: str) -> List[str]:
    return [f.strip().lower() for f in value.split(",") if f.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="ChaosMonkey nemesis for tx-fuzzer (Docker-based faults).")
    parser.add_argument(
        "--container",
        default=DEFAULT_CONTAINER,
        help=f"Docker container name to target (default: {DEFAULT_CONTAINER})",
    )
    parser.add_argument(
        "--faults",
        default="pause,kill,disconnect",
        help="Comma-separated list of faults to use: pause, kill, disconnect.",
    )
    parser.add_argument(
        "--interval-min",
        type=float,
        default=2.0,
        help="Minimum fault duration / cooldown in seconds (default: 2.0).",
    )
    parser.add_argument(
        "--interval-max",
        type=float,
        default=10.0,
        help="Maximum fault duration / cooldown in seconds (default: 10.0).",
    )

    args = parser.parse_args()

    config = ChaosConfig(
        container=args.container,
        faults=_parse_faults(args.faults),
        interval_min=args.interval_min,
        interval_max=args.interval_max,
    )

    nemesis = ChaosMonkey(config)
    stop_event = threading.Event()

    print(
        f"[nemesis] Starting ChaosMonkey for container={config.container}, "
        f"faults={config.faults}, interval={config.interval_min}-{config.interval_max}s",
        file=sys.stderr,
    )

    try:
        nemesis.run(stop_event)
    except KeyboardInterrupt:
        print("\n[nemesis] Stopping ChaosMonkey...", file=sys.stderr)
        stop_event.set()

    return 0


if __name__ == "__main__":
    sys.exit(main())

