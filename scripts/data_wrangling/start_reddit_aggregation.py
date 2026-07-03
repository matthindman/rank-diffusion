#!/usr/bin/env python3
"""Launch the Reddit monthly aggregation as a detached background job."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path

from phase2_common import ensure_layout, utc_now


def pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wd-root", default="/Volumes/My Passport for Mac")
    ap.add_argument("--ssd-root", default="/Volumes/T9/rank-diffusion-data")
    ap.add_argument("--start", default="2018-12")
    ap.add_argument("--end", default="2022-12")
    ap.add_argument("--progress-interval", type=int, default=5_000_000)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    ssd_root = Path(args.ssd_root)
    ensure_layout(ssd_root)
    log_dir = ssd_root / "logs"
    pid_path = log_dir / "reddit_monthly_aggregation.pid"
    stdout_path = log_dir / "reddit_monthly_aggregation.stdout.log"

    if pid_path.exists():
        try:
            old_pid = int(pid_path.read_text().strip())
        except ValueError:
            old_pid = -1
        if old_pid > 0 and pid_running(old_pid):
            raise SystemExit(f"Reddit aggregation already appears to be running as PID {old_pid}")

    script = Path(__file__).resolve().with_name("aggregate_reddit_monthly.py")
    cmd = [
        sys.executable,
        str(script),
        "--wd-root",
        args.wd_root,
        "--ssd-root",
        args.ssd_root,
        "--start",
        args.start,
        "--end",
        args.end,
        "--progress-interval",
        str(args.progress_interval),
    ]
    if args.force:
        cmd.append("--force")

    with stdout_path.open("a", buffering=1) as out:
        out.write(f"\n=== launch {utc_now()} ===\n")
        out.write(" ".join(cmd) + "\n")
        proc = subprocess.Popen(
            cmd,
            stdout=out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
    pid_path.write_text(f"{proc.pid}\n")
    print(f"Started Reddit aggregation PID {proc.pid}")
    print(f"PID file: {pid_path}")
    print(f"Log file: {stdout_path}")


if __name__ == "__main__":
    main()

