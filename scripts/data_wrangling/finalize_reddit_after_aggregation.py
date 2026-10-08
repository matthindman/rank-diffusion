#!/usr/bin/env python3
"""Wait for monthly aggregation, then build and validate full Reddit panels."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from aggregate_reddit_monthly import validate_output


def pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def expected_months(start: str, end: str) -> list[str]:
    import pandas as pd

    return [str(month) for month in pd.period_range(start, end, freq="M")]


def validate_monthlies(ssd_root: Path, start: str, end: str) -> None:
    failures = []
    for kind in ["submissions", "comments"]:
        for month in expected_months(start, end):
            path = (
                ssd_root
                / "aggregates"
                / "reddit"
                / "monthly"
                / kind
                / f"{kind}_{month}.parquet"
            )
            ok, message = validate_output(path, kind)
            if not ok:
                failures.append(f"{path}: {message}")
    if failures:
        joined = "\n".join(failures)
        raise RuntimeError(f"monthly aggregate validation failed:\n{joined}")


def run_logged(command: list[str], output_path: Path | None = None) -> None:
    print(f"RUN {' '.join(command)}", flush=True)
    if output_path is None:
        subprocess.run(command, check=True)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as output:
        subprocess.run(command, check=True, stdout=output, stderr=subprocess.STDOUT)
    print(f"WROTE {output_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregation-pid", type=int, required=True)
    parser.add_argument("--ssd-root", type=Path, default=Path("/Volumes/T9/rank-diffusion-data"))
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--start", default="2018-12")
    parser.add_argument("--end", default="2022-12")
    parser.add_argument("--poll-seconds", type=int, default=600)
    parser.add_argument("--detach", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.detach:
        if args.dry_run:
            raise SystemExit("--detach and --dry-run cannot be used together")
        log_dir = args.ssd_root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "reddit_finalizer.stdout.log"
        pid_path = log_dir / "reddit_finalizer.pid"
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--aggregation-pid",
            str(args.aggregation_pid),
            "--ssd-root",
            str(args.ssd_root),
            "--repo-root",
            str(args.repo_root),
            "--start",
            args.start,
            "--end",
            args.end,
            "--poll-seconds",
            str(args.poll_seconds),
        ]
        with log_path.open("a", buffering=1) as output:
            process = subprocess.Popen(
                command,
                stdout=output,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                cwd=str(args.repo_root),
            )
        pid_path.write_text(f"{process.pid}\n")
        print(f"Started Reddit finalizer PID {process.pid}")
        print(f"PID file: {pid_path}")
        print(f"Log file: {log_path}")
        return

    python = sys.executable
    build_command = [
        python,
        str(args.repo_root / "scripts" / "data_wrangling" / "build_reddit_panels.py"),
        "--ssd-root",
        str(args.ssd_root),
        "--start",
        args.start,
        "--end",
        args.end,
        "--require-complete",
    ]
    validate_command = [
        python,
        str(args.repo_root / "scripts" / "data_wrangling" / "validate_reddit_outputs.py"),
        "--ssd-root",
        str(args.ssd_root),
        "--repo-root",
        str(args.repo_root),
    ]
    test_command = [python, "-m", "pytest", "tests/", "-q"]
    if args.dry_run:
        print(f"Would wait for PID {args.aggregation_pid}")
        print(f"Would validate {2 * len(expected_months(args.start, args.end))} monthly aggregates")
        print(f"Would run: {' '.join(build_command)}")
        print(f"Would run: {' '.join(validate_command)}")
        print(f"Would run: {' '.join(test_command)}")
        return

    while pid_running(args.aggregation_pid):
        print(f"Aggregation PID {args.aggregation_pid} still running; sleeping {args.poll_seconds}s", flush=True)
        time.sleep(args.poll_seconds)

    print("Aggregation process exited; validating monthly outputs", flush=True)
    validate_monthlies(args.ssd_root, args.start, args.end)
    print("All monthly submission and comment aggregates validated", flush=True)
    run_logged(build_command)
    run_logged(validate_command, args.ssd_root / "logs" / "reddit_full_validation.json")
    run_logged(test_command, args.ssd_root / "logs" / "reddit_full_pytest.log")
    print("Reddit panel finalization complete", flush=True)


if __name__ == "__main__":
    main()
