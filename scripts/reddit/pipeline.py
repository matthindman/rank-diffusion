#!/usr/bin/env python3
"""
Orchestrator: process all Pushshift .zst files from an external drive.

Scans for RC_*.zst and RS_*.zst files in the given source directory,
processes each one via process_month.py logic, tracks progress in a
manifest.json for resumability, and optionally runs the merge step.

Usage:
    # Process all .zst files found on an external drive
    python pipeline.py /Volumes/EXTERNAL/reddit/

    # Process only a specific date range
    python pipeline.py /Volumes/EXTERNAL/reddit/ --start 2020-01 --end 2024-12

    # Process and then merge into final output
    python pipeline.py /Volumes/EXTERNAL/reddit/ --merge

    # Dry run: show what would be processed
    python pipeline.py /Volumes/EXTERNAL/reddit/ --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from process_month import detect_type, process_zst_file, aggregates_to_dataframe


def load_manifest(manifest_path: Path) -> dict:
    if manifest_path.exists():
        return json.loads(manifest_path.read_text())
    return {"completed": {}}


def save_manifest(manifest_path: Path, manifest: dict) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))


def find_zst_files(
    source_dir: Path,
    start: str | None,
    end: str | None,
) -> list[Path]:
    """Find and filter .zst files by date range."""
    pattern = re.compile(r"R[CS]_(\d{4}-\d{2})\.zst$", re.IGNORECASE)
    files = []

    # Search recursively (torrents may have subdirectories)
    for f in sorted(source_dir.rglob("*.zst")):
        m = pattern.search(f.name)
        if not m:
            continue
        month = m.group(1)
        if start and month < start:
            continue
        if end and month > end:
            continue
        files.append(f)

    return files


def process_one_file(
    zst_path: Path,
    output_dir: Path,
) -> dict:
    """Process a single .zst file and return result metadata."""
    record_type = detect_type(zst_path.name)
    month_match = re.search(r"(\d{4}-\d{2})", zst_path.name)
    month = month_match.group(1) if month_match else "unknown"

    output_path = output_dir / f"{record_type}_{month}.parquet"

    print(f"\n{'='*70}")
    print(f"Processing: {zst_path.name}")
    print(f"  Type: {record_type} | Month: {month}")
    print(f"  Size: {zst_path.stat().st_size / 1e9:.2f} GB")
    print(f"  Output: {output_path}")
    print()

    t0 = time.monotonic()
    aggregates, lines_processed, errors = process_zst_file(str(zst_path), record_type)
    elapsed = time.monotonic() - t0

    result = {
        "lines": lines_processed,
        "errors": errors,
        "pairs": len(aggregates),
        "elapsed_minutes": round(elapsed / 60, 1),
        "output": str(output_path),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    if not aggregates:
        print(f"  WARNING: no data extracted from {zst_path.name}")
        result["status"] = "empty"
        return result

    df = aggregates_to_dataframe(aggregates)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"\n  Done in {elapsed/60:.1f} min")
    print(f"  Lines: {lines_processed:,} | Errors: {errors:,}")
    print(f"  Unique (subreddit, day): {len(aggregates):,}")
    print(f"  Parquet: {output_path.stat().st_size / 1e6:.1f} MB")

    result["status"] = "processed"
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Process all Pushshift .zst files from a source directory."
    )
    parser.add_argument(
        "source_dir",
        help="Directory containing .zst files (e.g., external drive path)",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="Start month YYYY-MM (inclusive, default: no lower bound)",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End month YYYY-MM (inclusive, default: no upper bound)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for monthly parquets",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Run merge step after processing all files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without doing it",
    )
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    if not source_dir.exists():
        print(f"Error: source directory not found: {source_dir}", file=sys.stderr)
        sys.exit(1)

    repo_root = Path(__file__).resolve().parent.parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else repo_root / "data" / "reddit" / "monthly"
    manifest_path = repo_root / "data" / "reddit" / "manifest.json"

    # Find files
    files = find_zst_files(source_dir, args.start, args.end)
    if not files:
        print(f"No .zst files found in {source_dir}", file=sys.stderr)
        if args.start or args.end:
            print(f"  (filtered to {args.start or '*'} through {args.end or '*'})")
        sys.exit(1)

    # Load manifest
    manifest = load_manifest(manifest_path)

    # Determine what needs processing
    to_process = []
    already_done = []
    for f in files:
        key = f.name  # e.g., RC_2020-01.zst
        if key in manifest["completed"] and manifest["completed"][key].get("status") == "processed":
            already_done.append(f)
        else:
            to_process.append(f)

    total_size_gb = sum(f.stat().st_size for f in to_process) / 1e9

    print(f"Source: {source_dir}")
    print(f"Output: {output_dir}")
    print(f"Date range: {args.start or '*'} to {args.end or '*'}")
    print(f"Total .zst files found: {len(files)}")
    print(f"Already processed: {len(already_done)}")
    print(f"To process: {len(to_process)} ({total_size_gb:.1f} GB)")

    if args.dry_run:
        print("\nFiles to process:")
        for f in to_process:
            print(f"  {f.name} ({f.stat().st_size / 1e9:.2f} GB)")
        if already_done:
            print(f"\nAlready done ({len(already_done)}):")
            for f in already_done:
                print(f"  {f.name}")
        return

    if not to_process:
        print("\nAll files already processed.")
        if args.merge:
            print("Running merge step...")
            run_merge(repo_root, output_dir)
        return

    # Process files
    pipeline_t0 = time.monotonic()
    successes = 0
    failures = 0

    for i, zst_path in enumerate(to_process, 1):
        print(f"\n[{i}/{len(to_process)}] ", end="")

        try:
            result = process_one_file(zst_path, output_dir)
            manifest["completed"][zst_path.name] = result
            save_manifest(manifest_path, manifest)

            if result.get("status") == "processed":
                successes += 1
            else:
                failures += 1

        except Exception as e:
            print(f"  ERROR processing {zst_path.name}: {e}", file=sys.stderr)
            manifest["completed"][zst_path.name] = {
                "status": "error",
                "error": str(e),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            save_manifest(manifest_path, manifest)
            failures += 1

    pipeline_elapsed = time.monotonic() - pipeline_t0

    print(f"\n{'='*70}")
    print(f"Pipeline complete in {pipeline_elapsed/3600:.1f} hours")
    print(f"  Successes: {successes}")
    print(f"  Failures: {failures}")

    if args.merge and successes > 0:
        print("\nRunning merge step...")
        run_merge(repo_root, output_dir)


def run_merge(repo_root: Path, monthly_dir: Path) -> None:
    """Run the merge script."""
    import subprocess

    merge_script = repo_root / "scripts" / "reddit" / "merge.py"
    cmd = [
        sys.executable,
        str(merge_script),
        "--monthly-dir",
        str(monthly_dir),
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
