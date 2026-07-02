#!/usr/bin/env python3
"""
Stream and process the N most recent months from the PullPush webseed.

Streams each .zst file over HTTP (no local disk needed for the raw archives),
processes it into a daily subreddit aggregate parquet via process_month.py logic,
and tracks progress in a manifest so interrupted runs can resume.

Usage:
    # Stream the 6 most recent months (submissions + comments)
    python stream_recent.py

    # Only submissions
    python stream_recent.py --type submissions

    # Custom month count
    python stream_recent.py --months 3

    # Dry run
    python stream_recent.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from process_month import (
    WEBSEED_BASE,
    aggregates_to_dataframe,
    detect_month,
    detect_type,
    get_rss_mb,
    process_zst_stream,
)

# Map record type to the webseed subdirectory and file prefix.
TYPE_INFO = {
    "submissions": ("submissions", "RS"),
    "comments": ("comments", "RC"),
}


def probe_available_months(max_probe: int = 24) -> list[str]:
    """Walk backwards from the current month to find available archives."""
    from datetime import date

    today = date.today()
    year, month = today.year, today.month

    available: list[str] = []
    probed = 0

    while probed < max_probe:
        label = f"{year:04d}-{month:02d}"
        url = f"{WEBSEED_BASE}/submissions/RS_{label}.zst"
        try:
            req = urllib.request.Request(url, method="HEAD", headers={
                "User-Agent": "rankdiff-research/1.0",
            })
            ctx = _make_ssl_ctx()
            resp = urllib.request.urlopen(req, timeout=30, context=ctx)
            if resp.status == 200:
                available.append(label)
        except Exception:
            pass

        probed += 1
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return available


def _make_ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    try:
        import certifi
        ctx.load_verify_locations(certifi.where())
    except ImportError:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def build_job_list(
    months: list[str],
    types: list[str],
    output_dir: Path,
    manifest: dict,
) -> list[dict]:
    """Build the list of (url, output_path) jobs, skipping already-completed ones."""
    jobs = []
    for month in months:
        for rtype in types:
            subdir, prefix = TYPE_INFO[rtype]
            filename = f"{prefix}_{month}.zst"
            url = f"{WEBSEED_BASE}/{subdir}/{filename}"
            out_path = output_dir / f"{rtype}_{month}.parquet"
            key = filename

            if key in manifest.get("completed", {}) and manifest["completed"][key].get("status") == "processed":
                continue

            jobs.append({
                "month": month,
                "type": rtype,
                "filename": filename,
                "url": url,
                "output_path": out_path,
                "key": key,
            })
    return jobs


def stream_and_process(job: dict, progress_interval: int) -> dict:
    """Stream a single .zst file from HTTP and process it."""
    url = job["url"]
    output_path = job["output_path"]
    record_type = job["type"]
    month = job["month"]

    print(f"\n{'='*70}")
    print(f"Streaming: {job['filename']}")
    print(f"  URL:    {url}")
    print(f"  Type:   {record_type}")
    print(f"  Month:  {month}")
    print(f"  Output: {output_path}")
    print(f"  JSON parser: {'orjson' if 'orjson' in sys.modules else 'json (stdlib)'}")
    print(f"  RSS: {get_rss_mb():.0f} MB")

    ctx = _make_ssl_ctx()
    req = urllib.request.Request(url, headers={"User-Agent": "rankdiff-research/1.0"})
    response = urllib.request.urlopen(req, timeout=300, context=ctx)
    file_size = int(response.headers.get("Content-Length", 0))
    print(f"  Content-Length: {file_size / 1e9:.2f} GB")
    print()

    t0 = time.monotonic()
    aggregates, lines_processed, errors = process_zst_stream(
        response, file_size, progress_interval=progress_interval,
    )
    elapsed = time.monotonic() - t0

    result = {
        "lines": lines_processed,
        "errors": errors,
        "pairs": len(aggregates),
        "elapsed_minutes": round(elapsed / 60, 1),
        "output": str(output_path),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "source_url": url,
        "source_size_gb": round(file_size / 1e9, 2),
    }

    print()
    print(f"Done in {elapsed/60:.1f} minutes")
    print(f"  Lines processed: {lines_processed:,}")
    print(f"  Errors/skipped:  {errors:,}")
    print(f"  Unique (subreddit, day) pairs: {len(aggregates):,}")

    if not aggregates:
        print(f"  WARNING: no data extracted from {job['filename']}")
        result["status"] = "empty"
        return result

    df = aggregates_to_dataframe(aggregates)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    print(f"  Wrote: {output_path} ({output_path.stat().st_size / 1e6:.1f} MB)")

    print(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"  Unique subreddits: {df['subreddit'].nunique():,}")
    print(f"  Unique days: {df['date'].nunique()}")
    top = df.groupby("subreddit")["count"].sum().nlargest(5)
    print(f"  Top 5 subreddits by {record_type} count:")
    for sub, cnt in top.items():
        print(f"    r/{sub}: {cnt:,}")

    result["status"] = "processed"
    return result


def load_manifest(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {"completed": {}}


def save_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Stream and process recent PullPush archives."
    )
    parser.add_argument(
        "--months", type=int, default=6,
        help="Number of most-recent months to process (default: 6)",
    )
    parser.add_argument(
        "--type", dest="record_types", action="append",
        choices=["submissions", "comments"],
        help="Record type(s) to process (default: both). Can be repeated.",
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Output directory for monthly parquets",
    )
    parser.add_argument(
        "--progress-interval", type=int, default=10_000_000,
        help="Print progress every N lines (default: 10M)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be processed without doing it",
    )
    parser.add_argument(
        "--merge", action="store_true",
        help="Run merge step after all files are processed",
    )
    args = parser.parse_args()

    types = args.record_types or ["submissions", "comments"]

    repo_root = Path(__file__).resolve().parent.parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else repo_root / "data" / "reddit" / "monthly"
    manifest_path = repo_root / "data" / "reddit" / "manifest.json"

    # Probe for available months
    print(f"Probing {WEBSEED_BASE} for available months...")
    available = probe_available_months(max_probe=args.months + 6)
    if not available:
        print("ERROR: no archives found at the webseed.", file=sys.stderr)
        sys.exit(1)

    selected = available[: args.months]
    print(f"Available months (most recent {len(available)}): {', '.join(available)}")
    print(f"Selected {args.months} most recent: {', '.join(selected)}")

    # Build job list
    manifest = load_manifest(manifest_path)
    jobs = build_job_list(selected, types, output_dir, manifest)

    if not jobs:
        print("\nAll selected months are already processed.")
        if args.merge:
            _run_merge(repo_root, output_dir)
        return

    # Estimate total download
    total_gb = 0
    for job in jobs:
        try:
            req = urllib.request.Request(job["url"], method="HEAD",
                                        headers={"User-Agent": "rankdiff-research/1.0"})
            resp = urllib.request.urlopen(req, timeout=30, context=_make_ssl_ctx())
            size = int(resp.headers.get("Content-Length", 0))
            job["size_gb"] = round(size / 1e9, 2)
            total_gb += job["size_gb"]
        except Exception:
            job["size_gb"] = "?"

    print(f"\nJobs to process: {len(jobs)} files (~{total_gb:.0f} GB total)")
    for j in jobs:
        print(f"  {j['filename']:20s}  {j['type']:12s}  {j['size_gb']} GB")

    if args.dry_run:
        print("\n(dry run — nothing processed)")
        return

    # Process
    pipeline_t0 = time.monotonic()
    successes = 0
    failures = 0

    for i, job in enumerate(jobs, 1):
        print(f"\n[{i}/{len(jobs)}]", end="")
        try:
            result = stream_and_process(job, args.progress_interval)
            manifest.setdefault("completed", {})[job["key"]] = result
            save_manifest(manifest_path, manifest)
            if result.get("status") == "processed":
                successes += 1
            else:
                failures += 1
        except Exception as e:
            print(f"\n  ERROR processing {job['filename']}: {e}", file=sys.stderr)
            manifest.setdefault("completed", {})[job["key"]] = {
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
    print(f"  Failures:  {failures}")

    if args.merge and successes > 0:
        _run_merge(repo_root, output_dir)


def _run_merge(repo_root: Path, monthly_dir: Path) -> None:
    import subprocess
    merge_script = repo_root / "scripts" / "reddit" / "merge.py"
    cmd = [sys.executable, str(merge_script), "--monthly-dir", str(monthly_dir)]
    print(f"\nRunning: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
