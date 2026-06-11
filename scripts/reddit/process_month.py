#!/usr/bin/env python3
"""
Stream-process a single Pushshift .zst file into a daily subreddit aggregate parquet.

Reads a zstandard-compressed NDJSON file (RC_*.zst for comments, RS_*.zst for
submissions) line by line, extracts subreddit/score/timestamp, and writes a
small monthly aggregate parquet with columns:
    subreddit, date, score_sum, count, nsfw_count

Supports both local files and HTTP URLs (streams without saving to disk).

Usage:
    python process_month.py /path/to/RS_2020-01.zst
    python process_month.py --url https://seed.pullpush.io/file/RaiderB/reddit/submissions/RS_2024-09.zst
"""

from __future__ import annotations

import argparse
import io
import re
import resource
import ssl
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import zstandard

try:
    import orjson

    def parse_json(line: str) -> dict:
        return orjson.loads(line)

except ImportError:
    import json

    def parse_json(line: str) -> dict:
        return json.loads(line)


WEBSEED_BASE = "https://seed.pullpush.io/file/RaiderB/reddit"


def detect_type(name: str) -> str:
    upper = Path(name).name.upper()
    if upper.startswith("RC"):
        return "comments"
    elif upper.startswith("RS"):
        return "submissions"
    raise ValueError(
        f"Cannot auto-detect type from '{name}'. "
        "Use --type comments or --type submissions."
    )


def detect_month(name: str) -> str:
    m = re.search(r"(\d{4}-\d{2})", name)
    return m.group(1) if m else "unknown"


def get_rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)


def process_zst_stream(
    source,
    file_size: int,
    progress_interval: int = 10_000_000,
    sample_first_n: int = 3,
) -> tuple[dict[tuple[str, str], dict], int, int]:
    """
    Stream-process a .zst NDJSON stream.

    Args:
        source: file-like binary stream (local file handle or HTTP response)
        file_size: total bytes for progress tracking (0 if unknown)

    Returns:
        aggregates, lines_processed, errors
    """
    aggregates: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"score_sum": 0, "count": 0, "nsfw_count": 0}
    )

    dctx = zstandard.ZstdDecompressor(max_window_size=2**31)
    errors = 0
    lines_processed = 0
    t0 = time.monotonic()
    samples_shown = 0
    bytes_tracker = _BytesTracker(source)

    reader = dctx.stream_reader(bytes_tracker)
    text_reader = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")

    for line in text_reader:
        try:
            obj = parse_json(line)

            if samples_shown < sample_first_n:
                keys = list(obj.keys())[:12]
                print(f"  [sample {samples_shown + 1}] keys={keys}")
                print(f"    subreddit={obj.get('subreddit')!r} "
                      f"score={obj.get('score')!r} "
                      f"created_utc={obj.get('created_utc')!r} "
                      f"over_18={obj.get('over_18')!r}")
                samples_shown += 1

            subreddit = obj.get("subreddit")
            if not subreddit:
                errors += 1
                continue

            score = obj.get("score", 0)
            if score is None:
                score = 0

            created_utc = obj.get("created_utc")
            if created_utc is None:
                errors += 1
                continue

            if isinstance(created_utc, str):
                created_utc = int(float(created_utc))
            elif isinstance(created_utc, float):
                created_utc = int(created_utc)

            date_str = datetime.fromtimestamp(
                created_utc, tz=timezone.utc
            ).strftime("%Y-%m-%d")

            is_nsfw = obj.get("over_18", False)
            if is_nsfw is None:
                is_nsfw = False

            key = (subreddit, date_str)
            agg = aggregates[key]
            agg["score_sum"] += int(score)
            agg["count"] += 1
            if is_nsfw:
                agg["nsfw_count"] += 1

            lines_processed += 1
            if lines_processed % progress_interval == 0:
                elapsed = time.monotonic() - t0
                rate = lines_processed / elapsed
                bytes_read = bytes_tracker.bytes_read
                if file_size > 0 and bytes_read > 0:
                    pct = bytes_read / file_size * 100
                    eta_min = ((file_size - bytes_read) / (bytes_read / elapsed)) / 60
                    pct_str = f"{pct:5.1f}%"
                    eta_str = f"ETA {eta_min:.0f}m"
                else:
                    pct_str = "  ?  "
                    eta_str = "ETA ?"
                rss = get_rss_mb()
                print(
                    f"  {lines_processed:>13,} lines | "
                    f"{len(aggregates):>9,} pairs | "
                    f"{rate:,.0f} ln/s | "
                    f"{pct_str} | "
                    f"{eta_str} | "
                    f"RSS {rss:.0f} MB | "
                    f"{elapsed/60:.1f}m",
                    flush=True,
                )

        except (ValueError, KeyError, TypeError, OverflowError):
            errors += 1
            continue

    return dict(aggregates), lines_processed, errors


class _BytesTracker:
    """Wrapper that counts bytes read from a stream, for progress tracking."""

    def __init__(self, source):
        self._source = source
        self.bytes_read = 0

    def read(self, size=-1):
        data = self._source.read(size)
        self.bytes_read += len(data)
        return data

    def readinto(self, buf):
        n = self._source.readinto(buf)
        if n:
            self.bytes_read += n
        return n


def aggregates_to_dataframe(
    aggregates: dict[tuple[str, str], dict],
) -> pd.DataFrame:
    rows = [
        {
            "subreddit": sub,
            "date": date,
            "score_sum": agg["score_sum"],
            "count": agg["count"],
            "nsfw_count": agg["nsfw_count"],
        }
        for (sub, date), agg in aggregates.items()
    ]
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "subreddit"]).reset_index(drop=True)
    return df


def process_zst_file(
    zst_path: str | Path,
    record_type: str | None = None,
    progress_interval: int = 10_000_000,
    sample_first_n: int = 0,
) -> tuple[dict[tuple[str, str], dict], int, int]:
    """Process a local .zst archive and return aggregate rows.

    The pipeline orchestrator imports this helper directly so it can process
    local archives without duplicating the stream setup in this CLI module.
    """
    path = Path(zst_path)
    if not path.exists():
        raise FileNotFoundError(f"file not found: {path}")
    if record_type is not None:
        detected = detect_type(path.name)
        if detected != record_type:
            raise ValueError(f"record_type={record_type!r} does not match archive name {path.name!r}")

    with path.open("rb") as fh:
        return process_zst_stream(
            fh,
            path.stat().st_size,
            progress_interval=progress_interval,
            sample_first_n=sample_first_n,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Process a Pushshift .zst file into daily subreddit aggregates."
    )
    parser.add_argument("zst_path", nargs="?", default=None,
                        help="Path to a local .zst file")
    parser.add_argument("--url", default=None,
                        help="URL to stream a .zst file from (no local disk needed)")
    parser.add_argument("--type", dest="record_type",
                        choices=["comments", "submissions"], default=None,
                        help="Record type (auto-detected from filename if omitted)")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory for the parquet file")
    parser.add_argument("--progress-interval", type=int, default=10_000_000,
                        help="Print progress every N lines (default: 10M)")
    args = parser.parse_args()

    if not args.zst_path and not args.url:
        parser.error("Provide either a local path or --url")

    # Determine source name for type/month detection
    source_name = args.url.split("/")[-1] if args.url else Path(args.zst_path).name
    record_type = args.record_type or detect_type(source_name)
    month = detect_month(source_name)

    repo_root = Path(__file__).resolve().parent.parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else repo_root / "data" / "reddit" / "monthly"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{record_type}_{month}.parquet"

    print(f"Source: {args.url or args.zst_path}")
    print(f"Type: {record_type}")
    print(f"Month: {month}")
    print(f"Output: {output_path}")
    print(f"JSON parser: {'orjson' if 'orjson' in sys.modules else 'json (stdlib)'}")
    print(f"Initial RSS: {get_rss_mb():.0f} MB")

    t0 = time.monotonic()

    if args.url:
        print(f"Streaming from HTTP (no disk usage)...")
        ctx = ssl.create_default_context()
        try:
            import certifi
            ctx.load_verify_locations(certifi.where())
        except ImportError:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(args.url, headers={
            "User-Agent": "rankdiff-research/1.0",
        })
        response = urllib.request.urlopen(req, timeout=300, context=ctx)
        file_size = int(response.headers.get("Content-Length", 0))
        print(f"  Content-Length: {file_size / 1e9:.2f} GB")
        print()
        aggregates, lines_processed, errors = process_zst_stream(
            response, file_size,
            progress_interval=args.progress_interval,
        )
    else:
        zst_path = Path(args.zst_path)
        if not zst_path.exists():
            print(f"Error: file not found: {zst_path}", file=sys.stderr)
            sys.exit(1)
        file_size = zst_path.stat().st_size
        print(f"  File size: {file_size / 1e9:.2f} GB")
        print()
        with open(zst_path, "rb") as fh:
            aggregates, lines_processed, errors = process_zst_stream(
                fh, file_size,
                progress_interval=args.progress_interval,
            )

    elapsed = time.monotonic() - t0

    print()
    print(f"Done in {elapsed/60:.1f} minutes")
    print(f"  Lines processed: {lines_processed:,}")
    print(f"  Errors/skipped:  {errors:,}")
    print(f"  Unique (subreddit, day) pairs: {len(aggregates):,}")

    if not aggregates:
        print("Warning: no data extracted.", file=sys.stderr)
        sys.exit(1)

    df = aggregates_to_dataframe(aggregates)
    df.to_parquet(output_path, index=False)
    print(f"  Wrote: {output_path} ({output_path.stat().st_size / 1e6:.1f} MB)")

    print(f"\n  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    print(f"  Unique subreddits: {df['subreddit'].nunique():,}")
    print(f"  Unique days: {df['date'].nunique()}")
    top = df.groupby("subreddit")["count"].sum().nlargest(5)
    print(f"  Top 5 subreddits by {record_type} count:")
    for sub, cnt in top.items():
        print(f"    r/{sub}: {cnt:,}")


if __name__ == "__main__":
    main()
