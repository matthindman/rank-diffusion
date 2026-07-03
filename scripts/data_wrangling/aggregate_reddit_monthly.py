#!/usr/bin/env python3
"""Stream Pushshift monthly dumps from the WD drive into SSD aggregates."""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import resource
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import zstandard

from phase2_common import ensure_layout, file_manifest_row, sha256_file, upsert_manifest, utc_now

try:
    import orjson

    def parse_json(line: str) -> dict:
        return orjson.loads(line)

except ImportError:
    import json as _json

    def parse_json(line: str) -> dict:
        return _json.loads(line)


SCRIPT_NAME = "scripts/data_wrangling/aggregate_reddit_monthly.py"
PUSH_RE = re.compile(r"^(R[CS])_(\d{4}-\d{2})\.zst$", re.I)
SCORE_COLS = [
    "score_sum",
    "count",
    "score_max",
    "score_min",
    "score_square_sum",
    "score_positive_count",
    "score_negative_count",
    "score_zero_count",
    "log1p_positive_score_sum",
    "log1p_positive_score_square_sum",
    "nsfw_count",
]


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)


def record_type(name: str) -> str:
    return "comments" if name.upper().startswith("RC_") else "submissions"


def month_from_name(name: str) -> str:
    m = PUSH_RE.match(name)
    if not m:
        raise ValueError(f"not a Pushshift month file: {name}")
    return m.group(2)


def output_path(ssd_root: Path, src: Path) -> Path:
    typ = record_type(src.name)
    month = month_from_name(src.name)
    return ssd_root / "aggregates" / "reddit" / "monthly" / typ / f"{typ}_{month}.parquet"


def validate_output(path: Path, typ: str) -> tuple[bool, str]:
    if not path.exists() or path.stat().st_size == 0:
        return False, "missing_or_empty"
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        return False, f"read_error:{exc}"
    required = {"subreddit", "date", *SCORE_COLS}
    missing = sorted(required - set(df.columns))
    if missing:
        return False, f"missing_columns:{missing}"
    if df.duplicated(["subreddit", "date"]).any():
        return False, "duplicate_subreddit_date"
    if typ == "comments" and df["nsfw_count"].notna().any():
        return False, "comment_nsfw_not_null"
    return True, f"rows={len(df)}"


def find_sources(wd_root: Path, start: str | None, end: str | None, record_types: set[str]) -> list[Path]:
    root = wd_root / "pushshift" / "raw" / "raw"
    out = []
    for p in sorted(root.glob("R[CS]_*.zst")):
        m = PUSH_RE.match(p.name)
        if not m:
            continue
        typ = record_type(p.name)
        if typ not in record_types:
            continue
        month = m.group(2)
        if start and month < start:
            continue
        if end and month > end:
            continue
        out.append(p)
    return out


def new_agg() -> dict[str, float | int | None]:
    return {
        "score_sum": 0,
        "count": 0,
        "score_max": None,
        "score_min": None,
        "score_square_sum": 0,
        "score_positive_count": 0,
        "score_negative_count": 0,
        "score_zero_count": 0,
        "log1p_positive_score_sum": 0.0,
        "log1p_positive_score_square_sum": 0.0,
        "nsfw_count": 0,
    }


def update_agg(agg: dict, score: int, nsfw: bool | None, is_comments: bool) -> None:
    agg["score_sum"] += score
    agg["count"] += 1
    agg["score_max"] = score if agg["score_max"] is None else max(agg["score_max"], score)
    agg["score_min"] = score if agg["score_min"] is None else min(agg["score_min"], score)
    agg["score_square_sum"] += score * score
    if score > 0:
        agg["score_positive_count"] += 1
    elif score < 0:
        agg["score_negative_count"] += 1
    else:
        agg["score_zero_count"] += 1
    lp = math.log1p(max(score, 0))
    agg["log1p_positive_score_sum"] += lp
    agg["log1p_positive_score_square_sum"] += lp * lp
    if not is_comments and nsfw:
        agg["nsfw_count"] += 1


def process_file(src: Path, progress_interval: int) -> tuple[pd.DataFrame, dict[str, object]]:
    typ = record_type(src.name)
    is_comments = typ == "comments"
    aggregates = defaultdict(new_agg)
    lines = 0
    errors = 0
    bytes_read = 0
    file_size = src.stat().st_size
    t0 = time.monotonic()

    class Tracker:
        def __init__(self, fh):
            self.fh = fh
            self.bytes_read = 0

        def read(self, size=-1):
            data = self.fh.read(size)
            self.bytes_read += len(data)
            return data

        def readinto(self, buf):
            n = self.fh.readinto(buf)
            if n:
                self.bytes_read += n
            return n

    with src.open("rb") as raw:
        tracker = Tracker(raw)
        dctx = zstandard.ZstdDecompressor(max_window_size=2**31)
        with dctx.stream_reader(tracker) as reader:
            text_reader = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
            for line in text_reader:
                try:
                    obj = parse_json(line)
                    subreddit = obj.get("subreddit")
                    created_utc = obj.get("created_utc")
                    if not subreddit or created_utc is None:
                        errors += 1
                        continue
                    if isinstance(created_utc, str):
                        created_utc = int(float(created_utc))
                    elif isinstance(created_utc, float):
                        created_utc = int(created_utc)
                    score = obj.get("score", 0)
                    if score is None:
                        score = 0
                    score = int(score)
                    date = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime("%Y-%m-%d")
                    key = (str(subreddit), date)
                    update_agg(aggregates[key], score, bool(obj.get("over_18", False)), is_comments)
                    lines += 1
                    if progress_interval and lines % progress_interval == 0:
                        elapsed = time.monotonic() - t0
                        pct = tracker.bytes_read / file_size * 100 if file_size else 0
                        print(
                            f"  {lines:,} rows | {len(aggregates):,} pairs | "
                            f"{pct:.1f}% | {lines / max(elapsed, 1):,.0f} rows/s | RSS {rss_mb():.0f} MB",
                            flush=True,
                        )
                except Exception:
                    errors += 1
                    continue
        bytes_read = tracker.bytes_read

    rows = []
    for (subreddit, date), agg in aggregates.items():
        row = {"subreddit": subreddit, "date": pd.Timestamp(date)}
        row.update(agg)
        if is_comments:
            row["nsfw_count"] = pd.NA
        rows.append(row)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["date", "subreddit"]).reset_index(drop=True)
    meta = {
        "lines": lines,
        "errors": errors,
        "pairs": len(aggregates),
        "bytes_read": bytes_read,
        "elapsed_seconds": round(time.monotonic() - t0, 1),
    }
    return df, meta


def append_log(path: Path, row: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="") as fh:
        fieldnames = [
            "source_path",
            "record_type",
            "month",
            "status",
            "attempts",
            "lines",
            "errors",
            "parse_error_rate",
            "pairs",
            "output_path",
            "output_bytes",
            "message",
            "finished_at_utc",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wd-root", default="/Volumes/My Passport for Mac")
    ap.add_argument("--ssd-root", default="/Volumes/T9/rank-diffusion-data")
    ap.add_argument("--start", default="2018-12")
    ap.add_argument("--end", default="2022-12")
    ap.add_argument(
        "--record-types",
        nargs="+",
        choices=["comments", "submissions"],
        default=["comments", "submissions"],
        help="Restrict processing to one or both Pushshift record types.",
    )
    ap.add_argument("--progress-interval", type=int, default=5_000_000)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    wd_root = Path(args.wd_root)
    ssd_root = Path(args.ssd_root)
    manifest = ssd_root / "manifest" / "MANIFEST.csv"
    log_path = ssd_root / "logs" / "reddit_monthly_processing_log.csv"
    sources = find_sources(wd_root, args.start, args.end, set(args.record_types))
    print(f"Found {len(sources)} Pushshift monthly files")
    if args.dry_run:
        for src in sources:
            print(f"DRY RUN {src} -> {output_path(ssd_root, src)}")
        return

    ensure_layout(ssd_root)

    for idx, src in enumerate(sources, 1):
        typ = record_type(src.name)
        month = month_from_name(src.name)
        out = output_path(ssd_root, src)
        if out.exists() and not args.force:
            ok, msg = validate_output(out, typ)
            if ok:
                print(f"[{idx}/{len(sources)}] skip valid {out} ({msg})")
                continue
            print(f"[{idx}/{len(sources)}] existing output invalid ({msg}); reprocessing")

        attempts = 0
        while attempts < 2:
            attempts += 1
            try:
                print(f"[{idx}/{len(sources)}] processing {src} attempt {attempts}")
                df, meta = process_file(src, args.progress_interval)
                if df.empty:
                    raise RuntimeError("no aggregate rows produced")
                out.parent.mkdir(parents=True, exist_ok=True)
                tmp = out.with_suffix(".tmp.parquet")
                df.to_parquet(tmp, index=False)
                os.replace(tmp, out)
                ok, msg = validate_output(out, typ)
                if not ok:
                    raise RuntimeError(f"output validation failed: {msg}")
                out_sha = sha256_file(out)
                upsert_manifest(
                    manifest,
                    [
                        file_manifest_row(
                            out,
                            role=f"aggregate/reddit/monthly/{typ}",
                            sha256=out_sha,
                            source_paths=str(src),
                            source_bytes=src.stat().st_size,
                            source_sha256="",
                            script=SCRIPT_NAME,
                            parameters=json.dumps(
                                {
                                    "wd_root": str(wd_root),
                                    "ssd_root": str(ssd_root),
                                    "start": args.start,
                                    "end": args.end,
                                },
                                sort_keys=True,
                            ),
                            status="ok",
                            notes="source checksum omitted to avoid extra full read of multi-TB Reddit raw dumps",
                        )
                    ],
                )
                parse_error_rate = meta["errors"] / max(meta["lines"] + meta["errors"], 1)
                append_log(
                    log_path,
                    {
                        "source_path": src,
                        "record_type": typ,
                        "month": month,
                        "status": "ok",
                        "attempts": attempts,
                        "lines": meta["lines"],
                        "errors": meta["errors"],
                        "parse_error_rate": f"{parse_error_rate:.8f}",
                        "pairs": meta["pairs"],
                        "output_path": out,
                        "output_bytes": out.stat().st_size,
                        "message": msg,
                        "finished_at_utc": utc_now(),
                    },
                )
                print(f"  wrote {out} rows={len(df):,} bytes={out.stat().st_size:,} sha256={out_sha}")
                break
            except Exception as exc:
                print(f"  ERROR attempt {attempts}: {exc}", flush=True)
                if attempts >= 2:
                    append_log(
                        log_path,
                        {
                            "source_path": src,
                            "record_type": typ,
                            "month": month,
                            "status": "error",
                            "attempts": attempts,
                            "message": repr(exc),
                            "finished_at_utc": utc_now(),
                        },
                    )
                else:
                    time.sleep(5)


if __name__ == "__main__":
    main()
