#!/usr/bin/env python3
"""Download and aggregate a Wikimedia pageviews month with resource telemetry.

The public pageviews archive contains one gzip file per UTC hour for every
Wikimedia project.  This pilot keeps the compressed sources, filters English
Wikipedia desktop/mobile traffic, and emits exact daily and complete-week
Parquet panels without materializing decompressed text.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import ssl
import statistics
import threading
import time
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import certifi
import psutil
import pyarrow as pa
import pyarrow.parquet as pq


SCRIPT_NAME = "scripts/data_wrangling/wikipedia_pageviews_pilot.py"
USER_AGENT = "rank-diffusion-wikipedia-pilot/0.1 (academic research)"
FILE_RE = re.compile(
    r'href="(pageviews-(\d{8})-(\d{6})\.gz)"[^\n]*?</a>\s+'
    r'\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}\s+([0-9]{7,})'
)
NONARTICLE_PREFIXES = [
    "media", "special", "talk", "user", "user talk", "project", "wikipedia",
    "project talk", "wikipedia talk", "file", "image", "file talk", "image talk",
    "mediawiki", "mediawiki talk", "template", "tm", "template talk", "help",
    "help talk", "category", "category talk", "portal", "portal talk", "draft",
    "draft talk", "mos", "mos talk", "timedtext", "timedtext talk", "module",
    "module talk", "event", "event talk", "wp", "wt",
]
MAINSPACE_SQL = (
    "endpoint_id NOT IN ('Main_Page', '-') AND "
    "(strpos(endpoint_id, ':')=0 OR "
    "lower(replace(split_part(endpoint_id, ':', 1), '_', ' ')) NOT IN ("
    + ",".join(f"'{x}'" for x in NONARTICLE_PREFIXES)
    + "))"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def gzip_marker(path: Path) -> Path:
    return path.with_name(path.name + ".crc-ok")


def gzip_signature(path: Path) -> str:
    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime_ns}"


def mark_gzip_ok(path: Path) -> None:
    gzip_marker(path).write_text(gzip_signature(path) + "\n")


def gzip_ok(path: Path, chunk_size: int = 8 * 1024 * 1024, use_cache: bool = True) -> tuple[bool, str]:
    marker = gzip_marker(path)
    if use_cache and marker.exists() and marker.read_text().strip() == gzip_signature(path):
        return True, "cached"
    try:
        with gzip.open(path, "rb") as fh:
            while fh.read(chunk_size):
                pass
        if use_cache:
            mark_gzip_ok(path)
        return True, ""
    except (OSError, EOFError, zlib.error) as exc:
        return False, f"{type(exc).__name__}: {exc}"


def quarantine(path: Path, reason: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = path.with_name(f"{path.name}.corrupt.{stamp}")
    gzip_marker(path).unlink(missing_ok=True)
    path.replace(target)
    target.with_suffix(target.suffix + ".json").write_text(
        json.dumps({"source": str(path), "reason": reason, "quarantined_at_utc": utc_now()}, indent=2)
    )
    return target


def append_csv(path: Path, row: dict, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in fieldnames})
        fh.flush()
        os.fsync(fh.fileno())


@dataclass
class ResourceSampler:
    root: Path
    interval: float = 2.0
    samples: list[dict] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None

    def start(self) -> None:
        psutil.cpu_percent(interval=None)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        proc = psutil.Process()
        while not self._stop.wait(self.interval):
            try:
                vm = psutil.virtual_memory()
                disk = shutil.disk_usage(self.root)
                io = psutil.disk_io_counters()
                self.samples.append(
                    {
                        "utc": utc_now(),
                        "cpu_percent": psutil.cpu_percent(interval=None),
                        "system_available_bytes": vm.available,
                        "process_rss_bytes": proc.memory_info().rss,
                        "t9_free_bytes": disk.free,
                        "disk_read_bytes": io.read_bytes if io else None,
                        "disk_write_bytes": io.write_bytes if io else None,
                    }
                )
            except (psutil.Error, OSError):
                pass

    def stop(self, telemetry_path: Path) -> dict:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval + 1)
        telemetry_path.parent.mkdir(parents=True, exist_ok=True)
        with telemetry_path.open("w") as fh:
            for sample in self.samples:
                fh.write(json.dumps(sample, sort_keys=True) + "\n")
        if not self.samples:
            return {"samples": 0}
        cpu = [x["cpu_percent"] for x in self.samples]
        rss = [x["process_rss_bytes"] for x in self.samples]
        avail = [x["system_available_bytes"] for x in self.samples]
        free = [x["t9_free_bytes"] for x in self.samples]
        first, last = self.samples[0], self.samples[-1]
        return {
            "samples": len(self.samples),
            "cpu_percent_mean": statistics.fmean(cpu),
            "cpu_percent_max": max(cpu),
            "process_rss_peak_bytes": max(rss),
            "system_available_min_bytes": min(avail),
            "t9_free_min_bytes": min(free),
            "disk_read_delta_bytes": (
                last["disk_read_bytes"] - first["disk_read_bytes"]
                if last["disk_read_bytes"] is not None
                else None
            ),
            "disk_write_delta_bytes": (
                last["disk_write_bytes"] - first["disk_write_bytes"]
                if last["disk_write_bytes"] is not None
                else None
            ),
        }


def fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    context = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(request, context=context, timeout=60) as response:
        return response.read().decode("utf-8")


def month_files(month: str) -> list[dict]:
    year = month[:4]
    url = f"https://dumps.wikimedia.org/other/pageviews/{year}/{month}/"
    html = fetch_text(url)
    files = [
        {
            "name": name,
            "date": datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d"),
            "hour": hour,
            "bytes": int(size),
            "url": url + name,
        }
        for name, date, hour, size in FILE_RE.findall(html)
    ]
    files.sort(key=lambda x: x["name"])
    if not files:
        raise RuntimeError(f"no pageview files found at {url}")
    return files


def cached_month_files(paths: dict[str, Path], month: str) -> list[dict]:
    listing_path = paths["manifest"] / f"wikipedia_pageviews_{month}_sources.json"
    if listing_path.exists():
        return json.loads(listing_path.read_text())
    items = month_files(month)
    listing_path.write_text(json.dumps(items, indent=2, sort_keys=True))
    return items


def layout(ssd_root: Path, month: str) -> dict[str, Path]:
    base = ssd_root / "wikipedia"
    paths = {
        "base": base,
        "raw": base / "raw" / month,
        "hourly": base / "aggregates" / "hourly" / month,
        "daily": base / "aggregates" / "daily" / month,
        "derived": base / "derived",
        "temp": base / "tmp" / month,
        "manifest": base / "manifest",
        "logs": base / "logs",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


HOURLY_SCHEMA = pa.schema(
    [
        ("endpoint_id", pa.string()),
        ("access", pa.string()),
        ("hour", pa.uint8()),
        ("count_views", pa.uint64()),
    ]
)


def parquet_ok(path: Path) -> tuple[bool, str, int]:
    try:
        rows = 0
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=250_000):
            rows += batch.num_rows
        if rows != parquet.metadata.num_rows:
            return False, f"row count mismatch {rows} != {parquet.metadata.num_rows}", rows
        return True, "", rows
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}", 0


def extract_hour(src: Path, output: Path, hour: int, batch_size: int = 250_000) -> dict:
    """Filter one archive using an ends-aware parser; titles may contain spaces."""
    if output.exists() and output.stat().st_size:
        valid, message, rows = parquet_ok(output)
        if valid:
            return {
                "source": str(src), "output": str(output), "status": "skipped",
                "rows": rows, "malformed": 0,
                "source_bytes": src.stat().st_size, "output_bytes": output.stat().st_size,
                "elapsed_seconds": 0.0,
            }
        quarantine(output, message)
    t0 = time.monotonic()
    temp = output.with_suffix(output.suffix + ".part")
    endpoints: list[bytes] = []
    access: list[str] = []
    hours: list[int] = []
    counts: list[int] = []
    rows = malformed = 0
    writer = pq.ParquetWriter(
        temp, HOURLY_SCHEMA, compression="zstd", use_dictionary=["access"],
        write_statistics=True,
    )

    def flush() -> None:
        if not endpoints:
            return
        writer.write_table(
            pa.Table.from_arrays(
                [
                    pa.array(endpoints, type=pa.string()),
                    pa.array(access, type=pa.string()),
                    pa.array(hours, type=pa.uint8()),
                    pa.array(counts, type=pa.uint64()),
                ],
                schema=HOURLY_SCHEMA,
            ),
            row_group_size=batch_size,
        )
        endpoints.clear()
        access.clear()
        hours.clear()
        counts.clear()

    try:
        with gzip.open(src, "rb") as fh:
            for line in fh:
                try:
                    left, count, response_size = line.rstrip(b"\n").rsplit(b" ", 2)
                    domain, title = left.split(b" ", 1)
                    # Parse both numeric fields even though response size is not retained.
                    views = int(count)
                    int(response_size)
                except (ValueError, TypeError):
                    malformed += 1
                    continue
                if domain not in (b"en", b"en.m"):
                    continue
                endpoints.append(title)
                access.append("desktop" if domain == b"en" else "mobile")
                hours.append(hour)
                counts.append(views)
                rows += 1
                if len(endpoints) >= batch_size:
                    flush()
        flush()
    except Exception:
        writer.close()
        if temp.exists():
            temp.unlink()
        raise
    writer.close()
    if malformed:
        temp.unlink(missing_ok=True)
        raise RuntimeError(f"{src.name}: {malformed} malformed records")
    valid, message, validated_rows = parquet_ok(temp)
    if not valid or validated_rows != rows:
        quarantine(temp, message or f"validated rows {validated_rows} != {rows}")
        raise RuntimeError(f"{src.name}: output Parquet validation failed: {message}")
    temp.replace(output)
    return {
        "source": str(src), "output": str(output), "status": "ok", "rows": rows,
        "malformed": malformed, "source_bytes": src.stat().st_size,
        "output_bytes": output.stat().st_size,
        "elapsed_seconds": time.monotonic() - t0,
    }


def extract_batch(paths: dict[str, Path], items: list[dict], workers: int, require_all: bool) -> tuple[list[dict], float]:
    available = []
    for item in items:
        src = paths["raw"] / item["name"]
        if not src.exists():
            if require_all:
                raise RuntimeError(f"missing source: {src}")
            continue
        valid, message = gzip_ok(src)
        if not valid:
            if require_all:
                raise RuntimeError(f"gzip integrity failed: {src}: {message}")
            continue
        available.append((item, src))
    t0 = time.monotonic()
    meta = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for item, src in available:
            output = paths["hourly"] / item["name"].replace(".gz", ".parquet")
            futures[pool.submit(extract_hour, src, output, int(item["hour"][:2]))] = item
        for index, future in enumerate(as_completed(futures), 1):
            result = future.result()
            meta.append(result)
            if index % 24 == 0 or index == len(futures):
                print(
                    f"hourly extraction {index}/{len(futures)} | "
                    f"rows {sum(x['rows'] for x in meta):,} | "
                    f"elapsed {(time.monotonic()-t0)/60:.1f} min",
                    flush=True,
                )
    return meta, time.monotonic() - t0


def extract_available(ssd_root: Path, month: str, workers: int) -> dict:
    paths = layout(ssd_root, month)
    items = cached_month_files(paths, month)
    sampler = ResourceSampler(ssd_root)
    sampler.start()
    meta, elapsed = extract_batch(paths, items, workers, require_all=False)
    telemetry = sampler.stop(paths["logs"] / f"wikipedia_extract_{month}_telemetry.jsonl")
    summary = {
        "phase": "extract",
        "month": month,
        "workers": workers,
        "available_files": len(meta),
        "new_files": sum(x["status"] == "ok" for x in meta),
        "skipped_files": sum(x["status"] == "skipped" for x in meta),
        "rows": sum(x["rows"] for x in meta),
        "source_bytes": sum(x["source_bytes"] for x in meta),
        "output_bytes": sum(x["output_bytes"] for x in meta),
        "elapsed_seconds": elapsed,
        "telemetry": telemetry,
        "finished_at_utc": utc_now(),
    }
    (paths["logs"] / f"wikipedia_extract_{month}_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True)
    )
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    return summary


DOWNLOAD_FIELDS = [
    "file", "url", "expected_bytes", "actual_bytes", "status", "attempts",
    "resumed_from_bytes", "elapsed_seconds", "mbps_decimal", "sha256",
    "started_at_utc", "finished_at_utc", "message",
]


def download_one(item: dict, dest: Path, attempts_max: int = 8) -> dict:
    expected = item["bytes"]
    part = dest.with_suffix(dest.suffix + ".part")
    started = utc_now()
    t0 = time.monotonic()
    attempts = 0
    resumed_from = part.stat().st_size if part.exists() else 0
    message = ""
    while attempts < attempts_max:
        attempts += 1
        offset = part.stat().st_size if part.exists() else 0
        if offset == expected:
            valid, integrity_message = gzip_ok(part, use_cache=False)
            if valid:
                part.replace(dest)
                mark_gzip_ok(dest)
                elapsed = time.monotonic() - t0
                return {
                    "file": dest.name, "url": item["url"], "expected_bytes": expected,
                    "actual_bytes": expected, "status": "ok", "attempts": attempts,
                    "resumed_from_bytes": resumed_from, "elapsed_seconds": round(elapsed, 3),
                    "mbps_decimal": 0.0, "sha256": sha256_file(dest),
                    "started_at_utc": started, "finished_at_utc": utc_now(),
                    "message": "promoted CRC-clean complete partial",
                }
            quarantine(part, integrity_message)
            message = f"complete partial failed gzip integrity: {integrity_message}"
            offset = 0
        elif offset > expected:
            quarantine(part, f"partial larger than expected: {offset} > {expected}")
            message = f"quarantined oversized partial: {offset} > {expected}"
            offset = 0
        headers = {"User-Agent": USER_AGENT}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        request = urllib.request.Request(item["url"], headers=headers)
        try:
            context = ssl.create_default_context(cafile=certifi.where())
            response = urllib.request.urlopen(request, context=context, timeout=120)
            status = getattr(response, "status", 200)
            mode = "ab" if offset and status == 206 else "wb"
            if mode == "wb":
                offset = 0
            with response, part.open(mode) as fh:
                while chunk := response.read(4 * 1024 * 1024):
                    fh.write(chunk)
            actual = part.stat().st_size
            if actual != expected:
                raise IOError(f"size mismatch: {actual} != {expected}")
            valid, integrity_message = gzip_ok(part, use_cache=False)
            if not valid:
                quarantine(part, integrity_message)
                raise IOError(f"gzip integrity failure: {integrity_message}")
            part.replace(dest)
            mark_gzip_ok(dest)
            elapsed = time.monotonic() - t0
            return {
                "file": dest.name,
                "url": item["url"],
                "expected_bytes": expected,
                "actual_bytes": actual,
                "status": "ok",
                "attempts": attempts,
                "resumed_from_bytes": resumed_from,
                "elapsed_seconds": round(elapsed, 3),
                "mbps_decimal": round(actual * 8 / max(elapsed, 0.001) / 1_000_000, 3),
                "sha256": sha256_file(dest),
                "started_at_utc": started,
                "finished_at_utc": utc_now(),
                "message": message,
            }
        except Exception as exc:  # resumable network operation
            message = f"{type(exc).__name__}: {exc}"
            if attempts < attempts_max:
                retry_after = getattr(exc, "headers", {}).get("Retry-After") if hasattr(exc, "headers") else None
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempts, 60)
                time.sleep(delay)
    return {
        "file": dest.name,
        "url": item["url"],
        "expected_bytes": expected,
        "actual_bytes": part.stat().st_size if part.exists() else 0,
        "status": "error",
        "attempts": attempts,
        "resumed_from_bytes": resumed_from,
        "elapsed_seconds": round(time.monotonic() - t0, 3),
        "mbps_decimal": "",
        "sha256": "",
        "started_at_utc": started,
        "finished_at_utc": utc_now(),
        "message": message,
    }


def download_month(ssd_root: Path, month: str, workers: int) -> dict:
    paths = layout(ssd_root, month)
    items = cached_month_files(paths, month)
    expected_total = sum(x["bytes"] for x in items)
    free = shutil.disk_usage(ssd_root).free
    if free < expected_total + 30 * 1024**3:
        raise RuntimeError(f"insufficient T9 space: {free:,} bytes free")
    log_path = paths["logs"] / f"wikipedia_download_{month}.csv"
    sampler = ResourceSampler(ssd_root)
    sampler.start()
    t0 = time.monotonic()
    completed = skipped = errors = 0
    pending = []
    candidates = []
    corrupt_checkpoint = 0
    for item in items:
        dest = paths["raw"] / item["name"]
        if dest.exists() and dest.stat().st_size == item["bytes"]:
            candidates.append((item, dest))
        else:
            pending.append((item, dest))
    if candidates:
        print(f"CRC-validating {len(candidates)} checkpoint files with {workers} workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as verifier:
        checks = {verifier.submit(gzip_ok, dest): (item, dest) for item, dest in candidates}
        for index, future in enumerate(as_completed(checks), 1):
            item, dest = checks[future]
            valid, integrity_message = future.result()
            if valid:
                skipped += 1
            else:
                corrupt_checkpoint += 1
                quarantined = quarantine(dest, integrity_message)
                print(f"quarantined {quarantined.name}: {integrity_message}", flush=True)
                pending.append((item, dest))
            if index % 25 == 0:
                print(f"CRC checkpoint {index}/{len(candidates)} | corrupt {corrupt_checkpoint}", flush=True)
    initial_have = sum(p.stat().st_size for p in paths["raw"].glob("*.gz"))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(download_one, item, dest): item for item, dest in pending}
        for future in as_completed(futures):
            row = future.result()
            append_csv(log_path, row, DOWNLOAD_FIELDS)
            if row["status"] == "ok":
                completed += 1
            else:
                errors += 1
            done = skipped + completed + errors
            if completed % max(6, workers) == 0 or row["status"] != "ok":
                elapsed = time.monotonic() - t0
                have = sum(p.stat().st_size for p in paths["raw"].glob("*.gz"))
                new_bytes = max(have - initial_have, 0)
                rate = new_bytes * 8 / max(elapsed, 1) / 1_000_000
                eta = (expected_total - have) * 8 / max(rate, 0.001) / 1_000_000
                print(
                    f"[{done}/{len(items)}] {have/1e9:.2f}/{expected_total/1e9:.2f} GB "
                    f"| {rate:.1f} Mbps | ETA {eta/3600:.2f} h | errors {errors}",
                    flush=True,
                )
    elapsed = time.monotonic() - t0
    telemetry = sampler.stop(paths["logs"] / f"wikipedia_download_{month}_telemetry.jsonl")
    summary = {
        "phase": "download",
        "month": month,
        "started_files": completed,
        "skipped_files": skipped,
        "errors": errors,
        "expected_files": len(items),
        "expected_bytes": expected_total,
        "present_bytes": sum(p.stat().st_size for p in paths["raw"].glob("*.gz")),
        "elapsed_seconds": elapsed,
        "workers": workers,
        "effective_mbps_decimal": max(expected_total - initial_have, 0) * 8 / max(elapsed, 0.001) / 1_000_000,
        "finished_at_utc": utc_now(),
        "telemetry": telemetry,
    }
    (paths["logs"] / f"wikipedia_download_{month}_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True)
    )
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    if errors:
        raise RuntimeError(f"{errors} downloads failed; rerun to resume")
    return summary


def sql_string(path: Path) -> str:
    return str(path).replace("'", "''")


def aggregate_month(ssd_root: Path, month: str, memory_limit: str, extract_workers: int) -> dict:
    paths = layout(ssd_root, month)
    items = cached_month_files(paths, month)
    missing = [x["name"] for x in items if not (paths["raw"] / x["name"]).exists()]
    if missing:
        raise RuntimeError(f"missing {len(missing)} hourly files; run download first")
    sampler = ResourceSampler(ssd_root)
    sampler.start()
    t0 = time.monotonic()
    extract_meta, extract_elapsed = extract_batch(paths, items, extract_workers, require_all=True)
    con = duckdb.connect(str(paths["temp"] / "wikipedia_pilot.duckdb"))
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET threads=1")
    con.execute(f"SET temp_directory='{sql_string(paths['temp'] / 'spill')}'")
    daily_meta = []
    for day in sorted({x["date"] for x in items}):
        day_files = [
            paths["hourly"] / x["name"].replace(".gz", ".parquet")
            for x in items if x["date"] == day
        ]
        if len(day_files) != 24:
            raise RuntimeError(f"{day} has {len(day_files)} files, expected 24")
        output = paths["daily"] / f"wikipedia_en_{day}.parquet"
        file_list = ",".join(f"'{sql_string(p)}'" for p in day_files)
        q = f"""
            SELECT DATE '{day}' AS date,
                   endpoint_id,
                   SUM(count_views)::UBIGINT AS metric_value,
                   COALESCE(SUM(count_views) FILTER (access='desktop'), 0)::UBIGINT AS desktop_views,
                   COALESCE(SUM(count_views) FILTER (access='mobile'), 0)::UBIGINT AS mobile_views
            FROM read_parquet([{file_list}])
            WHERE {MAINSPACE_SQL}
            GROUP BY endpoint_id
        """
        d0 = time.monotonic()
        con.execute(
            f"COPY ({q}) TO '{sql_string(output)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)"
        )
        valid, message, _ = parquet_ok(output)
        if not valid:
            quarantine(output, message)
            raise RuntimeError(f"daily Parquet validation failed for {day}: {message}")
        stats = con.execute(
            f"SELECT COUNT(*), SUM(metric_value), MIN(metric_value), MAX(metric_value) "
            f"FROM read_parquet('{sql_string(output)}')"
        ).fetchone()
        daily_meta.append(
            {
                "date": day,
                "source_files": 24,
                "rows": stats[0],
                "total_views": stats[1],
                "min_views": stats[2],
                "max_views": stats[3],
                "output_bytes": output.stat().st_size,
                "elapsed_seconds": time.monotonic() - d0,
            }
        )
        print(
            f"[{len(daily_meta)}/{len({x['date'] for x in items})}] {day}: "
            f"{stats[0]:,} titles, {output.stat().st_size/1e6:.1f} MB, "
            f"{daily_meta[-1]['elapsed_seconds']:.1f}s",
            flush=True,
        )
    daily_glob = paths["daily"] / "*.parquet"
    monthly = paths["derived"] / f"wikipedia_en_{month}_daily.parquet"
    con.execute(
            f"COPY (SELECT date, endpoint_id, metric_value, desktop_views, mobile_views "
        f"FROM read_parquet('{sql_string(daily_glob)}') ORDER BY date, endpoint_id) "
        f"TO '{sql_string(monthly)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)"
    )
    valid, message, _ = parquet_ok(monthly)
    if not valid:
        quarantine(monthly, message)
        raise RuntimeError(f"monthly Parquet validation failed: {message}")
    weekly = paths["derived"] / f"wikipedia_en_{month}_weekly_complete.parquet"
    con.execute(
        f"""COPY (
            WITH d AS (
              SELECT *, date - CAST((dayofweek(date) + 6) % 7 AS INTEGER) AS week_date
              FROM read_parquet('{sql_string(monthly)}')
            ), complete AS (
              SELECT week_date FROM d GROUP BY week_date HAVING COUNT(DISTINCT date)=7
            )
            SELECT endpoint_id, week_date AS date,
                   SUM(metric_value)::UBIGINT AS metric_value,
                   SUM(desktop_views)::UBIGINT AS desktop_views,
                   SUM(mobile_views)::UBIGINT AS mobile_views
            FROM d JOIN complete USING (week_date)
            GROUP BY endpoint_id, week_date ORDER BY week_date, endpoint_id
        ) TO '{sql_string(weekly)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)"""
    )
    valid, message, _ = parquet_ok(weekly)
    if not valid:
        quarantine(weekly, message)
        raise RuntimeError(f"weekly Parquet validation failed: {message}")
    # Score-blind concentration diagnostics only; no K is selected here.
    concentration = con.execute(
        f"""WITH ranked AS (
              SELECT date, endpoint_id, metric_value,
                     ROW_NUMBER() OVER (PARTITION BY date ORDER BY metric_value DESC, endpoint_id) r,
                     SUM(metric_value) OVER (PARTITION BY date) total
              FROM read_parquet('{sql_string(weekly)}')
            ), ks(k) AS (VALUES (100),(1000),(5000),(10000),(25000),(50000),(100000),(250000),(500000))
            SELECT k, AVG(top_views / total) AS mean_weekly_share
            FROM ks CROSS JOIN LATERAL (
              SELECT date, MAX(total) total, SUM(metric_value) top_views
              FROM ranked WHERE r <= k GROUP BY date
            ) GROUP BY k ORDER BY k"""
    ).fetchall()
    panel_stats = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT date), COUNT(DISTINCT endpoint_id), SUM(metric_value) "
        f"FROM read_parquet('{sql_string(monthly)}')"
    ).fetchone()
    weekly_stats = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT date), COUNT(DISTINCT endpoint_id), SUM(metric_value) "
        f"FROM read_parquet('{sql_string(weekly)}')"
    ).fetchone()
    con.close()
    elapsed = time.monotonic() - t0
    telemetry = sampler.stop(paths["logs"] / f"wikipedia_aggregate_{month}_telemetry.jsonl")
    summary = {
        "phase": "aggregate",
        "month": month,
        "memory_limit": memory_limit,
        "extract_workers": extract_workers,
        "hourly_extraction": {
            "files": len(extract_meta),
            "rows": sum(x["rows"] for x in extract_meta),
            "output_bytes": sum(x["output_bytes"] for x in extract_meta),
            "elapsed_seconds": extract_elapsed,
            "new_files": sum(x["status"] == "ok" for x in extract_meta),
            "skipped_files": sum(x["status"] == "skipped" for x in extract_meta),
        },
        "daily_partitions": daily_meta,
        "daily_panel": {
            "path": str(monthly), "bytes": monthly.stat().st_size,
            "rows": panel_stats[0], "dates": panel_stats[1],
            "entities": panel_stats[2], "views": panel_stats[3],
        },
        "weekly_panel": {
            "path": str(weekly), "bytes": weekly.stat().st_size,
            "rows": weekly_stats[0], "weeks": weekly_stats[1],
            "entities": weekly_stats[2], "views": weekly_stats[3],
        },
        "concentration": [
            {"k": int(k), "mean_weekly_share": float(share)} for k, share in concentration
        ],
        "elapsed_seconds": elapsed,
        "telemetry": telemetry,
        "finished_at_utc": utc_now(),
        "scope": (
            "English Wikipedia requested titles; desktop+mobile; official non-main namespaces, "
            "Main_Page, and '-' excluded; redirects and page moves not canonicalized"
        ),
    }
    summary_path = paths["logs"] / f"wikipedia_aggregate_{month}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    # Register the durable synthetic products in a Wikipedia-specific manifest.
    manifest = paths["manifest"] / "MANIFEST.csv"
    fields = [
        "file_path", "role", "bytes", "sha256", "source_paths", "source_bytes",
        "script", "parameters", "produced_at_utc", "status", "notes",
    ]
    for product, role in [(monthly, "derived/wikipedia/daily"), (weekly, "derived/wikipedia/weekly")]:
        append_csv(
            manifest,
            {
                "file_path": product,
                "role": role,
                "bytes": product.stat().st_size,
                "sha256": sha256_file(product),
                "source_paths": paths["raw"],
                "source_bytes": sum(p.stat().st_size for p in paths["raw"].glob("*.gz")),
                "script": SCRIPT_NAME,
                "parameters": json.dumps({"month": month, "memory_limit": memory_limit}),
                "produced_at_utc": utc_now(),
                "status": "ok",
                "notes": summary["scope"],
            },
            fields,
        )
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["download", "extract", "aggregate", "all"])
    parser.add_argument("--month", default="2025-01")
    parser.add_argument("--ssd-root", type=Path, default=Path("/Volumes/T9/rank-diffusion-data"))
    parser.add_argument("--memory-limit", default="6GB")
    parser.add_argument("--download-workers", type=int, default=4)
    parser.add_argument("--extract-workers", type=int, default=2)
    args = parser.parse_args()
    if not args.ssd_root.exists() or args.ssd_root.stat().st_dev == Path("/").stat().st_dev:
        raise SystemExit(f"T9 target is not mounted: {args.ssd_root}")
    lock_path = args.ssd_root / "wikipedia" / f".{args.month}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"another Wikipedia pilot process holds {lock_path}")
        lock.write(f"pid={os.getpid()} phase={args.phase} started={utc_now()}\n")
        lock.flush()
        if args.phase in {"download", "all"}:
            download_month(args.ssd_root, args.month, args.download_workers)
        if args.phase == "extract":
            extract_available(args.ssd_root, args.month, args.extract_workers)
        if args.phase in {"aggregate", "all"}:
            aggregate_month(args.ssd_root, args.month, args.memory_limit, args.extract_workers)


if __name__ == "__main__":
    main()
