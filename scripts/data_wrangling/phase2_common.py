#!/usr/bin/env python3
"""Shared helpers for Phase 2 data migration.

These helpers intentionally avoid project model imports so the migration scripts
can run unattended from a detached shell on the external SSD.
"""

from __future__ import annotations

import csv
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


MANIFEST_COLUMNS = [
    "file_path",
    "role",
    "bytes",
    "sha256",
    "source_paths",
    "source_bytes",
    "source_sha256",
    "script",
    "parameters",
    "produced_at_utc",
    "status",
    "notes",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_layout(ssd_root: Path) -> None:
    for rel in [
        "raw_small/facebook/crowdtangle",
        "raw_small/facebook/crowdtangle/add",
        "raw_small/facebook/crowdtangle_backfill",
        "aggregates/facebook",
        "aggregates/reddit/monthly/submissions",
        "aggregates/reddit/monthly/comments",
        "derived",
        "manifest",
        "logs",
    ]:
        (ssd_root / rel).mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, chunk_size: int = 64 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_file_with_source_hash(src: Path, dst: Path, chunk_size: int = 64 * 1024 * 1024) -> str:
    """Copy src to dst and return the SHA-256 of bytes read from src."""
    h = hashlib.sha256()
    tmp = dst.with_name(dst.name + ".tmp")
    dst.parent.mkdir(parents=True, exist_ok=True)
    if tmp.exists():
        tmp.unlink()
    with src.open("rb") as rfh, tmp.open("wb") as wfh:
        for chunk in iter(lambda: rfh.read(chunk_size), b""):
            h.update(chunk)
            wfh.write(chunk)
    try:
        shutil_copystat(src, tmp)
    except OSError:
        pass
    os.replace(tmp, dst)
    return h.hexdigest()


def shutil_copystat(src: Path, dst: Path) -> None:
    import shutil

    shutil.copystat(src, dst, follow_symlinks=True)


def read_manifest(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    return {row["file_path"]: row for row in rows}


def upsert_manifest(manifest_path: Path, rows: Iterable[dict[str, object]]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    existing = read_manifest(manifest_path)
    for row in rows:
        clean = {col: "" for col in MANIFEST_COLUMNS}
        for key, value in row.items():
            if key not in clean:
                continue
            if isinstance(value, (list, tuple)):
                clean[key] = "|".join(str(v) for v in value)
            else:
                clean[key] = "" if value is None else str(value)
        if not clean["file_path"]:
            raise ValueError("manifest row missing file_path")
        existing[clean["file_path"]] = clean
    tmp = manifest_path.with_suffix(".tmp")
    with tmp.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        for key in sorted(existing):
            writer.writerow(existing[key])
    os.replace(tmp, manifest_path)


def file_manifest_row(
    path: Path,
    *,
    role: str,
    sha256: str | None,
    source_paths: list[str] | str | None,
    source_bytes: list[int] | int | None,
    source_sha256: list[str] | str | None,
    script: str,
    parameters: str,
    status: str = "ok",
    notes: str = "",
) -> dict[str, object]:
    return {
        "file_path": str(path),
        "role": role,
        "bytes": path.stat().st_size if path.exists() else "",
        "sha256": sha256 or "",
        "source_paths": source_paths,
        "source_bytes": source_bytes,
        "source_sha256": source_sha256,
        "script": script,
        "parameters": parameters,
        "produced_at_utc": utc_now(),
        "status": status,
        "notes": notes,
    }


def format_gb(n: int | float) -> str:
    return f"{float(n) / 1e9:.3f} GB"

