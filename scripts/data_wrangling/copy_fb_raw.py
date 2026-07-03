#!/usr/bin/env python3
"""Copy approved irreplaceable CrowdTangle raw files to the SSD.

Approved set:
  - crowdtangle_backfill/*_test.parquet
  - crowdtangle/full_fb.parquet
  - crowdtangle/full_fb_tuesdays.parquet
  - crowdtangle/fb_leaderboard.parquet
  - crowdtangle/add/*.csv

TSV duplicates, image tarballs, and bulk CSV chunk trees are intentionally not
copied here.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from phase2_common import (
    copy_file_with_source_hash,
    ensure_layout,
    file_manifest_row,
    format_gb,
    read_manifest,
    sha256_file,
    upsert_manifest,
)


SCRIPT_NAME = "scripts/data_wrangling/copy_fb_raw.py"
COPY_RATIONALE = (
    "CrowdTangle is discontinued; selected post-level FB files are irreplaceable "
    "and currently live on an aging WD drive. Reddit raw dumps are re-acquirable "
    "and are intentionally not copied."
)


def approved_sources(wd_root: Path) -> list[Path]:
    files: list[Path] = []
    files.extend(sorted((wd_root / "crowdtangle_backfill").glob("*_test.parquet")))
    for name in ["full_fb.parquet", "full_fb_tuesdays.parquet", "fb_leaderboard.parquet"]:
        files.append(wd_root / "crowdtangle" / name)
    files.extend(sorted((wd_root / "crowdtangle" / "add").glob("*.csv")))
    return files


def destination_for(src: Path, wd_root: Path, ssd_root: Path) -> Path:
    rel = src.relative_to(wd_root)
    return ssd_root / "raw_small" / "facebook" / rel


def copy_or_verify(src: Path, dst: Path, src_size: int, max_attempts: int) -> tuple[str, str, str]:
    if dst.exists() and dst.stat().st_size == src_size:
        print("  destination exists with matching size; verifying hashes")
        src_sha = sha256_file(src)
        dst_sha = sha256_file(dst)
        if src_sha == dst_sha:
            return src_sha, dst_sha, "copied_verified"
        print("  hash mismatch; recopying")
    else:
        print(f"  copying to {dst}")

    src_sha = ""
    dst_sha = ""
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            print(f"  retrying copy attempt {attempt}/{max_attempts}")
        src_sha = copy_file_with_source_hash(src, dst)
        dst_sha = sha256_file(dst)
        if src_sha == dst_sha:
            return src_sha, dst_sha, "copied_verified"
        print(f"  attempt {attempt} hash mismatch: source={src_sha} dest={dst_sha}")
    return src_sha, dst_sha, "hash_mismatch"


def manifest_verified_copy(
    src: Path, dst: Path, src_size: int, manifest_rows: dict[str, dict[str, str]]
) -> tuple[str, str, str] | None:
    row = manifest_rows.get(str(dst))
    if not row:
        return None
    if row.get("status") != "copied_verified":
        return None
    if row.get("source_paths") != str(src):
        return None
    if row.get("source_bytes") != str(src_size):
        return None
    if row.get("sha256") != row.get("source_sha256"):
        return None
    if not dst.exists() or dst.stat().st_size != src_size:
        return None

    print("  manifest has verified source==dest; checking destination hash")
    dst_sha = sha256_file(dst)
    if dst_sha != row["sha256"]:
        print("  destination hash no longer matches manifest; recopying")
        return None
    return row["source_sha256"], dst_sha, "copied_verified"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wd-root", default="/Volumes/My Passport for Mac")
    ap.add_argument("--ssd-root", default="/Volumes/T9/rank-diffusion-data")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="Copy at most N files, for testing")
    ap.add_argument("--max-copy-attempts", type=int, default=2)
    args = ap.parse_args()

    wd_root = Path(args.wd_root)
    ssd_root = Path(args.ssd_root)
    manifest = ssd_root / "manifest" / "MANIFEST.csv"

    files = [p for p in approved_sources(wd_root) if p.exists()]
    if args.limit is not None:
        files = files[: args.limit]

    total_bytes = sum(p.stat().st_size for p in files)
    print(f"Approved FB raw files: {len(files)} ({format_gb(total_bytes)})")
    if args.dry_run:
        for src in files:
            print(f"DRY RUN {src} -> {destination_for(src, wd_root, ssd_root)}")
        return

    ensure_layout(ssd_root)
    manifest_rows = read_manifest(manifest)

    for i, src in enumerate(files, 1):
        dst = destination_for(src, wd_root, ssd_root)
        src_size = src.stat().st_size
        print(f"[{i}/{len(files)}] {src} ({format_gb(src_size)})")

        verified = manifest_verified_copy(src, dst, src_size, manifest_rows)
        if verified is None:
            src_sha, dst_sha, status = copy_or_verify(
                src, dst, src_size, max_attempts=args.max_copy_attempts
            )
        else:
            src_sha, dst_sha, status = verified

        print(f"  source sha256: {src_sha}")
        print(f"  dest   sha256: {dst_sha}")
        if src_sha != dst_sha:
            raise SystemExit(f"checksum mismatch after copy: {src} -> {dst}")

        row = file_manifest_row(
            dst,
            role="raw_small/facebook",
            sha256=dst_sha,
            source_paths=str(src),
            source_bytes=src_size,
            source_sha256=src_sha,
            script=SCRIPT_NAME,
            parameters=f"--wd-root {wd_root} --ssd-root {ssd_root}",
            status=status,
            notes=COPY_RATIONALE,
        )
        upsert_manifest(manifest, [row])

    print(f"Done. Manifest: {manifest}")


if __name__ == "__main__":
    main()
