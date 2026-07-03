#!/usr/bin/env python3
"""Create the Phase 2 SSD layout and local repo symlink."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from phase2_common import ensure_layout


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ssd-root", default="/Volumes/T9/rank-diffusion-data")
    ap.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ssd_root = Path(args.ssd_root)
    repo_root = Path(args.repo_root)
    symlink = repo_root / "data" / "ssd"

    print(f"SSD root: {ssd_root}")
    print(f"Repo root: {repo_root}")
    if args.dry_run:
        print("DRY RUN: would create SSD layout and data/ssd symlink")
        return

    ensure_layout(ssd_root)

    if symlink.exists() or symlink.is_symlink():
        if symlink.is_symlink() and Path(os.readlink(symlink)) == ssd_root:
            print(f"Symlink already correct: {symlink} -> {ssd_root}")
        else:
            raise SystemExit(f"Refusing to replace existing path: {symlink}")
    else:
        symlink.symlink_to(ssd_root)
        print(f"Created symlink: {symlink} -> {ssd_root}")


if __name__ == "__main__":
    main()

