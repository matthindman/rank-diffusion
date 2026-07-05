#!/usr/bin/env python3
"""P4 (2026-07-05, review B7/B8/B9/F6/R5): robustness battery for the mix
exponent b = s(h*)/s(1) and the temperament dispersion s.

Measurements (all declared, none tuned):
  1. Baseline b (committed convention: acf_lags=2, no detrending).
  2. Detrended b: per-entity LINEAR detrending of the change series u
     (= quadratic level detrending; constant drift is already removed by the
     per-entity variance).  Bounds lifecycle-arc contamination (2n caveat).
     Pre-registered prediction: comments b moves toward 1 more than FB.
  3. Split-window b: first vs second half of the panel, at the half-window
     h* AND at a common h=4 (the halves cannot support h*=13).
  4. Moving-block bootstrap over WEEKS for b (time-window uncertainty the
     entity bootstrap cannot see -- review B9).  Blocks of 3*h consecutive
     weeks, gap-relabeled so no spurious cross-block change pairs exist;
     min_changes=6 inside the bootstrap (declared: resampled panels support
     fewer non-overlapping h-changes per entity).
  5. Temperament kappa_acf sensitivity: s at pooled-ACF depth 1..6 (review
     B7: too-shallow kappa overstates nu -> biases s up).

Usage: python llm_fitting/b_robustness.py <platform> <K> [B_boot]
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402


def b_at_h(df: pd.DataFrame, s1: float, h: int, min_changes: int = 8,
           detrend: bool = False, acf_lags: int = 2) -> float:
    """s(h)/s(1) at a FIXED horizon h (estimate_mix_b's kernel without the
    h* selection), so windows of different length stay comparable."""
    if s1 <= 0:
        return np.nan
    sub = df[df["period"] % h == 0].copy()
    sub["period"] //= h
    sh = mrd.estimate_temperament(sub, min_changes=min_changes,
                                  detrend=detrend, acf_lags=acf_lags)["s"]
    return float(np.clip(sh / s1, 0.0, 1.5))


def block_resample(df: pd.DataFrame, L: int, rng) -> pd.DataFrame:
    """Circular moving-block bootstrap over weeks with GAP RELABELING:
    block j's weeks get periods j*(L+2) + offset, so consecutive-period
    change pairs exist only within genuine blocks."""
    T = int(df["period"].max()) + 1
    n_blocks = int(np.ceil(T / L))
    parts = []
    for j in range(n_blocks):
        s = int(rng.integers(0, T - L + 1))
        blk = df[(df["period"] >= s) & (df["period"] < s + L)].copy()
        blk["period"] = blk["period"] - s + j * (L + 2)
        parts.append(blk)
    return pd.concat(parts, ignore_index=True)


def main() -> None:
    platform = sys.argv[1] if len(sys.argv) > 1 else "reddit_comments"
    K = int(sys.argv[2]) if len(sys.argv) > 2 else 12500
    B = int(sys.argv[3]) if len(sys.argv) > 3 else 200
    cfg = mrd.PLATFORMS[platform]
    df = mrd.restrict_universe(mrd.load_panel(cfg), K, buffer_mult=4)
    T = int(df["period"].max()) + 1
    hstar = next((h for h in (13, 8, 4) if T // h >= 9), 4)
    print(f"=== b robustness: {platform} K={K} T={T} h*={hstar} B_boot={B} ===")

    # 1+2: baseline vs detrended (s1 measured under the SAME convention as b)
    s1 = mrd.estimate_temperament(df)["s"]
    b0 = mrd.estimate_mix_b(df, s1)
    s1_d = mrd.estimate_temperament(df, detrend=True)["s"]
    b_d = mrd.estimate_mix_b(df, s1_d, detrend=True)
    print(f"baseline : s(1)={s1:.3f}  b={b0:.3f}")
    print(f"detrended: s(1)={s1_d:.3f}  b={b_d:.3f}   (u ~ a + c*t per entity; "
          f"arc-linear component removed)")

    # 3: split-window (halves; h* of the half + common h=4)
    half = T // 2
    first = df[df["period"] < half]
    second = df[df["period"] >= half].copy()
    second["period"] -= half
    for name, sub in (("first-half", first), ("second-half", second)):
        Th = int(sub["period"].max()) + 1
        hh = next((h for h in (13, 8, 4) if Th // h >= 9), 4)
        s1_h = mrd.estimate_temperament(sub)["s"]
        print(f"{name:11s}: s(1)={s1_h:.3f}  b(h*={hh})={b_at_h(sub, s1_h, hh):.3f}"
              f"  b(h=4)={b_at_h(sub, s1_h, 4):.3f}")
    print(f"full-window b(h=4)={b_at_h(df, s1, 4):.3f}   (common-h reference)")

    # 4: moving-block bootstrap over weeks (b at fixed h*)
    rng = np.random.default_rng(0)
    L = 3 * hstar
    vals = []
    for i in range(B):
        rs = block_resample(df, L, rng)
        s1_b = mrd.estimate_temperament(rs)["s"]
        if s1_b <= 0:
            continue
        vals.append(b_at_h(rs, s1_b, hstar, min_changes=6))
        if (i + 1) % 25 == 0:
            print(f"  boot {i + 1}/{B}: running b median {np.nanmedian(vals):.3f}")
    v = np.array([x for x in vals if np.isfinite(x)])
    print(f"block-bootstrap (weeks, L={L}): b median={np.median(v):.3f}  "
          f"95% CI [{np.percentile(v, 2.5):.3f}, {np.percentile(v, 97.5):.3f}]  "
          f"(n={len(v)})")

    # 5: kappa_acf lag-depth sensitivity (review B7)
    print("kappa_acf sensitivity (pooled-ACF depth -> kappa, s):")
    for lags in range(1, 7):
        t = mrd.estimate_temperament(df, acf_lags=lags)
        print(f"  lags 1..{lags}: kappa={t['kappa']:.4f}  s={t['s']:.4f}")


if __name__ == "__main__":
    main()
