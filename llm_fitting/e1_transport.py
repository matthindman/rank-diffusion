#!/usr/bin/env python3
"""E1 runner — parameter transport, with a machine-readable frozen reference
(protocol §3 E1 + A6.2).

--make-reference (run BEFORE extension access; reads only the T=136 panel):
    computes the four registered quantities on `reddit_comments`
    (K=12,500 / B=50,000 universe, registered stack, A4 NNLS estimator) and
    writes `e1_reference.json` + its SHA-256:
      s        temperament (min_changes=12)
      b8       mix exponent at the FROZEN horizon h=8 (A6.2: the recorded
               1.08 was h*=13-specific; a 77-week segment auto-selects h*=8,
               so both sides are frozen at h=8; reference from the recorded
               s(h) curve is ~1.016)
      kappa    md6 kappa(z) pooled to a 12-band summary + head/mid/deep
               thirds (bands 1-4 / 5-8 / 9-12)
      specb    centered Spec-B floor, 12 bands (z, sigma_obs)

--score T0 (the E1 evaluation, run on the extended panel AFTER the intake
    gate): same quantities on the extension segment (periods >= T0,
    re-indexed; membership = full-extended-window per A5), scored against
    the frozen reference:
      s  in [0.64, 0.74];  b8 in [0.95, 1.15];
      kappa: head third strictly the most persistent (head < min(mid, deep));
      Spec-B within +/-25% at all 12 extension z-coordinates (reference
      interpolated to them).
    E1 passes only if ALL FOUR pass.  A block-bootstrap CI for s is
    reported (non-gating, A6.2).

Usage:
  python llm_fitting/e1_transport.py --make-reference
  python llm_fitting/e1_transport.py --score 136 --platform reddit_comments_ext
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

REF_PATH = "llm_fitting/e1_reference.json"
K, BUF = 12_500, 4
B_H = 8
S_BAND = (0.64, 0.74)
B_BAND = (0.95, 1.15)
SPECB_TOL = 0.25


def b_at_h(df, s1: float, h: int = B_H, min_changes: int = 8) -> float:
    """estimate_mix_b's h-block at a FIXED horizon (A6.2 freeze)."""
    if s1 <= 0:
        return 0.0
    sub = df[df["period"] % h == 0].copy()
    sub["period"] //= h
    sh = mrd.estimate_temperament(sub, min_changes=min_changes)["s"]
    return float(np.clip(sh / s1, 0.0, 1.5))


def kappa_bands12(kappa_z: np.ndarray) -> list[float]:
    """Pool the knot-level kappa curve into 12 contiguous equal-count bands."""
    chunks = np.array_split(np.asarray(kappa_z, dtype=float), 12)
    return [float(np.mean(c)) for c in chunks]


def kappa_thirds(bands12) -> tuple[float, float, float]:
    b = np.asarray(bands12, dtype=float)
    return float(b[0:4].mean()), float(b[4:8].mean()), float(b[8:12].mean())


def kappa_orientation_ok(bands12) -> bool:
    """A6.2 (corrected against the frozen reference's own dry run): the HEAD
    third is strictly the most persistent — head kappa < min(mid, deep).
    No ordering imposed between mid and deep (their difference is within
    estimation noise in the reference: 0.0198 vs 0.0191)."""
    h, m, d = kappa_thirds(bands12)
    return min(m, d) > h + 1e-9


def _quantities(df, tag: str) -> dict:
    import spec_b_sigma_obs as sb
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
                     mix_hetero=True)
    daily = sb.load_daily(set(df["entity_id"].unique()),
                          path=mrd.PLATFORMS["reddit_comments"]["daily_path"],
                          day_guard=False)
    cur = sb.spec_b_curve(df, daily)
    b12 = kappa_bands12(p.kappa_z)
    h, m, d = kappa_thirds(b12)
    return dict(tag=tag, s=float(p.temper_s), b8=b_at_h(df, p.temper_s),
                kappa_bands12=b12, kappa_head=h, kappa_mid=m, kappa_deep=d,
                specb_z=[float(z) for z in cur["z"]],
                specb_sigma=[float(x) for x in cur["sigma_obs"]])


def make_reference() -> None:
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments"])
    df = mrd.restrict_universe(df, K, buffer_mult=BUF)
    q = _quantities(df, "reference_T136_nnls")
    q["estimator"] = "NNLS (A4 re-freeze default)"
    q["stack"] = "temper min_knot 8 md6 t-tails mix (registered E1)"
    Path(REF_PATH).write_text(json.dumps(q, indent=1))
    sha = hashlib.sha256(Path(REF_PATH).read_bytes()).hexdigest()
    print(f"wrote {REF_PATH}  sha256: {sha}")
    print(f"  s={q['s']:.4f}  b8={q['b8']:.4f}  "
          f"kappa head/mid/deep={q['kappa_head']:.4f}/{q['kappa_mid']:.4f}/"
          f"{q['kappa_deep']:.4f}")
    print(f"  specb {q['specb_sigma'][0]:.3f}..{q['specb_sigma'][-1]:.3f}")


def score(platform: str, t0: int, boot: int = 100) -> None:
    ref = json.loads(Path(REF_PATH).read_text())
    df = mrd.load_panel(mrd.PLATFORMS[platform])
    df = mrd.restrict_universe(df, K, buffer_mult=BUF)   # A5: full-window
    seg = df[df["period"] >= t0].copy()
    seg["period"] -= t0
    q = _quantities(seg, f"extension_T0={t0}")

    ok_s = S_BAND[0] <= q["s"] <= S_BAND[1]
    ok_b = B_BAND[0] <= q["b8"] <= B_BAND[1]
    ok_k = kappa_orientation_ok(q["kappa_bands12"])
    ref_interp = np.interp(q["specb_z"], ref["specb_z"], ref["specb_sigma"])
    rel = np.abs(np.array(q["specb_sigma"]) - ref_interp) / np.clip(ref_interp, 1e-9, None)
    ok_f = bool(np.all(rel <= SPECB_TOL))

    # non-gating: moving-block bootstrap CI for s over segment weeks (A6.2)
    rng = np.random.default_rng(0)
    T = int(seg["period"].max()) + 1
    L = max(4, T // 8)
    svals = []
    for _ in range(boot):
        starts = rng.integers(0, T - L + 1, size=int(np.ceil(T / L)))
        periods = np.concatenate([np.arange(s0, s0 + L) for s0 in starts])[:T]
        rs = seg[seg["period"].isin(set(periods.tolist()))]
        try:
            svals.append(mrd.estimate_temperament(rs, min_changes=12)["s"])
        except Exception:
            continue
    ci = np.percentile(svals, [2.5, 97.5]) if svals else (np.nan, np.nan)

    print(f"E1 transport, segment T0={t0} (T_seg={T}):")
    print(f"  s   = {q['s']:.4f}  band {S_BAND}  -> {'PASS' if ok_s else 'FAIL'}"
          f"   [block-boot CI {ci[0]:.3f}, {ci[1]:.3f}; sub-window ref 0.64-0.67 — non-gating]")
    print(f"  b8  = {q['b8']:.4f}  band {B_BAND}  -> {'PASS' if ok_b else 'FAIL'}"
          f"   (frozen h=8 both sides; ref {ref['b8']:.4f})")
    print(f"  kappa head/mid/deep = {q['kappa_head']:.4f}/{q['kappa_mid']:.4f}/"
          f"{q['kappa_deep']:.4f}  (ref {ref['kappa_head']:.4f}/"
          f"{ref['kappa_mid']:.4f}/{ref['kappa_deep']:.4f})  "
          f"head-most-persistent -> {'PASS' if ok_k else 'FAIL'}")
    print(f"  specb max band rel dev = {rel.max():.3f}  (tol {SPECB_TOL}) "
          f"-> {'PASS' if ok_f else 'FAIL'}")
    print(f"E1 VERDICT: {'PASS (all four)' if all([ok_s, ok_b, ok_k, ok_f]) else 'FAIL'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--make-reference", action="store_true")
    g.add_argument("--score", type=int, metavar="T0")
    ap.add_argument("--platform", default="reddit_comments_ext")
    a = ap.parse_args()
    if a.make_reference:
        make_reference()
    else:
        score(a.platform, a.score)
