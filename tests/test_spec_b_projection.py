"""Spec-B projection identification (2026-07-05 P0 adjudication).

The Toeplitz bases sum to the all-ones matrix J and the week-mean centering
annihilates J (CJC = 0), so the centered residual covariance leaves one
Toeplitz direction unidentified: Sigma and Sigma + delta*J are observationally
equivalent after centering, while the uncentered floor p'Sig p differs by
delta.  The invariant (and pinned) quantity is the centered floor p'CSigCp.
These tests pin that algebra and the spec_b_curve convention switch.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import spec_b_sigma_obs as sb  # noqa: E402

C7 = np.eye(7) - np.ones((7, 7)) / 7


def _toeplitz(c):
    Sig = np.empty((7, 7))
    for i in range(7):
        for j in range(7):
            Sig[i, j] = c[abs(i - j)]
    return Sig


def _residuals_with_cov(M, n=7000):
    """Rows R (n x 7) with R'R/n == M exactly (M psd)."""
    w, U = np.linalg.eigh(M)
    w = np.clip(w, 0.0, None)
    L = U @ np.diag(np.sqrt(w))          # M = L L'
    base = np.sqrt(7.0) * L.T            # 7 rows, base'base = 7 M
    reps = n // 7
    return np.vstack([base] * reps)      # R'R/n = M


def test_common_component_is_invisible_after_centering_and_centered_floor_invariant():
    rng = np.random.default_rng(0)
    c = np.array([1.0, -0.1, -0.08, -0.05, 0.0, 0.0, 0.0]) * 0.4
    Sig = _toeplitz(c)
    delta = 0.25                          # within-week common component
    SigJ = Sig + delta * np.ones((7, 7))
    p = rng.dirichlet(np.ones(7) * 5.0, size=64)

    out, outJ = [], []
    for S, acc in ((Sig, out), (SigJ, outJ)):
        R = _residuals_with_cov(C7 @ S @ C7)   # what week-mean centering leaves
        acc.extend(sb._toeplitz_floor(R, p))
    sig_d, fl_leg, fl_cent = out
    sig_dJ, fl_legJ, fl_centJ = outJ

    # the two DGPs are observationally equivalent after centering: every
    # reported quantity must agree (the estimator cannot see delta*J)
    assert fl_cent == pytest.approx(fl_centJ, rel=1e-6)
    assert fl_leg == pytest.approx(fl_legJ, rel=1e-6)

    # the centered floor equals the true invariant p'CSigCp (same for both
    # DGPs by construction); the legacy floor cannot equal the true p'Sig p
    # for both, since those differ by delta -- non-identification made exact
    true_cent = float(np.mean(np.einsum("nj,jk,nk->n", p, C7 @ Sig @ C7, p)))
    truth_leg = float(np.mean(np.einsum("nj,jk,nk->n", p, Sig, p)))
    truth_legJ = float(np.mean(np.einsum("nj,jk,nk->n", p, SigJ, p)))
    assert fl_cent == pytest.approx(true_cent, rel=1e-6)
    assert truth_legJ - truth_leg == pytest.approx(delta, rel=1e-6)
    assert not (fl_leg == pytest.approx(truth_leg, rel=1e-3)
                and fl_leg == pytest.approx(truth_legJ, rel=1e-3))


def _tiny_panel(n_ent=120, n_weeks=10, seed=1):
    rng = np.random.default_rng(seed)
    ents = [f"e{i}" for i in range(n_ent)]
    weeks = pd.date_range("2020-01-06", periods=n_weeks, freq="7D")
    drows, urows = [], []
    for wi, w in enumerate(weeks):
        vals = {}
        for ei, e in enumerate(ents):
            lvl = 1000.0 / (ei + 1)
            day_vals = lvl * np.exp(rng.normal(0, 0.3, size=7))
            for d in range(7):
                drows.append((w + pd.Timedelta(days=d), e, day_vals[d]))
            vals[e] = day_vals.sum()
        order = sorted(vals, key=vals.get, reverse=True)
        for r, e in enumerate(order, start=1):
            urows.append((wi, w, e, r))
    daily = pd.DataFrame(drows, columns=["date", "endpoint_id", "metric_value"])
    daily["date"] = pd.to_datetime(daily["date"])
    daily["week"] = daily["date"] - pd.to_timedelta(daily["date"].dt.weekday, unit="D")
    daily["dow"] = daily["date"].dt.weekday
    uni = pd.DataFrame(urows, columns=["period", "ts", "entity_id", "rank"])
    return uni, daily


def test_spec_b_curve_floor_convention_switch():
    uni, daily = _tiny_panel()
    cent = sb.spec_b_curve(uni, daily, n_bands=1)
    leg = sb.spec_b_curve(uni, daily, n_bands=1, floor="legacy")
    t = cent["table"]
    assert np.allclose(cent["sigma_obs"], t["sigma_obsB_cent"].to_numpy())
    assert np.allclose(leg["sigma_obs"], t["sigma_obsB"].to_numpy())
    # real (mildly mean-reverting) residuals: conventions must actually differ
    assert not np.allclose(cent["sigma_obs"], leg["sigma_obs"])
    with pytest.raises(ValueError):
        sb.spec_b_curve(uni, daily, n_bands=1, floor="nope")
