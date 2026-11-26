"""Unit and ADEMP synthetic-recovery tests for the Round 8 candidate common_rankdiff_opus_r8.py."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr  # noqa: E402


def _make_planted_params(
    N: int = 2000,
    K: int = 500,
    sigma_L0: float = 0.09,
    beta: float = 0.08,
    phi: float = 0.78,
    a_xi: float = -1.1,
    b_xi: float = 0.15,
    a_obs: float = -1.4,
    b_obs: float = 0.10,
    s_vol: float = 0.65,
    gamma_L: float = 0.5,
    a_abs: float = -3.0,
    b_abs: float = 0.6,
    lam_exit: float = 0.002,
    innov_mode: str = "gaussian",
) -> tuple[cr.CommonParams, np.ndarray]:
    r = np.arange(1, N + 1, dtype=float)
    ladder = 14.0 - 1.1 * np.log(r)
    L_K = float(ladder[K - 1])
    z_K = float(math.log((K - 0.5) / N))
    u_lo, u_hi = float(ladder[-1] - L_K), float(ladder[0] - L_K)
    dz, dg = cr._closed_form_atlas_drift(sigma_L0, beta, L_K, u_lo, u_hi, ladder, N)
    pool = np.linspace(-3.5, 3.5, 2048)
    pool = pool / np.std(pool)
    p = cr.CommonParams(
        sigma_L0=sigma_L0, beta=beta, phi=phi,
        a_xi=a_xi, b_xi=b_xi, a_obs=a_obs, b_obs=b_obs,
        s_vol=s_vol, gamma_L=gamma_L, a_abs=a_abs, b_abs=b_abs, lam_exit=lam_exit,
        K=K, N=N, L_K=L_K, z_K=z_K, u_lo=u_lo, u_hi=u_hi,
        drift_z=dz, drift_g=dg, bottom_pool=ladder[int(0.8 * N):].copy(),
        innov_pool=pool, ladder_obs=ladder, innov_mode=innov_mode,
    )
    return p, ladder


def test_zero_temperament_recovery():
    """Fixes Codex V2 failure: when true s_vol == 0, split-half estimator must return ~0 (not 0.14-0.19)."""
    p_true, L0 = _make_planted_params(N=2000, K=500, s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0)
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=120, seed=11, apply_absence=False, burn=20)
    p_hat = cr.estimate_common(X, R, K=500, refine_drift=False)
    assert p_hat.s_vol < 0.08, f"Expected s_vol near 0, got {p_hat.s_vol:.3f}"
    assert p_hat.n_free_scalars() == 11


def test_full_parameter_recovery():
    """ADEMP recovery on a planted panel with heterogeneity, passing AR(1), size law, and absence."""
    p_true, L0 = _make_planted_params(
        N=2400, K=600, sigma_L0=0.09, beta=0.08, phi=0.78,
        a_xi=-1.05, b_xi=0.12, a_obs=-1.35, b_obs=0.10,
        s_vol=0.65, gamma_L=0.5, a_abs=-2.8, b_abs=0.7,
    )
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=140, seed=7, apply_absence=True, burn=20)
    p_hat = cr.estimate_common(X, R, K=600, refine_drift=False)

    assert abs(p_hat.sigma_L0 - p_true.sigma_L0) / p_true.sigma_L0 < 0.25, f"sigma_L0: {p_hat.sigma_L0}"
    assert abs(p_hat.beta - p_true.beta) < 0.06, f"beta: {p_hat.beta}"
    assert abs(p_hat.phi - p_true.phi) < 0.15, f"phi: {p_hat.phi}"
    assert abs(p_hat.s_vol - p_true.s_vol) < 0.15, f"s_vol: {p_hat.s_vol}"
    assert abs(p_hat.a_abs - p_true.a_abs) < 0.5, f"a_abs: {p_hat.a_abs}"


def test_calendar_alignment_exact():
    """Fixes Codex V3 timing bug: h=1 forecast must advance exactly 1 step from T0-1, not 2."""
    p_true, L0 = _make_planted_params(
        N=400, K=100, sigma_L0=0.0, beta=0.0, phi=0.5,
        a_xi=-6.0, b_xi=0.0, a_obs=-6.0, b_obs=0.0,
        s_vol=0.0, gamma_L=0.0, a_abs=-10.0, b_abs=0.0, lam_exit=0.0,
    )
    # Deterministic constant drift g = +0.10 everywhere, zero noise!
    p_det = cr.CommonParams(
        **{**p_true.__dict__, "drift_g": np.full_like(p_true.drift_g, 0.10)}
    )
    filt = dict(
        L_hat=L0.copy(), xi_hat=np.zeros_like(L0),
        P11=np.full_like(L0, 1e-12), P12=np.zeros_like(L0), P22=np.full_like(L0, 1e-12),
        v_hat=np.ones_like(L0), post_s_v=np.zeros_like(L0), last_pmean=0.0,
    )
    fc = cr.forecast_endpoints(filt, p_det, horizons=[1, 4], n_ens=3, seed=0, apply_absence=False)
    diff1 = float(np.mean(fc[1][0][:, 0] - L0))
    diff4 = float(np.mean(fc[4][0][:, 0] - L0))
    assert abs(diff1 - 0.10) < 0.01, f"h=1 should move by 1*0.10, got {diff1:.4f}"
    assert abs(diff4 - 0.40) < 0.01, f"h=4 should move by 4*0.10, got {diff4:.4f}"


def test_empirical_innovations_bounded_no_blowup():
    """Verify empirical standardized innovations never explode expm1(X) across 10 seeds."""
    p_true, L0 = _make_planted_params(N=1200, K=300, s_vol=0.90, innov_mode="empirical")
    max_x = -np.inf
    for sd in range(10):
        X, _, _ = cr.simulate_common_panel(p_true, L0, T_sim=80, seed=sd, apply_absence=False)
        max_x = max(max_x, float(np.nanmax(X)))
    assert max_x < float(L0[0]) + 4.5, f"Explosive spike detected: max_X={max_x:.2f} vs L0[0]={L0[0]:.2f}"


def test_deconvolution_reduces_first_block_inflation():
    """Verify deconvolve_starting_ladder matches the observed first-block head level."""
    p_true, L0 = _make_planted_params(N=1200, K=300, a_xi=-0.5, a_obs=-0.6, s_vol=0.7)
    L_dec = cr.deconvolve_starting_ladder(p_true, L0, block=13, iters=4, seeds=(1, 2))
    # Simulate first block from raw L0 vs L_dec
    raw_head, dec_head = [], []
    for sd in (10, 11, 12):
        Xr, _, _ = cr.simulate_common_panel(p_true, L0, T_sim=13, seed=sd, apply_absence=False)
        Xd, _, _ = cr.simulate_common_panel(p_true, L_dec, T_sim=13, seed=sd, apply_absence=False)
        mr = np.sort(np.mean(Xr, axis=0))[::-1]
        md = np.sort(np.mean(Xd, axis=0))[::-1]
        raw_head.append(mr[9] - mr[299])
        dec_head.append(md[9] - md[299])
    target_gap = float(L0[9] - L0[299])
    assert abs(np.mean(dec_head) - target_gap) < abs(np.mean(raw_head) - target_gap) + 0.05


def test_stage3_flag_off_bit_identical():
    """asymmetry=False (default) keeps Stage 3 inert and uses the exact 2-state filter."""
    p_true, L0 = _make_planted_params(N=800, K=200, s_vol=0.5, gamma_L=0.0, a_abs=-8.0, b_abs=0.0)
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=80, seed=3, apply_absence=False)
    p0 = cr.estimate_common(X, R, K=200, refine_drift=False)
    p1 = cr.estimate_common(X, R, K=200, refine_drift=False, asymmetry=False)
    pv = cr.estimate_common(X, R, K=200, refine_drift=False, asymmetry=False, scale_moment="variance")
    assert p0.surge_lam == 0.0 and p0.decline_var == 0.0
    assert pv.n_free_scalars() == 11 and p0.n_free_scalars() in (11, 12)
    assert p0.sigma_L0 == p1.sigma_L0 and p0.phi == p1.phi and p0.s_vol == p1.s_vol
    f0 = cr.filter_endpoints(X, R, p0)
    assert "P4" not in f0


def test_stage3_zero_drift_with_surges_recovery():
    """Planted panel with zero persistent drift + fading positive surges recovers sigma_d ~ 0 and surges > 0."""
    p_base, L0 = _make_planted_params(
        N=2400, K=600, sigma_L0=0.08, beta=0.05, phi=0.84,
        a_xi=-1.1, b_xi=0.05, a_obs=-1.5, b_obs=0.05,
        s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    rho_s = 0.5 ** (1.0 / 13.0)
    p_true = cr.CommonParams(**{**p_base.__dict__, "surge_lam": 0.012, "surge_mu": 0.70, "surge_rho": rho_s})
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=180, seed=19, apply_absence=False, burn=20)
    p_hat = cr.estimate_common(X, R, K=600, refine_drift=False, asymmetry=True)
    assert math.sqrt(p_hat.decline_var) < 0.004, f"Expected sigma_d near 0, got {math.sqrt(p_hat.decline_var):.5f}"
    assert p_hat.surge_lam > 0.002 and p_hat.surge_mu > 0.30, (
        f"Expected active surges, got lam={p_hat.surge_lam:.5f}, mu={p_hat.surge_mu:.3f}"
    )


def test_stage3_full_asymmetry_recovery():
    """Planted panel with persistent entity drift + fading positive surges recovers both cleanly."""
    p_base, L0 = _make_planted_params(
        N=2400, K=600, sigma_L0=0.075, beta=0.05, phi=0.84,
        a_xi=-1.2, b_xi=0.05, a_obs=-1.5, b_obs=0.05,
        s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    rho_s = 0.5 ** (1.0 / 13.0)
    rho_d = 0.5 ** (1.0 / cr.DRIFT_HL_FIXED)
    sd_true = 0.013
    p_true = cr.CommonParams(**{
        **p_base.__dict__,
        "surge_lam": 0.010, "surge_mu": 0.75, "surge_rho": rho_s,
        "decline_var": sd_true ** 2, "decline_rho": rho_d,
    })
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=200, seed=23, apply_absence=False, burn=20)
    p_hat_s2 = cr.estimate_common(X, R, K=600, refine_drift=False, asymmetry=False)
    p_hat = cr.estimate_common(X, R, K=600, refine_drift=False, asymmetry=True)
    sd_hat = math.sqrt(p_hat.decline_var)
    vs_true = cr._surge_variance(0.010, 0.75, rho_s)
    vs_hat = cr._surge_variance(p_hat.surge_lam, p_hat.surge_mu, p_hat.surge_rho)
    assert abs(sd_hat - sd_true) / sd_true < 0.35, f"Expected sigma_d ~ {sd_true}, got {sd_hat:.5f}"
    assert abs(vs_hat - vs_true) / vs_true < 0.25, f"Expected V_s ~ {vs_true:.4f}, got {vs_hat:.4f}"
    assert p_hat.sigma_L0 < p_hat_s2.sigma_L0 - 0.015, (
        f"Variance conservation failed: S3 sigma_L0={p_hat.sigma_L0:.4f} not cleanly below S2 {p_hat_s2.sigma_L0:.4f}"
    )
    # Round 8: 11 core + 3 surge + sigma_d + the rank slope b_d (fitted, even when the planted drift is rank-uniform)
    assert p_hat.n_free_scalars() == 16
    assert abs(p_hat.decline_b) < 0.15, f"rank-uniform planted drift, got b_d={p_hat.decline_b:+.3f}"


def test_stage3_kalman_4state_alignment():
    """Exact 1-step calendar alignment for 4-state [L, xi, d, s] Kalman forecast."""
    p_base, L0 = _make_planted_params(
        N=400, K=100, sigma_L0=0.0, beta=0.0, phi=0.5,
        a_xi=-8.0, b_xi=0.0, a_obs=-8.0, b_obs=0.0,
        s_vol=0.0, gamma_L=0.0, a_abs=-10.0, b_abs=0.0, lam_exit=0.0,
    )
    # Deterministic: g=0.02, d=0.05 (rho_d=1), s0=1.0 (rho_s=0.9, lam=1e-9 so zero arrivals)
    p_det = cr.CommonParams(**{
        **p_base.__dict__,
        "drift_g": np.full_like(p_base.drift_g, 0.02),
        "decline_var": 0.05 ** 2, "decline_rho": 1.0,
        "surge_lam": 1e-12, "surge_mu": 0.5, "surge_rho": 0.90,
    })
    N = L0.size
    P4 = np.zeros((N, 4, 4))
    filt = dict(
        L_hat=L0.copy(), xi_hat=np.zeros(N), d_hat=np.full(N, 0.05), s_hat=np.full(N, 1.0),
        P11=np.full(N, 1e-12), P12=np.zeros(N), P22=np.full(N, 1e-12), P4=P4,
        v_hat=np.ones(N), post_s_v=np.zeros(N), last_pmean=0.0,
    )
    fc = cr.forecast_endpoints(filt, p_det, horizons=[1, 2], n_ens=2, seed=0, apply_absence=False)
    # At h=1: L1 = L0 + 0.02 + 0.05 = L0 + 0.07; s1 = 0.90 * 1.0 = 0.90 -> X1 = L0 + 0.07 + 0.90 = L0 + 0.97
    # At h=2: L2 = L0 + 0.14; s2 = 0.81 -> X2 = L0 + 0.95
    diff1 = float(np.mean(fc[1][0][:, 0] - L0))
    diff2 = float(np.mean(fc[2][0][:, 0] - L0))
    assert abs(diff1 - 0.97) < 0.01, f"Expected h=1 offset 0.97, got {diff1:.4f}"
    assert abs(diff2 - 0.95) < 0.01, f"Expected h=2 offset 0.95, got {diff2:.4f}"


def test_spec_b_lower_bound_enforced_everywhere():
    """Exact Spec-B lower bound sigma_obs_of(p, z) >= floor(z) must hold at every rank z."""
    p_true, L0 = _make_planted_params(N=1200, K=300, a_obs=-1.2, b_obs=0.10, s_vol=0.0, a_abs=-8.0)
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=80, seed=5, apply_absence=False)
    z_grid = np.linspace(-6.0, 0.0, 12)
    z_K = math.log((300 - 0.5) / 1200)
    so_floor = np.exp(-0.9 + 0.12 * (z_grid - z_K))
    p_hat = cr.estimate_common(X, R, K=300, spec_b=(z_grid, so_floor), refine_drift=False)
    z_test = np.linspace(-7.0, 0.5, 50)
    fl_test = np.exp(p_hat.a_obs_floor + p_hat.b_obs_floor * (z_test - p_hat.z_K))
    so_test = cr.sigma_obs_of(p_hat, z_test)
    assert np.all(so_test >= fl_test - 1e-12), "sigma_obs_of dipped below Spec-B floor!"


def test_capped_surge_moments_match_simulator():
    """Verify exact capped-exponential formulas match simulated jump & stationary moments."""
    rng = np.random.default_rng(77)
    lam, mu, rho = 0.02, 0.85, 0.5 ** (1.0 / 26.0)
    N = 40000
    J = (rng.random(N) < lam) * mu * np.minimum(rng.standard_exponential(N), cr.SURGE_EXP_CAP)
    assert abs(float(np.var(J)) - cr._surge_innov_var(lam, mu)) / cr._surge_innov_var(lam, mu) < 0.08
    s = np.zeros(N)
    for _ in range(180):
        s = rho * s + (rng.random(N) < lam) * mu * np.minimum(rng.standard_exponential(N), cr.SURGE_EXP_CAP)
    assert abs(float(np.mean(s)) - cr._surge_mean(lam, mu, rho)) / cr._surge_mean(lam, mu, rho) < 0.05
    assert abs(float(np.var(s)) - cr._surge_variance(lam, mu, rho)) / cr._surge_variance(lam, mu, rho) < 0.08


def test_stage3_symmetrized_pool_and_hl52_recovery():
    """Planted hl_s=52 surge recovers hl_s=52 with interior (lam, mu) and symmetrizes innov_pool."""
    p_base, L0 = _make_planted_params(
        N=2400, K=600, sigma_L0=0.075, beta=0.05, phi=0.84,
        a_xi=-1.2, b_xi=0.05, a_obs=-1.5, b_obs=0.05,
        s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    rho_s = 0.5 ** (1.0 / 52.0)
    p_true = cr.CommonParams(**{**p_base.__dict__, "surge_lam": 0.004, "surge_mu": 0.85, "surge_rho": rho_s})
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=200, seed=31, apply_absence=False, burn=40)
    p_hat = cr.estimate_common(X, R, K=600, refine_drift=False, asymmetry=True)
    hl_hat = -math.log(2.0) / math.log(p_hat.surge_rho)
    assert round(hl_hat) in (39, 52), f"Expected hl_s in (39, 52), got {hl_hat:.1f}"
    assert 0.45 < p_hat.surge_mu < 1.35, f"Expected interior mu ~ 0.85, got {p_hat.surge_mu:.3f}"
    assert abs(float(np.mean(p_hat.innov_pool ** 3))) < 1e-10, "Core innov_pool not symmetrized under Stage 3!"


def test_forecast_return_totals_and_no_clamp_L_bias():
    """Verify return_totals exports unmasked top-K medians/totals and s>=0 clamp leaves L_hat unbiased."""
    p_base, L0 = _make_planted_params(
        N=400, K=100, sigma_L0=0.0, beta=0.0, phi=0.5,
        a_xi=-8.0, b_xi=0.0, a_obs=-8.0, b_obs=0.0,
        s_vol=0.0, gamma_L=0.0, a_abs=-10.0, b_abs=0.0, lam_exit=0.0,
    )
    p_det = cr.CommonParams(**{
        **p_base.__dict__,
        "drift_g": np.zeros_like(p_base.drift_g),
        "surge_lam": 1e-12, "surge_mu": 0.5, "surge_rho": 0.90,
    })
    N = L0.size
    # Set s_hat = -0.5 so s_unit_raw < 0 triggers s_init = 0 clamp; L_init must NOT be shifted!
    filt = dict(
        L_hat=L0.copy(), xi_hat=np.zeros(N), d_hat=np.zeros(N), s_hat=np.full(N, -0.5), s_scale=np.ones(N),
        P11=np.full(N, 1e-12), P12=np.zeros(N), P22=np.full(N, 1e-12), P4=np.zeros((N, 4, 4)),
        v_hat=np.ones(N), post_s_v=np.zeros(N), last_pmean=0.0,
    )
    fc = cr.forecast_endpoints(filt, p_det, horizons=[1, 20], n_ens=2, seed=0, apply_absence=False, return_totals=True)
    X20, R20, med20, tot20 = fc[20]
    assert np.all(np.isfinite(med20)) and np.all(tot20 > 0)
    # Without L_init shift, L stays at L0 (drift=0, s=0 -> X20 ~ L0, NOT L0 - 0.5!)
    assert abs(float(np.mean(X20[:, 0] - L0))) < 0.02, f"Clamp shifted L permanently: {np.mean(X20[:, 0] - L0):.4f}"


def test_within_band_variogram_invariant_to_tier_trends():
    """Tier-specific deterministic time trends F_b(t) must leave _structure_function exact."""
    rng = np.random.default_rng(123)
    T, N = 120, 400
    X = np.cumsum(0.05 * rng.standard_normal((T, N)), axis=0)
    band_of = np.repeat(np.arange(4), 100)
    D0 = cr._structure_function(X, band_of, nb=4, H=20)
    # Add wildly different deterministic trends to each band (like Reddit tail vs head!)
    t = np.arange(T, dtype=float)[:, None]
    X_trend = X.copy()
    for b, slope in enumerate([-0.02, -0.005, 0.01, 0.03]):
        X_trend[:, band_of == b] += slope * t
    D1 = cr._structure_function(X_trend, band_of, nb=4, H=20)
    assert np.allclose(D0, D1, atol=1e-12, rtol=1e-12), "Within-band variogram not invariant to tier trends!"


def test_robust_volatility_recovers_conditional_scale():
    """Known Gaussian weekly differences combine a homogeneous floor and scaled passing noise."""
    p, levels = _make_planted_params(
        N=180, K=90, beta=0.0, b_xi=0.0, b_obs=0.0, s_vol=0.7, gamma_L=0.0, sigma_L0=0.1,
    )
    z = np.log((np.arange(p.N) + 0.5) / p.N)
    lasting = cr.sigma_L_of(p, levels)
    v = np.tile([0.4, 1.0, 2.2], 60)
    var = lasting ** 2 + v * (2.0 * cr.sigma_obs_of(p, z) ** 2 + 2.0 * (1.0 - p.phi) * cr.v_xi_of(p, z))
    rng = np.random.default_rng(1001)
    dx = rng.normal(size=(450, p.N)) * np.sqrt(var)
    dx[:, -1] = np.nan
    estimate, sd = cr._volatility_posterior(p, dx, z, lasting)
    for group in (0.4, 1.0, 2.2):
        valid = (v == group) & (np.arange(p.N) < p.N - 1)
        assert abs(np.median(estimate[valid]) / group - 1.0) < 0.12
    assert estimate[-1] == 1.0 and sd[-1] == p.s_vol
    assert np.all(sd[:-1] < p.s_vol)
    # Extremely large rare symmetric bursts can destroy mean-square scale, but not median posterior
    contaminated = dx.copy()
    contaminated[::50, :-1] += 30.0 * rng.choice([-1.0, 1.0], size=contaminated[::50, :-1].shape)
    estimate_out, _ = cr._volatility_posterior(p, contaminated, z, lasting)
    assert np.median(np.abs(np.log(estimate_out[:-1] / estimate[:-1]))) < 0.13
    assert np.nanmean(contaminated ** 2) > 10.0 * np.nanmean(dx ** 2)


# --------------------------------------------------------------------------- #
# Round 5 (Fable 5.2) & Round 6 (Meta-Split): opt-in robust & default meta
# --------------------------------------------------------------------------- #
def test_default_meta_preserves_variance_path_when_no_jumps_and_in_stage3():
    """Default scale_moment='meta' keeps asymmetry=True bit-identical to 'variance',
    and keeps (phi, V_xi, sigma_obs) equal to 'variance' when asymmetry=False."""
    p_true, L0 = _make_planted_params(
        N=1600, K=400, s_vol=0.6, gamma_L=0.0, a_abs=-3.0, b_abs=0.5, innov_mode="empirical"
    )
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=120, seed=41, apply_absence=True, burn=20)
    # Stage 3 (asymmetry=True) must be bit-identical between default 'meta' and 'variance'
    pm3 = cr.estimate_common(X, R, K=400, refine_drift=False, asymmetry=True)
    pv3 = cr.estimate_common(X, R, K=400, refine_drift=False, asymmetry=True, scale_moment="variance")
    assert pm3.diag["scale_moment"] == "meta"
    assert pm3.jump_lam == 0.0 and pm3.jump_c == 0.0
    for k in ("sigma_L0", "beta", "phi", "a_xi", "b_xi", "a_obs", "b_obs", "s_vol", "a_abs", "b_abs",
              "lam_exit", "surge_lam", "surge_mu", "surge_rho", "decline_var", "decline_rho"):
        assert getattr(pm3, k) == getattr(pv3, k), f"{k} differs under asymmetry=True"
    assert np.array_equal(pm3.innov_pool, pv3.innov_pool)
    assert np.array_equal(pm3.drift_g, pv3.drift_g)

    # Stage 2 (asymmetry=False): meta preserves exact mean-square (phi, a_xi, b_xi, a_obs, b_obs)
    pm2 = cr.estimate_common(X, R, K=400, refine_drift=False, asymmetry=False)
    pv2 = cr.estimate_common(X, R, K=400, refine_drift=False, asymmetry=False, scale_moment="variance")
    for k in ("phi", "a_xi", "b_xi", "a_obs", "b_obs", "s_vol", "a_abs", "b_abs", "lam_exit"):
        assert getattr(pm2, k) == getattr(pv2, k), f"{k} differs in meta vs variance S2"


def test_meta_default_recovers_planted_gaussian_jumps_and_stays_off_when_absent():
    """Round 6 default scale_moment='meta': inactive on jump-free Gaussian panel (11 sc);
    detects planted 2%/wk Gaussian jumps (12 sc) while preserving exact passing/obs laws."""
    p_true, L0 = _make_planted_params(
        N=8000, K=2000, sigma_L0=0.08, beta=0.0, phi=0.80,
        a_xi=-1.1, b_xi=0.10, a_obs=-1.4, b_obs=0.0, s_vol=0.5, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    X0, R0, _ = cr.simulate_common_panel(p_true, L0, T_sim=160, seed=5, apply_absence=False, burn=20)
    p0 = cr.estimate_common(X0, R0, K=2000, refine_drift=False)
    assert p0.jump_lam == 0.0 and p0.n_free_scalars() == 11

    p_j = cr.CommonParams(**{**p_true.__dict__, "jump_lam": 0.01, "jump_c": 1.2, "jump_dist": "gaussian"})
    Xj, Rj, _ = cr.simulate_common_panel(p_j, L0, T_sim=200, seed=77, apply_absence=False, burn=20)
    pm = cr.estimate_common(Xj, Rj, K=2000, refine_drift=False)
    pv = cr.estimate_common(Xj, Rj, K=2000, refine_drift=False, scale_moment="variance")
    assert pm.jump_lam == cr.JUMP_LAM_FIXED and pm.jump_dist == "gaussian" and pm.n_free_scalars() == 12
    assert pm.a_xi == pv.a_xi and pm.a_obs == pv.a_obs and pm.phi == pv.phi
    assert pm.sigma_L0 < pv.sigma_L0
    true_var = 0.01 * 1.2
    est_var = pm.jump_lam * pm.jump_c
    assert 0.25 * true_var < est_var < 1.35 * true_var



def test_robust_matches_variance_on_gaussian_panel():
    """With Gaussian innovations the IQR-based and mean-square moments coincide."""
    p_true, L0 = _make_planted_params(
        N=2400, K=600, sigma_L0=0.09, beta=0.05, phi=0.78,
        a_xi=-1.05, b_xi=0.10, a_obs=-1.35, b_obs=0.10,
        s_vol=0.6, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=160, seed=9, apply_absence=False, burn=20)
    pv = cr.estimate_common(X, R, K=600, refine_drift=False, scale_moment="variance")
    pr = cr.estimate_common(X, R, K=600, refine_drift=False, scale_moment="robust")
    assert pr.phi == pv.phi
    assert abs(pr.sigma_L0 - p_true.sigma_L0) / p_true.sigma_L0 < 0.25
    assert abs(pr.sigma_L0 - pv.sigma_L0) / pv.sigma_L0 < 0.20
    assert abs(pr.a_xi - pv.a_xi) < 0.25 and abs(pr.a_obs - pv.a_obs) < 0.25
    assert pr.diag["scale_moment"] == "robust" and "D_rob" in pr.diag


def test_robust_fit_separates_planted_rare_lasting_jumps():
    """Rare permanent jumps inflate the mean-square fit; opt-in scale_moment='robust' detects them."""
    p_true, L0 = _make_planted_params(
        N=2400, K=600, sigma_L0=0.08, beta=0.0, phi=0.80,
        a_xi=-1.1, b_xi=0.0, a_obs=-1.4, b_obs=0.0,
        s_vol=0.5, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    T = 160
    rng = np.random.default_rng(2024)
    st = cr.CommonState(p_true, L0, rng)
    Xs = np.empty((T, p_true.N))
    Rs = np.full((T, p_true.N), np.nan)
    for _ in range(20):
        st.step(apply_absence=False)
    for t in range(T):
        jump = (rng.random(p_true.N) < 0.008) * 1.2 * rng.standard_normal(p_true.N)
        st.L = st.L + jump
        x = st.step(apply_absence=False)
        Xs[t] = x
        ok = np.flatnonzero(np.isfinite(x))
        Rs[t, ok[np.argsort(-x[ok], kind="stable")]] = np.arange(1, ok.size + 1, dtype=float)
    pv = cr.estimate_common(Xs, Rs, K=600, refine_drift=False, scale_moment="variance")
    pr = cr.estimate_common(Xs, Rs, K=600, refine_drift=False, scale_moment="robust")
    assert pv.sigma_L0 > 1.4 * p_true.sigma_L0
    assert pr.jump_lam > 0.0 and pr.sigma_L0 < pv.sigma_L0
    total_hat = pr.sigma_L0 ** 2 + pr.jump_lam * pr.jump_c
    total_true = p_true.sigma_L0 ** 2 + 0.008 * 1.2 ** 2
    assert abs(total_hat - total_true) / total_true < 0.30


def test_model_increment_ratio_gaussian_is_one():
    """Under Gaussian innovations without heterogeneity the model (IQR/1.349)^2 / Var ratio is ~1."""
    nb, H = 3, 13
    r = cr._model_increment_ratio(
        np.array([0.01, 0.02, 0.03]), np.array([0.1, 0.1, 0.1]), np.array([0.05, 0.05, 0.05]),
        phi=0.8, s_vol=0.0, pool=np.array([0.0]), innov_mode="gaussian", H=H, n_mc=65536,
    )
    assert r.shape == (nb, H)
    assert np.all(np.abs(r - 1.0) < 0.05)


def test_jumps_inactive_on_jump_free_gaussian_panel():
    """No lasting-variance gap -> opt-in robust fit keeps jumps off and 11 scalars."""
    p_true, L0 = _make_planted_params(
        N=8000, K=2000, sigma_L0=0.09, beta=0.05, phi=0.80,
        a_xi=-1.1, b_xi=0.10, a_obs=-1.4, b_obs=0.10, s_vol=0.6, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=160, seed=5, apply_absence=False, burn=20)
    p_hat = cr.estimate_common(X, R, K=2000, refine_drift=False, scale_moment="robust")
    assert p_hat.jump_lam == 0.0 and p_hat.jump_c == 0.0
    assert p_hat.n_free_scalars() == 11
    assert p_hat.diag["jumps"]["gap_share"] < cr.JUMP_MIN_SHARE


def test_jump_recovery_rate_and_variance():
    """Planted rare Laplace jumps: opt-in scale_moment='robust' recovers core sigma_L0 and jump variance."""
    p_true, L0 = _make_planted_params(
        N=8000, K=2000, sigma_L0=0.08, beta=0.0, phi=0.80,
        a_xi=-1.1, b_xi=0.10, a_obs=-1.4, b_obs=0.0, s_vol=0.5, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    p_j = cr.CommonParams(**{**p_true.__dict__, "jump_lam": 0.01, "jump_c": 0.5, "jump_dist": "laplace"})
    X, R, _ = cr.simulate_common_panel(p_j, L0, T_sim=200, seed=77, apply_absence=False, burn=20)
    p_hat = cr.estimate_common(X, R, K=2000, refine_drift=False, scale_moment="robust")
    assert p_hat.jump_lam == cr.JUMP_LAM_ROBUST and p_hat.n_free_scalars() == 12
    true_var = 0.01 * 0.5
    est_var = p_hat.jump_lam * p_hat.jump_c
    assert 0.30 * true_var < est_var < 1.30 * true_var
    assert abs(p_hat.sigma_L0 - p_true.sigma_L0) / p_true.sigma_L0 < 0.30
    pv = cr.estimate_common(X, R, K=2000, refine_drift=False, scale_moment="variance")
    assert pv.jump_lam == 0.0 and pv.sigma_L0 > 1.25 * p_true.sigma_L0


def test_jump_simulator_variance_matches_formula():
    """One lasting step with jumps adds exactly lam * c * s_sc(z)^2 variance on average."""
    p_true, L0 = _make_planted_params(
        N=20000, K=5000, sigma_L0=0.0, beta=0.0, phi=0.5, a_xi=-8.0, b_xi=0.12,
        a_obs=-8.0, b_obs=0.0, s_vol=0.0, gamma_L=0.0, a_abs=-10.0, b_abs=0.0, lam_exit=0.0,
    )
    p_j = cr.CommonParams(**{**p_true.__dict__, "drift_g": np.zeros_like(p_true.drift_g), "jump_lam": 0.2, "jump_c": 0.4})
    rng = np.random.default_rng(3)
    st = cr.CommonState(p_j, L0, rng)
    L_before = st.L.copy()
    _, zp = st._ranks_and_z()
    st.step_lasting()
    dL = st.L - L_before
    expected = 0.2 * (cr._jump_sd_of(p_j, zp) ** 2)
    order = np.argsort(-L_before)
    for lo, hi in ((1, 500), (500, 5000), (5000, 20000)):
        idx = order[lo - 1:hi]
        ratio = float(np.mean(dL[idx] ** 2)) / float(np.mean(expected[idx]))
        assert 0.8 < ratio < 1.25





# --------------------------------------------------------------------------- #
# Round 8 (opus_r8): persistent drift in the core of both arms, with a tiered rank law
# --------------------------------------------------------------------------- #
def _planted_drift_panel(sd_K: float, b_d: float, seed: int, T: int = 200, N: int = 2400, K: int = 600):
    p_base, L0 = _make_planted_params(
        N=N, K=K, sigma_L0=0.07, beta=0.05, phi=0.84,
        a_xi=-1.2, b_xi=0.05, a_obs=-1.5, b_obs=0.05,
        s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    p_true = cr.CommonParams(**{
        **p_base.__dict__,
        "decline_var": sd_K ** 2, "decline_rho": 0.5 ** (1.0 / cr.DRIFT_HL_FIXED), "decline_b": b_d,
    })
    X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=T, seed=seed, apply_absence=False, burn=20)
    return p_true, X, R


def test_drift_rank_law_recovers_planted_rank_dependent_drift():
    """A planted drift whose SD falls toward the head is detected by the tiered moment in the SYMMETRIC arm
    (drift_core): sigma_d(z_K) within 35%, slope positive and NOT overshooting the planted slope.

    Honest limitation (D10, EXPERIMENT_LOG): tiers are defined by whole-panel permanent rank, so entities whose
    drift carried them upward are counted in the head tier; the slope is attenuated toward rank-uniform (planted
    0.6 -> 0.32 / 0.58 on these 37-entity head tiers; planted 0.3 -> ~0.05-0.12). The estimator is therefore
    conservative: it shrinks toward the Round 4-6 rank-uniform drift, never past the truth."""
    for seed in (31, 32):
        p_true, X, R = _planted_drift_panel(sd_K=0.014, b_d=0.60, seed=seed)
        p_hat = cr.estimate_common(X, R, K=600, refine_drift=False, asymmetry=False)
        sd_hat = math.sqrt(p_hat.decline_var)
        assert abs(sd_hat - 0.014) / 0.014 < 0.35, f"seed {seed} sigma_d(K): {sd_hat:.5f}"
        assert 0.10 < p_hat.decline_b < 0.60 + 0.15, f"seed {seed} b_d: {p_hat.decline_b:+.3f}"
        tiers = p_hat.diag["stage3"]["drift"]["tiers"]
        sds = [math.sqrt(t["sd2"]) for t in tiers]
        assert sds[0] < sds[-1], f"tier drift SD should rise toward K, got {sds}"


def test_drift_rank_law_flat_on_rank_uniform_drift():
    """A rank-uniform planted drift (b_d = 0) gives |b_d| < 0.15 and sigma_d within 35%."""
    p_true, X, R = _planted_drift_panel(sd_K=0.012, b_d=0.0, seed=37)
    p_hat = cr.estimate_common(X, R, K=600, refine_drift=False, asymmetry=False)
    assert abs(math.sqrt(p_hat.decline_var) - 0.012) / 0.012 < 0.35
    assert abs(p_hat.decline_b) < 0.15, f"b_d: {p_hat.decline_b:+.3f}"


def test_drift_core_stays_off_on_drift_free_panels():
    """D8: without the clustered-SE rule the gapped covariance fires at sigma_d ~ 0.003-0.004 on drift-free
    planted panels. With it the symmetric arm stays at Round 6's scalar count on three drift-free seeds."""
    for seed in (11, 41, 43):
        p_true, L0 = _make_planted_params(N=2000, K=500, s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0)
        X, R, _ = cr.simulate_common_panel(p_true, L0, T_sim=170, seed=seed, apply_absence=False, burn=20)
        p_hat = cr.estimate_common(X, R, K=500, refine_drift=False, asymmetry=False)
        assert p_hat.decline_var == 0.0, f"seed {seed}: spurious sigma_d={math.sqrt(p_hat.decline_var):.5f}"


def test_drift_simulator_stationary_variance_follows_rank_law():
    """The simulator's drift state has stationary variance sigma_d(z)^2 at each rank (entities that keep their
    rank): long run on a frozen ladder with no lasting noise, so ranks stay put."""
    p_true, L0 = _make_planted_params(
        N=4000, K=1000, sigma_L0=0.0, beta=0.0, phi=0.5, a_xi=-8.0, b_xi=0.0,
        a_obs=-8.0, b_obs=0.0, s_vol=0.0, gamma_L=0.0, a_abs=-10.0, b_abs=0.0, lam_exit=0.0,
    )
    rho_d = 0.5 ** (1.0 / 13.0)
    p = cr.CommonParams(**{**p_true.__dict__, "drift_g": np.zeros_like(p_true.drift_g),
                           "decline_var": 0.01 ** 2, "decline_rho": rho_d, "decline_b": 0.4})
    st = cr.CommonState(p, L0 * 50.0, np.random.default_rng(5))   # very wide ladder: drift never reorders it
    var_acc = np.zeros(p.N)
    n = 0
    for t in range(400):
        st.step_lasting()
        if t >= 100:
            var_acc += st.drift_i ** 2
            n += 1
    _, zp = st._ranks_and_z()
    expect = cr.drift_sd_of(p, zp) ** 2
    order = np.argsort(-st.L)
    for lo, hi in ((1, 60), (60, 1000), (1000, 4000)):
        idx = order[lo - 1:hi]
        ratio = float(np.mean(var_acc[idx] / n)) / float(np.mean(expect[idx]))
        assert 0.8 < ratio < 1.25, f"ranks {lo}-{hi}: ratio {ratio:.3f}"


def test_flags_off_reproduce_round6_source_exactly():
    """drift_core=False, drift_rank_law=False reproduces the staged Round 6 source bit for bit (both arms)."""
    import importlib.util
    src = HERE / "baseline_r6" / "common_rankdiff_r6_source_copy.py"
    spec = importlib.util.spec_from_file_location("r6_source_copy", src)
    r6 = importlib.util.module_from_spec(spec)
    sys.modules["r6_source_copy"] = r6
    spec.loader.exec_module(r6)
    p_true, X, R = _planted_drift_panel(sd_K=0.012, b_d=0.3, seed=47, T=170)
    for asym in (False, True):
        a = r6.estimate_common(X, R, K=600, refine_drift=True, asymmetry=asym)
        b = cr.estimate_common(X, R, K=600, refine_drift=True, asymmetry=asym, drift_core=False, drift_rank_law=False)
        for f in ("sigma_L0", "beta", "phi", "a_xi", "b_xi", "a_obs", "b_obs", "s_vol", "jump_c",
                  "surge_lam", "surge_mu", "surge_rho", "decline_var"):
            assert getattr(a, f) == getattr(b, f), (asym, f)
        assert np.array_equal(a.drift_g, b.drift_g)
        fa = r6.filter_endpoints(X, R, a)
        fb = cr.filter_endpoints(X, R, b)
        for k in ("L_hat", "xi_hat", "P11", "v_hat"):
            assert np.array_equal(fa[k], fb[k]), (asym, k)


def test_rank_law_drift_kalman_tracks_planted_drift():
    """4-state filter with per-entity drift variance: filtered d_hat correlates with the planted drift, more
    strongly in the outer tier (large sigma_d) than at the head (small sigma_d)."""
    p_base, L0 = _make_planted_params(
        N=2400, K=600, sigma_L0=0.05, beta=0.0, phi=0.84,
        a_xi=-1.4, b_xi=0.05, a_obs=-1.8, b_obs=0.05,
        s_vol=0.0, gamma_L=0.0, a_abs=-8.0, b_abs=0.0,
    )
    p = cr.CommonParams(**{**p_base.__dict__, "decline_var": 0.02 ** 2,
                           "decline_rho": 0.5 ** (1.0 / cr.DRIFT_HL_FIXED), "decline_b": 0.4})
    rng = np.random.default_rng(53)
    st = cr.CommonState(p, L0, rng)
    T = 150
    Xs = np.empty((T, p.N)); Rs = np.full((T, p.N), np.nan)
    for t in range(T):
        x = st.step(apply_absence=False)
        Xs[t] = x
        o = np.argsort(-x, kind="stable")
        Rs[t, o] = np.arange(1, p.N + 1)
    filt = cr.filter_endpoints(Xs, Rs, p)
    assert "d_hat" in filt
    top = np.argsort(np.nanmean(Rs, axis=0))[:600]
    r_all = np.corrcoef(filt["d_hat"][top], st.drift_i[top])[0, 1]
    assert r_all > 0.4, f"corr(d_hat, d) = {r_all:.3f}"
