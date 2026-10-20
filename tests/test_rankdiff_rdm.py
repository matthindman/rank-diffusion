"""Known-answer tests for the moving-home (rank-drift) model, llm_fitting/rankdiff_rdm.py (MODEL_STATUS 2z-dd)."""
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
import rankdiff_rdm as rdm  # noqa: E402


def test_structure_function_recovers_a_planted_lasting_variance():
    """X = random walk (variance s per week) + iid noise (variance e): D(h) = 2e + s h."""
    rng = np.random.default_rng(3)
    T, M, s, e = 120, 3000, 0.01, 0.04
    L = np.cumsum(rng.normal(0, np.sqrt(s), (T, M)), axis=0)
    X = L + rng.normal(0, np.sqrt(e), (T, M))
    D, C = rdm.structure_function(X, np.zeros(M, int), 1, 40)
    hs = np.arange(1, 41)
    slope, icept = np.polyfit(hs, D[0], 1)
    assert abs(slope / s - 1) < 0.1, slope
    assert abs(icept / (2 * e) - 1) < 0.1, icept


def test_velocity_shape_has_the_right_limits():
    h = np.arange(1, 30, dtype=float)
    near_one = rdm._vel_shape(h, 0.9999)
    assert np.allclose(near_one / h ** 2, 1.0, rtol=0.01)          # persistent velocity: variance grows as h^2
    zero = rdm._vel_shape(h, 1e-9)
    assert np.allclose(zero, h, rtol=1e-6)                          # uncorrelated velocity: a random walk


def test_mixture_without_jumps_or_amplitude_is_gaussian():
    exc, q3 = rdm._mixture_moments(0.5, 1.0, 0.0, 0.0)
    assert abs(exc) < 1e-9
    assert abs(q3 - 0.0026998) < 1e-4
    exc_amp, _ = rdm._mixture_moments(0.0, 1.0, 0.0, 0.8)          # all swing, amplitude alone: 3 (e^{s^2} - 1)
    assert abs(exc_amp - 3 * (np.exp(0.64) - 1)) < 0.05
    exc_l, _ = rdm._mixture_moments(1.0, 1.0, 0.0, 0.8)            # all lasting, amplitude on the swings only
    assert abs(exc_l) < 1e-9


def test_drift_from_a_constant_slide_is_uniform_and_not_centred():
    """A constant net slide s at every cut-off gives the same drift -s to every stratum (the Atlas/Gabaix case:
    the floor, not a centring, supplies the restoring force)."""
    k = np.geomspace(1, 1000, 40)
    z, g = rdm.drift_from_slides(k, np.full(k.size, 0.004), 1000)
    assert np.allclose(g, -0.004, atol=1e-12)


def _toy_params(drift):
    from types import SimpleNamespace
    zk = np.array([-8.0, -4.0, 0.0])
    base = SimpleNamespace(z_knots=zk, kappa_z=np.full(3, 0.2), kappa=0.2, sigma_perm=np.full(3, 0.1),
                           phi=np.full(3, 0.5), sigma_trans=np.full(3, 0.05), sigma_obs=np.full(3, 0.05),
                           exit_rate=np.zeros(3), bottom_mu=np.array([0.0]), temper_s=0.0, mix_b=0.0, t_df=float("inf"))
    return SimpleNamespace(base=base, drift_z=zk, drift_g=np.full(3, drift), band_level=np.array([0.0, 10.0]),
                           sigma0=0.05, beta=0.0, L_ref=5.0, pi=0.0, lam=0.0, vel_var=0.0, vel_rho=0.0,
                           amp_lasting=False, kappa_m=0.1, V_m=np.full(3, 0.01), band_z=zk)


def test_ablation_arms_reduce_to_their_parents_without_drift():
    """2z-dg ablations: with zero drift, 'homedrift' is exactly A's 'home' and 'movinghome_nodrift' is exactly
    'movinghome' (same seed, same draws)."""
    L0 = np.linspace(10.0, 1.0, 50)
    p0 = _toy_params(0.0)
    for arm, parent in (("homedrift", "home"), ("movinghome_nodrift", "movinghome")):
        a = rdm.simulate_panel(p0, L0, 20, seed=3, lasting=arm)[0]
        b = rdm.simulate_panel(p0, L0, 20, seed=3, lasting=parent)[0]
        assert np.array_equal(a, b)


def test_homedrift_moves_the_home_by_rank():
    """With a constant negative drift and no noise in the home, 'homedrift' lowers every home by the drift each week."""
    L0 = np.linspace(10.0, 1.0, 50)
    p = _toy_params(-0.01)
    st = rdm._State(p, L0, np.random.default_rng(0), lasting="homedrift")
    for _ in range(10):
        st.step()
    assert np.allclose(st.home, L0 - 0.1)


def test_variance_preserving_home_keeps_model_A_one_step_variance():
    """2z-dg A5b: with a random-walking home, 'movinghome_vp' keeps model A's one-step variance of the level
    (2 sigma_p^2 / (2 - kappa)) while the frozen 'movinghome' is short by about sigma_L^2."""
    from types import SimpleNamespace
    N, kap, sp, sL = 20000, 0.2, 0.1, 0.08
    p = _toy_params(0.0)
    p.sigma0 = sL
    p.base.kappa_z = np.full(3, kap)
    p.base.sigma_perm = np.full(3, sp)
    p.base.sigma_trans = np.zeros(3)
    p.base.sigma_obs = np.zeros(3)
    p.base.phi = np.zeros(3)
    L0 = np.linspace(9.0, 1.0, N)
    out = {}
    for arm in ("home", "movinghome", "movinghome_vp"):
        st = rdm._State(p, L0, np.random.default_rng(1), lasting=arm)
        for _ in range(150):
            st.step()
        L1 = st.L.copy()
        st.step()
        out[arm] = np.var(st.L - L1)
    target = 2 * sp ** 2 / (2 - kap)
    assert abs(out["home"] / target - 1) < 0.05
    assert abs(out["movinghome_vp"] / target - 1) < 0.05
    assert out["movinghome"] < target - 0.8 * sL ** 2


def test_corrected_nodrift_arm_reduces_to_corrected_without_drift():
    """2z-dg: with zero drift, 'movinghome_vp_nodrift' is exactly 'movinghome_vp' (same seed, same draws)."""
    L0 = np.linspace(10.0, 1.0, 50)
    p0 = _toy_params(0.0)
    a = rdm.simulate_panel(p0, L0, 20, seed=5, lasting="movinghome_vp_nodrift")[0]
    b = rdm.simulate_panel(p0, L0, 20, seed=5, lasting="movinghome_vp")[0]
    assert np.array_equal(a, b)


def test_corrected_moving_home_applies_drift_by_rank():
    """2z-dg bug guard: 'movinghome_vp' must apply the drift by rank to the home (fc28c25 ran it without drift);
    'movinghome_vp_nodrift' must not.  With sigma0 = 0 the home moves by the drift alone."""
    L0 = np.linspace(10.0, 1.0, 50)
    p = _toy_params(-0.01)
    p.sigma0 = 0.0
    for arm, expect in (("movinghome_vp", -0.1), ("movinghome", -0.1), ("movinghome_vp_nodrift", 0.0), ("movinghome_nodrift", 0.0)):
        st = rdm._State(p, L0, np.random.default_rng(0), lasting=arm)
        for _ in range(10):
            st.step()
        assert np.allclose(st.home - L0, expect), arm


def test_factor_exposure_off_is_bit_identical():
    """R12 (c) is opt-in: with no factor path (attribute absent or None) the simulation is unchanged."""
    p0 = _toy_params(0.0)
    p1 = _toy_params(0.0)
    p1.fac_path, p1.fac_load, p1.fac_pool = None, None, None
    L0 = np.linspace(10.0, 0.0, 50)
    X0, _ = rdm.simulate_panel(p0, L0, 30, seed=7, lasting="movinghome_vp")
    X1, _ = rdm.simulate_panel(p1, L0, 30, seed=7, lasting="movinghome_vp")
    assert np.array_equal(X0, X1)


def test_factor_exposure_moves_homes_by_loading():
    """R12 (c): with a constant factor f and loading deviations +1 / -1, the two homes part by 2 f per week."""
    p = _toy_params(0.0)
    p.sigma0 = 1e-12                                   # no lasting noise: the factor is the only home movement
    p.fac_path = np.full(40, 0.01)
    load = np.zeros(20)
    load[3], load[4] = 1.0, -1.0
    p.fac_load, p.fac_pool = load, np.zeros(5)
    st = rdm._State(p, np.linspace(10.0, 0.0, 20), np.random.default_rng(1), lasting="movinghome_vp")
    h0 = st.home.copy()
    for _ in range(25):
        st.step()
    moved = st.home - h0
    assert abs(moved[3] - 0.25) < 1e-6 and abs(moved[4] + 0.25) < 1e-6, moved[[3, 4]]
    assert np.allclose(np.delete(moved, [3, 4]), 0.0, atol=1e-6)


def test_velocity_clip_bounds_fixed_heterogeneity():
    """R12-A1: with vel_rho = 1 and vel_clip = 2.5, every velocity stays within 2.5 sd; vel_clip = 0 leaves draws unchanged."""
    p = _toy_params(0.0)
    p.vel_var, p.vel_rho, p.vel_clip = 0.01 ** 2, 1.0, 2.5
    st = rdm._State(p, np.linspace(10.0, 0.0, 4000), np.random.default_rng(5), lasting="movinghome_vp")
    assert np.max(np.abs(st.vel)) <= 2.5 * 0.01 + 1e-12
    p0 = _toy_params(0.0)
    p0.vel_var, p0.vel_rho = 0.01 ** 2, 1.0
    a = rdm._State(p0, np.linspace(10.0, 0.0, 50), np.random.default_rng(5), lasting="movinghome_vp").vel
    p0.vel_clip = 0.0
    b = rdm._State(p0, np.linspace(10.0, 0.0, 50), np.random.default_rng(5), lasting="movinghome_vp").vel
    assert np.array_equal(a, b)


def _r16_toy(vel=False):
    p = _toy_params(-0.001)
    p.base.exit_rate = np.full(3, 0.01)
    p.base.bottom_mu = np.array([0.0, 0.5])
    if vel:
        p.vel_var, p.vel_rho, p.vel_clip = 0.004 ** 2, 1.0, 2.5
    return p


def test_r16_defaults_are_bit_identical():
    """R16 terms are opt-in: with their fields absent or at their defaults, the panels match the pre-R16 code
    (fingerprints taken before the change, 2026-10-21)."""
    import hashlib
    ref = {("movinghome_vp", False): "baf7bb58eeec036950452f3bda7a66b92b84f6ec77ae8ee983e9332f2263765c",
           ("movinghome_vp", True): "daabf2ccebcffc8c018c78c6f940cb269c0f22543ba10007da0c29ba94b47bf3",
           ("rdm", False): "40431c8e8e2fe4af317edb39717c675a66b29ee27a32677e924c97c4ef20732c",
           ("rdm", True): "c4bd79c6021df8f7c013e5d37f73d98a3f877daecfcab7963d02be4857c3ba13"}
    for (lasting, vel), h in ref.items():
        for explicit in (False, True):
            p = _r16_toy(vel)
            if explicit:
                p.vel_head_k, p.vel_head_mult, p.ou_kappa_mult = 0, 1.0, 1.0
                p.dec_delta, p.dec_enter, p.dec_exit = 0.0, 0.0, 0.0
            X, ids = rdm.simulate_panel(p, np.linspace(10.0, 0.0, 300), 60, seed=11, lasting=lasting)
            assert hashlib.sha256(X.tobytes() + ids.tobytes()).hexdigest() == h, (lasting, vel, explicit)
    p = _r16_toy()
    p.vel_var, p.vel_rho = 0.004 ** 2, 0.98
    st = rdm._State(p, np.linspace(10.0, 0.0, 300), np.random.default_rng(4))
    for _ in range(40):
        st.step_lasting()
    assert hashlib.sha256(st.L.tobytes() + st.ids.tobytes()).hexdigest() == "be7bda475c902e526c96e7c4cc09aaa41166177287495ad6ca447fb3e325444c"


def test_decline_state_has_its_stationary_share_and_mean_zero():
    """R16-A: the state's share stays at pi_D = a / (a + b), and its increment -delta (Z - pi_D) averages zero."""
    p = _r16_toy()
    p.dec_delta, p.dec_enter, p.dec_exit = 0.01, 0.004, 0.016          # pi_D = 0.2
    st = rdm._State(p, np.linspace(10.0, 0.0, 20000), np.random.default_rng(2), lasting="movinghome_vp")
    shares, incs = [], []
    for _ in range(200):
        shares.append(st.dec.mean())
        incs.append(float(np.mean(-p.dec_delta * (st.dec - st.dec_pi))))
        st.step()
    assert abs(np.mean(shares) - 0.2) < 0.01
    assert abs(np.mean(incs)) < 2e-4


def test_decline_state_moves_a_member_by_delta_while_in_it():
    """R16-A, exact: with no lasting noise and no switching out, a slot in the state drifts by -delta (1 - pi_D) a week."""
    p = _r16_toy()
    p.base.exit_rate = np.zeros(3)
    p.drift_g = np.zeros(3)
    p.sigma0 = 1e-12
    p.dec_delta, p.dec_enter, p.dec_exit = 0.01, 1e-12, 1e-12          # pi_D = 0.5; no switches in practice
    st = rdm._State(p, np.linspace(10.0, 0.0, 200), np.random.default_rng(3), lasting="movinghome_vp")
    z0, h0 = st.dec.copy(), st.home.copy()
    for _ in range(25):
        st.step()
    moved = st.home - h0
    assert np.allclose(moved[z0], -0.25 * 0.5, atol=1e-6) and np.allclose(moved[~z0], 0.25 * 0.5, atol=1e-6)


def test_head_spread_scales_the_head_velocities_only():
    """R16-B: slots that start in the top vel_head_k get velocities times vel_head_mult; the rest are unchanged."""
    L0 = np.linspace(10.0, 0.0, 400)
    p0 = _r16_toy(vel=True)
    a = rdm._State(p0, L0, np.random.default_rng(5), lasting="movinghome_vp").vel
    p1 = _r16_toy(vel=True)
    p1.vel_head_k, p1.vel_head_mult = 40, 2.0
    b = rdm._State(p1, L0, np.random.default_rng(5), lasting="movinghome_vp").vel
    assert np.allclose(b[:40], 2.0 * a[:40]) and np.array_equal(b[40:], a[40:])


def test_ou_kappa_multiplier_keeps_the_swing_variance():
    """R16-B: a faster OU reversion with the same stationary variance leaves var(L - home) about unchanged."""
    def dev_var(km):
        p = _r16_toy()
        p.base.exit_rate = np.zeros(3)
        p.sigma0 = 1e-12
        p.ou_kappa_mult = km
        st = rdm._State(p, np.linspace(10.0, 0.0, 4000), np.random.default_rng(8), lasting="movinghome")
        out = []
        for t in range(300):
            st.step()
            if t >= 100:
                out.append(np.var(st.L - st.home))
        return np.mean(out)
    v1, v2 = dev_var(1.0), dev_var(2.0)
    assert abs(v2 / v1 - 1) < 0.1, (v1, v2)
