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
