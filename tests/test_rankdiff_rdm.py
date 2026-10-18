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
