"""Known-answer tests for M2 (entrants and shape).  Synthetic panels only."""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_step1b as m2  # noqa: E402
from test_balance_accounting import lasting_plus_noise, weekly_ranks  # noqa: E402


def planted_histories():
    """160 weeks, four 40-week blocks, 60 steady endpoints and seven with known histories."""
    T, n = 160, 67
    X = np.tile(np.linspace(10, 4, n)[None, :], (T, 1))
    X[:, 60:] = 12.0                                   # the seven special ones sit at the top when present
    X[:50, 60] = np.nan                                # A: first seen in week 50 (inside block 1)
    X[11:80, 61] = np.nan                              # B: seen in block 0, silent through block 1
    X[40:70, 62] = np.nan                              # C: present 10 of block 1's 40 weeks
    X[40:50, 63] = np.nan                              # D: present 30 of 40: misses the 90% rule
    X[80:, 64] = np.nan                                # E: leaves the record after block 1
    X[80:100, 65] = np.nan                             # F: present half of block 2
    X[80:120, 66] = np.nan                             # G: silent in block 2, back in block 3
    return X


def test_entrants_and_exits_are_classified_from_their_histories():
    X = planted_histories()
    block, start, nb = 40, 0, 4
    M = ba.block_means(X, block, min_frac=0.9)
    first, last, pres = m2.history(X, block, start, nb)
    e = m2.entrant_origins(M, first, pres, 1, 20, block, start)
    assert e["n_entrants"] == 4
    assert [e["classes"][c]["n"] for c in m2.ENTRANT_CLASSES] == [1, 1, 1, 1]
    assert e["classes"]["new"]["median_weeks_since_first_observed"] == 30.0
    x = m2.exit_fates(M, last, pres, 1, 20, block, start, nb)
    assert (x["n_exits"], x["gone"], x["partly_present_next_block"], x["silent_then_back"]) == (3, 1, 1, 1)
    assert x["right_censored"] is False


def test_top_k_origins_partition_the_top_k():
    X = lasting_plus_noise("rw", n=2000, T=160, seed=2)
    X[:60, -100:] = np.nan                             # 100 endpoints first seen after the first block
    X[:, -100:] += 6.0                                 # and large enough to reach the top
    R = weekly_ranks(X)
    block, start, nb, K = 40, 0, 4, 300
    M = ba.block_means(X, block, min_frac=0.9)
    first, last, pres = m2.history(X, block, start, nb)
    pr0, pres0 = ba.perm_rank(R, 0, block)
    o = m2.topk_origins(M, pr0, pres0, first, 3, K, block)
    assert sum(v["n"] for v in o.values()) == K
    assert o["not observed through the first block"]["n"] > 50
    assert o["in the first block's cohort"]["share"] > 0.3


def test_band_trend_recovers_a_planted_compression():
    """Ladder level at rank r is -s(t) log r with s falling linearly: every band's
    level against the centre is s(t) times a constant, so its trend is exact."""
    K, T, s0, gam = 64, 104, 1.0, 0.002
    r = np.arange(1, 4 * K + 1)
    X = -(s0 * (1 - gam * np.arange(T)))[:, None] * np.log(r)[None, :]
    w = m2.weekly_ladder_series(X, K)
    edges = w["edges"]
    assert edges[:3] == [0.0, K / 128, K / 64] and edges[-2:] == [2.0 * K, 4.0 * K]
    centre_const = -np.log(r[:K]).mean()
    for j in range(len(edges) - 1):
        lo, hi = int(edges[j]), int(edges[j + 1])
        if hi == lo:
            continue
        const = -np.log(r[lo:hi]).mean() - centre_const
        tr = m2.trend(w["bands"][:, j] - w["centre"], n_boot=50)
        assert np.isclose(tr["per_year"], -52 * s0 * gam * const, rtol=1e-9, atol=1e-12)
        assert abs(tr["q95"] - tr["q05"]) < 1e-9


def test_trend_interval_for_noise_and_for_a_planted_slope():
    """Autocorrelated noise with no trend: the 5-95% interval should contain zero
    in most of 60 series (nominal 90%; a block bootstrap runs a little short).
    A planted slope is recovered and its interval excludes zero."""
    rng = np.random.default_rng(3)
    def ar():
        e = np.zeros(208)
        for t in range(1, 208):
            e[t] = 0.8 * e[t - 1] + 0.05 * rng.standard_normal()
        return e
    hits = wide = 0
    for i in range(80):
        tr = m2.trend(ar(), n_boot=100, seed=i)
        hits += tr["q05"] < 0 < tr["q95"]
        wide += tr["wide05"] < 0 < tr["wide95"]
    assert 0.6 <= hits / 80 < 0.9                   # the nominal interval runs short, and the code says so
    assert wide / 80 >= 0.9                         # the doubled interval is the one used for scoring
    e = ar()
    up = m2.trend(e + 0.004 * np.arange(208))
    assert up["q05"] > 0 and abs(up["per_year"] - 0.208) < 0.06
    assert m2.trend(e, drop=np.arange(208) % 9 == 0)["n"] < 208


def test_split_trend_and_half_change_on_a_planted_break():
    t = np.arange(200, dtype=float)
    y = np.where(t < 100, 0.002 * t, 0.2 - 0.004 * (t - 100))
    sp = m2.split_trend(y, 100, n_boot=50)
    assert np.isclose(sp["before"]["per_year"], 0.104) and np.isclose(sp["after"]["per_year"], -0.208)
    assert np.isclose(sp["change"], -0.312) and sp["change_wide95"] < 0
    slow = np.where(t < 100, 0.0, np.clip((t - 100) / 40, 0, 1))
    fast = np.where(t < 100, 0.0, np.clip((t - 100) / 10, 0, 1))
    hs, hf = m2.half_change_weeks(slow, 100), m2.half_change_weeks(fast, 100)
    assert 19 <= hs <= 22 and hf < hs
    assert m2.half_change_weeks(np.zeros(200), 100) is None


def test_rank_shares_reproduce_a_planted_ladder():
    """Raw metric at rank r is 1000 / r every week, so the share at rank r is
    (1 / r) / H_n whatever the week; the series is flat and equals that."""
    K, T = 64, 60
    n = 4 * K
    raw = 1000.0 / np.arange(1, n + 1)
    X = np.tile(np.log1p(raw)[None, :], (T, 1))
    w = m2.weekly_ladder_series(X, K)
    logtot = np.log(raw.sum())
    share = np.log(np.expm1(w["at_rank"])) - logtot
    for i, r in enumerate(w["ranks"]):
        assert np.allclose(share[:, i], np.log(raw[r - 1] / raw.sum()))
    assert np.allclose(np.log(w["topn"][:, 1]) - logtot, np.log(raw[:5].sum() / raw.sum()))


def test_runner_end_to_end_and_scoring_contract():
    X = lasting_plus_noise("rw", n=3000, T=160, seed=7)
    X[:70, -80:] = np.nan
    X[:, -80:] += 3.0
    R = weekly_ranks(X)
    per, ent = np.nonzero(np.isfinite(X))
    df = pd.DataFrame(dict(entity_id=[f"e{j:05d}" for j in ent], period=per, X=X[per, ent], rank=R[per, ent]))
    df["N"] = df.groupby("period")["entity_id"].transform("size")
    df["metric"] = np.expm1(df["X"].clip(lower=0))
    df["ts"] = pd.Timestamp("2020-01-06") + pd.to_timedelta(df["period"] * 7, unit="D")
    res = m2.analyse(df, K=300, block=40, boot=40, flagged_weeks=[5, 6], break_date="2021-07-05")
    json.dumps(res, default=float)
    assert res["blocks"]["n"] == 4 and len(res["blocks"]["first_week_of_each"]) == 4
    assert res["shape"]["n_flagged_weeks"] == 2
    assert res["break"]["week"] == 78 and res["break"]["block_containing"] == 1
    assert [r["rank"] for r in res["break"]["rank_shares"]] == [1, 3, 5, 10, 25, 50, 100, 250, 500, 1000]
    assert len(res["break"]["bands_vs_centre"]) == len(res["shape"]["bands"])
    assert all(abs(r["shape"] - (-r["g"] + r["r"] + r["f"])) < 1e-9 for a in res["accounting"] for r in a["rows"])
    assert any(e.get("classes", {}).get("new", {}).get("n", 0) > 0 for e in res["entrants"])
    for system, panel in (("reddit", "reddit_submissions_long"), ("reddit", "reddit_comments_ext"), ("facebook", "facebook_a")):
        res["panel"] = panel
        sc = m2.score(res, system)
        assert set(sc) == {"E1", "E2", "E3", "E4", "E5", "E6", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"}
        assert all(v["status"] in ("PASS", "FAIL", m2.NA) for v in sc.values())
        json.dumps(sc, default=float)
    nobreak = m2.analyse(df, K=300, block=40, boot=20)
    assert nobreak["break"]["week"] is None and "bands_vs_centre" not in nobreak["break"]
