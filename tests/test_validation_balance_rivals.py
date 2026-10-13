"""Known-answer and leakage checks for the V4 synthetic recovery harness."""
import copy
import json
from pathlib import Path
import sys

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"llm_fitting"))
import balance_accounting as ba
import mts_step1 as m1
from validation import planted_sim
from validation import balance_rivals_frozen as frozen
from validation import balance_rivals_sim as sim
from validation import balance_rivals_study as study
from validation import balance_rivals_report as report
from validation import balance_rivals_inputs as inputs


def saved_parameters(K=256, T=24, block=6):
    n = 4*K
    p = dict(N=n, w0=(18-.6*np.log(np.arange(1, n+1))).tolist(),
             z_knots=np.log(np.array([.5, n-.5])/n).tolist(), kappa=0.,
             kappa_z=[.02, .02], sigma_perm=[.06, .06], phi=[.45, .45],
             sigma_trans=[.10, .10], phi2=[.85, .85], sigma_trans2=[.03, .03],
             sigma_obs=[.03, .03], exit_rate=[0., .0002], lam=[1., 1.],
             temper_s=.3, mix_b=1., t_df=5., factor_rho=.8, sigma_F=.02,
             bottom_mu=[13., 13.2], T_curve=[18., 13.])
    saved = dict(panel="facebook_a", K=K, T=T, parameters=p, input_sha256="synthetic-fixture",
                 git="synthetic-fixture", universe=dict(rows=.98*n*T, identities=n))
    config = dict(K=K, T=T, block=block, system="reddit", source="fixture")
    return saved, config


def test_frozen_layers_do_not_change_any_original_output():
    saved, config = saved_parameters()
    p = sim.calibrate(saved, config)["p"]
    for seed in (1000, 1001):
        original = planted_sim.simulate(p, 12, seed, return_full=True, track=100)
        layered = frozen.simulate_with_layers(p, 12, seed, return_full=True, track=100)
        for key in original:
            np.testing.assert_array_equal(original[key], layered[key])
        assert layered["full_latent"].shape == original["full_values"].shape
        assert (layered["full_amp"] > 0).all()


@pytest.mark.parametrize("rival", sim.RIVALS)
def test_future_generation_preserves_prefix_and_identity_schema(rival):
    saved, config = saved_parameters(K=64)
    cal = sim.calibrate(saved, config)
    process = sim.Process(cal, rival, 1000)
    before, future = process.generate(9), process.generate(5)
    whole = sim.Process(cal, rival, 1000).generate(14)
    for key in whole:
        np.testing.assert_array_equal(np.concatenate([before[key], future[key]]), whole[key])
    df = sim.long_frame(before)
    assert not df.duplicated(["period", "entity_id"]).any()
    for _, group in df.groupby("period"):
        np.testing.assert_array_equal(group["rank"], np.arange(1, len(group)+1))
        assert group["N"].eq(len(group)).all()
        assert np.all(np.diff(group["X"]) <= 0)
    assert before["values"].shape[1] == config["K"]*(16 if rival == "replacement" else 8)


def test_plain_ou_matches_declared_lag_variance_without_a_personal_home():
    saved, config = saved_parameters()
    c = sim.calibrate(saved, config)
    p = sim.Process(c, "position_ou", 1000)
    assert 2*p.ou_var*(1-p.ou_rho**config["block"]) == pytest.approx(c["a"]*config["block"])
    assert np.isscalar(p.ou_centre)
    assert p.ou_rho < 1


def test_replacement_uses_new_names_and_alignment_preserves_deaths():
    saved, config = saved_parameters(K=64)
    c = sim.calibrate(saved, config)
    c["a"], c["S0"] = .1, .2
    path = sim.Process(c, "replacement", 1000).generate(8)
    assert path["ids"].max() > 16*config["K"]
    ids = np.unique(path["ids"])
    X, L = sim.aligned_layers(path, ids)
    df = sim.long_frame(path)
    expected = df.pivot(index="period", columns="entity_id", values="X").reindex(columns=ids).to_numpy()
    np.testing.assert_array_equal(X, expected)
    assert np.array_equal(np.isnan(X), np.isnan(L))


def test_movement_prediction_cannot_read_slope_or_reordering():
    pair = dict(role="primary", select=0, weeks=52,
                bins=[(16., 32.)], drift=[dict(n=10, mean=-.52)],
                diffusion=[dict(k=k, v=.52, S="FORBIDDEN", R_diff="FORBIDDEN") for k in (16, 32, 64, 128, 256)])
    result = dict(K=256, boundaries={"1.0": dict(ks=[16, 32, 64, 128, 256], blocks=dict(pairs=[pair]))})
    forecast = study.slope_predictions(result)
    assert all(v["predicted"] == pytest.approx(.5) for v in forecast.values())
    pair["drift"][0]["mean"] = 0.
    assert all(v["status"] == "FAILED" for v in study.slope_predictions(result).values())


def test_paired_seed_is_held_out_for_every_rival_in_classifier():
    rows = [dict(status="RAN-OK", provenance=dict(panel="fixture", rival=rv, seed=seed),
                 features=dict(x=float(i)+seed*.001))
            for i, rv in enumerate(sim.RIVALS) for seed in (1000, 1001, 1002)]
    a = report.classify(rows)[0]
    changed = copy.deepcopy(rows)
    for row in changed:
        if row["provenance"]["seed"] == 1000 and row["provenance"]["rival"] != sim.RIVALS[0]:
            row["features"]["x"] = -1e10
    b = report.classify(changed)[0]
    assert a["prediction"] == b["prediction"]


def test_coverage_targets_population_mean_not_sample_mean():
    bank = [dict(absolute_bin_moments={"0": dict(n=100, total=-100.)}) for _ in range(20)]
    prediction = dict(bin_counts=[10, 0], G_pred=999., G_pred_q05=8., G_pred_q95=12.)
    result = report.coverage_target(bank, prediction, 0., 10.)
    assert result["target"] == 1 and result["target_mcse"] == 0
    assert result["covered"] and result["precise"]
    prediction["G_pred_q05"], prediction["G_pred_q95"] = 20., 30.
    assert not report.coverage_target(bank, prediction, 0., 10.)["covered"]


def test_full_small_fixture_uses_unmodified_m1_and_oracle_closes(tmp_path):
    saved, config = saved_parameters()
    study.write(tmp_path/"inputs/facebook_a.json", saved)
    row = study.run_cell("facebook_a", "fixed_walk", 1000, "evaluation", tmp_path, config)
    assert row["status"] == "RAN-OK", row.get("traceback")
    assert row["scores"]["A1"]["status"] == "PASS"
    assert len(row["heldout"]) == 5
    assert max(abs(r["resid"]) for r in row["oracle"]["contributions"]) < 1e-8
    frozen_forecast = study.read(tmp_path/"predictions/facebook_a/fixed_walk/1000.json")
    assert all("truth" not in p for p in frozen_forecast["predictions"].values())
    assert study.run_cell("facebook_a", "fixed_walk", 1000, "evaluation", tmp_path, config) == row
    saved["parameters"]["sigma_F"] = .3
    study.write(tmp_path/"inputs/facebook_a.json", saved)
    with pytest.raises(RuntimeError, match="Checkpoint identity changed"):
        study.run_cell("facebook_a", "fixed_walk", 1000, "evaluation", tmp_path, config)


def test_incomplete_grid_refuses_a_completed_report(tmp_path):
    report.summarize(tmp_path, allow_partial=True)
    assert study.read(tmp_path/"progress.json")["pending_inputs"]
    for panel in sim.PANELS:
        study.write(tmp_path/"inputs"/(panel+".json"), {})
        study.write(tmp_path/"inputs"/(panel+"_absence.json"), {})
    with pytest.raises(RuntimeError, match="Grid incomplete"):
        report.summarize(tmp_path)
    report.summarize(tmp_path, allow_partial=True)
    assert study.read(tmp_path/"progress.json")["status"] == "IN PROGRESS"
    assert not (tmp_path/"RECOVERY_MAP.md").exists()


def test_wilson_precision_target_and_all_success_bound():
    result = report.wilson(10, 20)
    assert (result["hi"]-result["lo"])/2 < .21
    assert report.wilson(20, 20)["lo"] > .8
    assert report.wilson(0, 20)["hi"] < .2


def test_complete_report_path_and_wrong_size_fail_closed(tmp_path):
    """Manufactured grid tests reporting mechanics, not scientific recovery."""
    saved, config = saved_parameters()
    smoke = tmp_path/"smoke"
    study.write(smoke/"inputs/facebook_a.json", saved)
    template = study.run_cell("facebook_a", "fixed_walk", 1000, "evaluation", smoke, config)
    assert template["status"] == "RAN-OK"
    out = tmp_path/"manufactured_grid"
    for panel, cfg in sim.PANELS.items():
        study.write(out/"inputs"/(panel+".json"), saved)
        study.write(out/"inputs"/(panel+"_absence.json"), {})
        inp = study.digest(out/"inputs"/(panel+".json"))
        absence_hash = study.digest(out/"inputs"/(panel+"_absence.json"))
        for rv in sim.RIVALS:
            for stage, seeds in (("evaluation", range(1000, 1020)), ("calibration", range(2000, 2020))):
                for seed in seeds:
                    row = copy.deepcopy(template)
                    row["provenance"].update(panel=panel, rival=rv, seed=seed, stage=stage,
                                               config=cfg, input_sha256=inp)
                    row["provenance"]["absence_sha256"] = absence_hash
                    row["analysis"].update({k: cfg[k] for k in ("K", "T", "block")})
                    row["simulation_population"] = cfg["K"]*(16 if rv == "replacement" else 8)
                    row["invariance"] = dict(same_input=True, same_output=True)
                    study.write(out/"cells"/panel/rv/f"{stage}_{seed}.json", row)
    report.summarize(out)
    assert study.read(out/"audit.json")["terminal"] == 1200
    assert set(study.read(out/"predictions.json")) == {f"P{i}" for i in range(1, 9)}
    assert (out/"RECOVERY_MAP.md").exists()
    path = out/"cells/facebook_a/home/evaluation_1000.json"
    wrong = study.read(path)
    wrong["analysis"]["T"] = 8
    study.write(path, wrong)
    with pytest.raises(RuntimeError, match="Wrong analysis dimensions"):
        report.collect(out)


def test_absence_profile_penalizes_unseen_weeks_at_the_full_population_floor():
    import pandas as pd
    df = pd.DataFrame(dict(entity_id=[0, 0, 1], period=[0, 1, 0], rank=[1, 1, 2], N=[100, 100, 100]))
    prof = inputs.absence_profile(df, K=64)
    # Identity 1's permanent rank is (2 + 101)/2, not observed-week rank 2.
    low = min(prof["rows"], key=lambda r: r["rank"])
    high = max(prof["rows"], key=lambda r: r["rank"])
    assert low["rank"] == 1 and low["probability"] == 0
    assert high["rank"] == 51.5 and high["probability"] == .5
    cal = dict(absence=prof, missing=.9)
    np.testing.assert_allclose(sim.missing_probabilities(cal, [1, 51.5, 10000]), [0, .5, .5])
