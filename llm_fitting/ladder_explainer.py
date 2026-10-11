#!/usr/bin/env python3
"""
Step-by-step ladder from a simple rank diffusion to the frozen stacks.

EXPLORATORY, DEVELOPMENT data only (MODEL_STATUS 2z-ap).  The design -- panels,
rungs, both scores, the combined index, the dataset weights and the
predictions -- was written to llm_fitting/runs/2026-10-12_ladder/DESIGN.md
before any cell was run; this file implements it.  Nothing here changes a
default, a frozen stack or the gate: every rung is reached through the public
estimator and simulator arguments, plus restricted versions of the
minimum-distance partition that are swapped in for the duration of one fit
and restored afterwards.

Two subcommands:

    run        compute cells (one JSON per panel x kind x rung, written
               atomically; a cell with a JSON on disk is skipped)
    summarize  read the cells, compute the indices and bands, write
               summary.json and the GATE_LEDGER rows

Each rung is a REFIT.  `kind` is `struct` (in-sample unconditional simulation,
the macro-stability score) or `gate` (Gate v1, the held-out movement score).

    python -u llm_fitting/ladder_explainer.py run --panel facebook_a \
        --out llm_fitting/runs/2026-10-12_ladder
    python llm_fitting/ladder_explainer.py summarize \
        --out llm_fitting/runs/2026-10-12_ladder
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import community_metrics as cm  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402

# --------------------------------------------------------------------------- #
# Declared design (DESIGN.md)
# --------------------------------------------------------------------------- #
CORE = ["R0", "R1", "R2", "R3", "R4", "R5"]
LONG_STRUCT = CORE + ["S1", "S2", "S3", "S4"]

PANELS = {
    "facebook_a": dict(
        label="Facebook Era A", struct="facebook_a", gate="facebook_a", K=3500,
        system="fb", platform="facebook", slim=False,
        struct_path=LONG_STRUCT, gate_path=CORE + ["G1", "G2"], g2="spec_b"),
    "reddit_comments": dict(
        label="Reddit comments", struct="reddit_comments", gate="reddit_comments",
        K=12500, system="rc", platform="reddit", slim=True,
        struct_path=LONG_STRUCT, gate_path=CORE + ["G1", "G2"], g2="mix_hetero"),
    "reddit_submissions_long": dict(
        label="Reddit submissions, long", struct="reddit_submissions_long",
        gate="reddit_submissions_long", K=5000, system="rs", platform="reddit",
        slim=True, struct_path=LONG_STRUCT, gate_path=CORE + ["G1"], g2=None),
    "reddit": dict(
        label="Reddit submissions, short", struct="reddit", gate="reddit", K=5000,
        system="rs", platform="reddit", slim=False,
        struct_path=CORE, gate_path=CORE + ["G1"], g2=None),
    "instagram": dict(
        label="Instagram rescue", struct="instagram_hm", gate="instagram_hm_ts",
        K=10000, system="ig", platform="instagram", slim=False,
        struct_path=CORE + ["S2"], gate_path=CORE + ["G1"], g2=None,
        member_ids_file="llm_fitting/ig_trainsafe_members.parquet"),
}

RUNG_LABEL = {
    "R0": "Rank diffusion", "R1": "+ noise", "R2": "+ home",
    "R3": "+ short-lived swings", "R4": "+ endpoint amplitude",
    "R5": "+ fat tails", "S1": "+ long-horizon moments",
    "S2": "+ stationary common level", "S3": "+ medium component",
    "S4": "+ amplitude on the permanent part",
    "G1": "+ forecast from the actual state", "G2": "+ panel-specific identification",
}

# Macro-stability index: blocks, statistics, error type.  "ratio" errors are
# |ln(sim/emp)| / ln(1.2); "abs" errors are |sim - emp| / tolerance.
RATIO_TOL = float(np.log(1.2))
ABS_TOL = 0.08
ERR_CAP = 20.0
MACRO_BLOCKS = {
    "ladder": [("S1", "ratio"), ("S10", "ratio"), ("S100", "ratio")],
    "variance": [("VR2", "ratio"), ("VR4", "ratio"), ("VR8", "ratio"),
                 ("VR13", "ratio"), ("ACF1", "abs"), ("ACF2", "abs")],
    "order": [("rR2_1", "abs"), ("rR2_4", "abs"), ("rR2_13", "abs"),
              ("RACF1", "abs"), ("RACF4", "abs"), ("RACF13", "abs")],
    "topset": [("rPers1", "pers"), ("rPers4", "pers"), ("rPers13", "pers"),
               ("outfluxK", "ratio")],
}

WEIGHTS = {
    "primary": {"fb": 1 / 3, "rc": 1 / 3, "rs": 1 / 3, "ig": 0.0},
    "D_instagram_as_fourth_system": {"fb": 0.25, "rc": 0.25, "rs": 0.25, "ig": 0.25},
}
GATE_KEYS_C = (1, 5, 20)
SEEDS = 20
N_SPLITS = 5


def spec_for(panel: str, kind: str, rung: str) -> dict:
    """Cumulative flags for a rung on a panel's structure or gate path."""
    cfg = PANELS[panel]
    path = cfg["struct_path"] if kind == "struct" else cfg["gate_path"]
    if rung not in path:
        raise SystemExit(f"{rung} is not on the {kind} path of {panel}: {path}")
    on = path[: path.index(rung) + 1]
    core = [r for r in on if r.startswith("R")]
    part = {"R0": "rw", "R1": "rw_noise", "R2": "ou_noise"}.get(core[-1], "full")
    spec = dict(partition=part, temper="R4" in on, t_tails="R5" in on,
                md_vr_long="S1" in on, stat_factor="S2" in on,
                two_scale="S3" in on, mix_hetero="S4" in on,
                conditional="state" if "G1" in on else None, spec_b=False)
    if "G2" in on:
        spec[cfg["g2"]] = True
    return spec


def param_count(spec: dict) -> dict:
    """Rank profiles and scalars a rung estimates, beyond what every rung
    shares (exit-rate and factor-loading profiles, sigma_F, the seeded
    ladder)."""
    prof = {"rw": 1, "rw_noise": 2, "ou_noise": 3, "full": 5}[spec["partition"]]
    prof += 2 * bool(spec["two_scale"])
    scal = (bool(spec["temper"]) + bool(spec["t_tails"]) + bool(spec["stat_factor"])
            + bool(spec["mix_hetero"]))
    return dict(profiles=int(prof), scalars=int(scal))


# --------------------------------------------------------------------------- #
# Restricted minimum-distance partitions (R0-R2)
# --------------------------------------------------------------------------- #
RW_A = 0.9999   # reversion 1e-4 per week: a random-walk level to four decimals
_ORIG_PARTITION = mrd._md_partition


@contextlib.contextmanager
def _grids(a_grid=None, phi_grid=None):
    save = (mrd.A_GRID, mrd.PHI_GRID)
    if a_grid is not None:
        mrd.A_GRID = np.asarray(a_grid, dtype=float)
    if phi_grid is not None:
        mrd.PHI_GRID = np.asarray(phi_grid, dtype=float)
    try:
        yield
    finally:
        mrd.A_GRID, mrd.PHI_GRID = save


def part_rw(gk, **kw):
    """R0: every weekly change is permanent.  sigma_eta^2 = gamma_0."""
    return 0.0, float(np.sqrt(max(float(gk[0]), 0.0))), 0.0, 0.0, 0.0


def part_rw_noise(gk, **kw):
    """R1: random-walk level + white noise, fitted to gamma_0..L by the
    committed solver with the reversion grid fixed at RW_A and phi at 0.  The
    white component is returned as sigma_obs."""
    with _grids([RW_A], [0.0]):
        _, s_eta, _, s_white, _ = _ORIG_PARTITION(gk, **{**kw, "s_e_fix": 0.0})
    return 0.0, s_eta, 0.0, 0.0, s_white


def part_ou_noise(gk, **kw):
    """R2: OU level + white noise (phi fixed at 0), committed reversion grid."""
    with _grids(None, [0.0]):
        kap, s_eta, _, s_white, _ = _ORIG_PARTITION(gk, **{**kw, "s_e_fix": 0.0})
    return kap, s_eta, 0.0, 0.0, s_white


PARTITIONS = {"rw": part_rw, "rw_noise": part_rw_noise, "ou_noise": part_ou_noise,
              "full": None}


@contextlib.contextmanager
def partition(name: str):
    """Swap the restricted partition in for one fit; always restore."""
    fn = PARTITIONS[name]
    save = mrd._md_partition
    if fn is not None:
        mrd._md_partition = fn
    try:
        yield
    finally:
        mrd._md_partition = save


# --------------------------------------------------------------------------- #
# Panel and universe cache (one load per process; optional slimming)
# --------------------------------------------------------------------------- #
class PanelCache:
    """Memoizes load_panel and restrict_universe inside one process so that
    every rung of a panel sees the same frames.  With slim(), the full panel
    is replaced by a one-row-per-entity stand-in once the universes a run
    needs exist; a later cache miss then fails closed."""

    def __init__(self):
        self._load, self._ru = mrd.load_panel, mrd.restrict_universe
        self.panels, self.universes, self.slimmed = {}, {}, set()
        mrd.load_panel, mrd.restrict_universe = self.load_panel, self.restrict_universe

    @staticmethod
    def _pkey(cfg):
        return (cfg["path"], cfg.get("date_min"), cfg.get("date_max"), cfg.get("max_rank"))

    def load_panel(self, cfg):
        key = self._pkey(cfg)
        if key not in self.panels:
            df = self._load(cfg)
            df.attrs["_ladder_key"] = key
            self.panels[key] = df
        return self.panels[key]

    def restrict_universe(self, df, top_k, buffer_mult=4, member_window=None,
                          member_span=None, member_ids=None):
        pkey = df.attrs.get("_ladder_key")
        # only the cached panel object itself is memoized: frames derived from
        # it inherit attrs and must take the ordinary path
        if pkey is None or self.panels.get(pkey) is not df or member_span is not None:
            return self._ru(df, top_k, buffer_mult=buffer_mult,
                            member_window=member_window, member_span=member_span,
                            member_ids=member_ids)
        mid = None
        if member_ids is not None:
            mid = hashlib.sha1("\n".join(sorted(map(str, member_ids))).encode()).hexdigest()
        key = (pkey, int(top_k), int(buffer_mult), member_window, mid)
        if key not in self.universes:
            if pkey in self.slimmed:
                raise RuntimeError(f"universe {key[1:]} requested after the panel "
                                   f"was slimmed -- refusing to rebuild from a stub")
            u = self._ru(df, top_k, buffer_mult=buffer_mult,
                         member_window=member_window, member_ids=member_ids)
            self.universes[key] = (u, _fingerprint(u))
        return self.universes[key][0]

    def slim(self, cfg):
        key = self._pkey(cfg)
        df = self.panels[key]
        t_last = int(df["period"].max())
        stub = pd.concat([df.drop_duplicates("entity_id"),
                          df[df["period"] == t_last].head(1)], ignore_index=True)
        stub.attrs["_ladder_key"] = key
        self.panels[key] = stub
        self.slimmed.add(key)

    def check_unmutated(self):
        for key, (u, fp) in self.universes.items():
            if _fingerprint(u) != fp:
                raise RuntimeError(f"cached universe {key[1:]} was mutated in place")


def _fingerprint(df) -> tuple:
    return (len(df), float(df["X"].sum()), int(df["rank"].sum()),
            int(df["period"].sum()))


# --------------------------------------------------------------------------- #
# Structure cell
# --------------------------------------------------------------------------- #
def _stats(values, ranks, top_ids, ranksize, top_k, score_k, M) -> dict:
    """Card rows plus the rolling and ladder statistics, one code path for
    empirical and simulated structures."""
    card = mrd.diagnostics(values, ranks, top_ids, ranksize, top_k, score_k=score_k)
    out = {k: float(v) for k, v in card.items() if not k.startswith("_")}
    V = cm._scored_complete(values, ranks, score_k)
    for k, (m, _) in cm.rolling_r2(V).items():
        out["r" + k] = float(m)
    for k, (m, _) in cm.rolling_pers(top_ids, top_k).items():
        out["r" + k] = float(m)
    rs = ranksize[:, :M]
    for k in (1, 10, 100):
        out[f"S{k}"] = float(cm.top_share(rs, k))
    out["o_dot"] = float(cm.flux_turnover(top_ids, score_k)["o_dot"])
    out["ladder_drift"] = float(cm.ladder_drift(rs))
    return out


def _fit(df, spec, sigma_obs_fix=None):
    with partition(spec["partition"]):
        return mrd.estimate(
            df, temper=spec["temper"], min_knot_n=8, md_lags=6,
            t_tails=spec["t_tails"], sigma_obs_fix=sigma_obs_fix,
            md_vr_long=spec["md_vr_long"], stat_factor=spec["stat_factor"],
            two_scale=spec["two_scale"], mix_hetero=spec["mix_hetero"])


def _param_summary(p) -> dict:
    def arr(a):
        return None if a is None else [float(x) for x in np.asarray(a)]
    return dict(
        N=int(p.N), temper_s=float(p.temper_s), t_df=float(p.t_df),
        mix_b=float(p.mix_b),
        factor_rho=None if p.factor_rho is None else float(p.factor_rho),
        z_knots=arr(p.z_knots), kappa_z=arr(p.kappa_z), phi=arr(p.phi),
        sigma_perm=arr(p.sigma_perm), sigma_trans=arr(p.sigma_trans),
        sigma_obs=arr(p.sigma_obs), phi2=arr(p.phi2),
        sigma_trans2=arr(p.sigma_trans2))


_EMP = {}


def run_struct(panel: str, rung: str, reps: int) -> dict:
    cfg = PANELS[panel]
    spec = spec_for(panel, "struct", rung)
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                               cfg["K"], buffer_mult=4)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    top_k = max(10, int(round(0.01 * score_k)))
    p = _fit(df, spec)
    if panel not in _EMP:
        ev, er, et, ers = mrd.empirical_structures(df, top_k, topid_k=score_k)
        M = int(min(ers.shape[1], min(p.N, 2000)))
        _EMP[panel] = (_stats(ev, er, et, ers, top_k, score_k, M), M)
    emp, M = _EMP[panel]
    seeds = []
    for s in range(reps):
        sim = mrd.simulate(p, T, seed=s, kappa=None, top_record=score_k)
        seeds.append(_stats(*mrd._sim_struct(sim), top_k, score_k, M))
        print(f"    seed {s + 1}/{reps}", flush=True)

    def card(n):
        mean = {k: float(np.nanmean([d[k] for d in seeds[:n]])) for k in emp}
        npass, ntot, churn = mrd._score(emp, mean, top_k)
        return dict(passes=int(npass), total=int(ntot), churn_err=float(churn))

    return dict(panel=panel, kind="struct", rung=rung, spec=spec,
                params=param_count(spec), platform=cfg["struct"], K=cfg["K"],
                T=T, top_k=top_k, M=M, reps=reps, emp=emp, seeds=seeds,
                card_5=card(min(5, reps)), card_all=card(reps),
                fit=_param_summary(p))


# --------------------------------------------------------------------------- #
# Gate cell (Gate v1 through rk.oos_movement, moments recorded on the way)
# --------------------------------------------------------------------------- #
class _Tee(io.TextIOBase):
    def __init__(self, *streams):
        self.streams = streams

    def write(self, s):
        for st in self.streams:
            st.write(s)
        return len(s)

    def flush(self):
        for st in self.streams:
            st.flush()


@contextlib.contextmanager
def _record_gate():
    """Record the (held-out, baseline, model) moment vectors the gate builds
    per split, and skip the sigma_obs-scale search when sigma_obs is
    identically zero (the nine candidates are then one simulation; the
    committed search returns 1.0)."""
    rec = dict(calls=[], in_cal=False)
    m0, c0 = rk._moments, rk._calibrate_scale

    def moments(dist, racf1, colls, horizons):
        out = m0(dist, racf1, colls, horizons)
        if not rec["in_cal"]:
            rec["calls"].append({k: float(v) for k, v in out.items()})
        return out

    def calibrate(p, df_tr, horizons, Ltr, reps=3):
        if not np.any(np.asarray(p.sigma_obs) > 0):
            return 1.0
        rec["in_cal"] = True
        try:
            return c0(p, df_tr, horizons, Ltr, reps=reps)
        finally:
            rec["in_cal"] = False

    rk._moments, rk._calibrate_scale = moments, calibrate
    try:
        yield rec
    finally:
        rk._moments, rk._calibrate_scale = m0, c0


def split_errors(em: dict, bm: dict, sm: dict, hor) -> tuple[float, float]:
    """The gate's per-split relative errors, recomputed from the recorded
    moment vectors exactly as rk.oos_movement computes them."""
    keys = [f"dRank{h}" for h in hor] + ["RACF1"] + [f"coll{c}" for c in GATE_KEYS_C]
    kk = [k for k in keys if np.isfinite(em.get(k, np.nan))
          and np.isfinite(sm.get(k, np.nan)) and abs(em[k]) >= rk.MOM_FLOOR]
    me = float(np.mean([abs(sm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk]))
    be = float(np.mean([abs(bm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk
                        if np.isfinite(bm.get(k, np.nan))]))
    return me, be


def run_gate(panel: str, rung: str, reps: int, boot: int, log_path: Path) -> dict:
    cfg = PANELS[panel]
    spec = spec_for(panel, "gate", rung)
    plat = cfg["gate"]
    T = int(mrd.load_panel(mrd.PLATFORMS[plat])["period"].max()) + 1
    origins, test_len = rk._gate_windows(T, N_SPLITS)
    hor = [h for h in (1, 4, 13) if h < test_len]
    buf = io.StringIO()
    with open(log_path, "w", encoding="utf-8") as lf, _record_gate() as rec, \
            partition(spec["partition"]), \
            contextlib.redirect_stdout(_Tee(sys.__stdout__, lf, buf)):
        res = rk.oos_movement(
            plat, n_splits=N_SPLITS, reps=reps, boot=boot, top_k=cfg["K"],
            buffer_mult=4, temper=spec["temper"], min_knot_n=8, md_lags=6,
            t_tails=spec["t_tails"], spec_b=spec["spec_b"],
            conditional=spec["conditional"], mix_hetero=spec["mix_hetero"],
            dist_scores=True, member_ids_file=cfg.get("member_ids_file"))
    calls = rec["calls"]
    if len(calls) != 3 * len(origins):
        raise RuntimeError(f"expected {3 * len(origins)} recorded moment vectors, "
                           f"got {len(calls)}")
    splits = []
    for i, T0 in enumerate(origins):
        em, bm, sm = calls[3 * i: 3 * i + 3]
        me, be = split_errors(em, bm, sm, hor)
        splits.append(dict(T0=int(T0), scale=float(res["scales"][i]), model=me,
                           baseline=be, emp=em, base=bm, sim=sm))
    me_all = [s["model"] for s in splits]
    be_all = [s["baseline"] for s in splits]
    if abs(np.mean(me_all) - res["model_rel"]) > 1e-9 or \
            abs(np.mean(be_all) - res["base_rel"]) > 1e-9:
        raise RuntimeError("recomputed gate errors differ from the gate's own summary")
    crps = {int(h): (float(m), float(sd)) for h, m, sd in re.findall(
        r"CRPS skill vs persistence @ dRank(\d+):\s+([+-][\d.]+) ± ([\d.]+)",
        buf.getvalue())}
    return dict(panel=panel, kind="gate", rung=rung, spec=spec,
                params=param_count(spec), platform=plat, K=cfg["K"], T=T,
                origins=[int(o) for o in origins], test_len=int(test_len),
                reps=reps, boot=boot, splits=splits,
                model_rel=float(res["model_rel"]),
                model_rel_sd=float(res["model_rel_sd"]),
                base_rel=float(res["base_rel"]), base_rel_sd=float(res["base_rel_sd"]),
                coverage=float(res["coverage"]), crps_skill=crps,
                log=str(log_path))


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def _commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       text=True).strip()
    except Exception:
        return "unknown"


def _write_json(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def cmd_run(a) -> None:
    out = Path(a.out)
    (out / "cells").mkdir(parents=True, exist_ok=True)
    (out / "logs").mkdir(parents=True, exist_ok=True)
    cfg = PANELS[a.panel]
    todo = []
    for kind in a.kinds:
        path = cfg["struct_path"] if kind == "struct" else cfg["gate_path"]
        for rung in (a.rungs or path):
            if rung in path:
                todo.append((kind, rung))
    todo = [(k, r) for k, r in todo
            if a.force or not (out / "cells" / f"{a.panel}__{k}__{r}.json").exists()]
    print(f"[{a.panel}] cells to run: {todo}", flush=True)
    if not todo:
        return
    cache = PanelCache()
    if cfg["slim"]:
        t0 = time.time()
        kinds = {k for k, _ in todo}
        if "struct" in kinds:
            mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                                  cfg["K"], buffer_mult=4)
        if "gate" in kinds:
            full = mrd.load_panel(mrd.PLATFORMS[cfg["gate"]])
            T = int(full["period"].max()) + 1
            for T0 in rk._gate_windows(T, N_SPLITS)[0]:
                mrd.restrict_universe(full, cfg["K"], buffer_mult=4, member_window=T0)
        for plat in {cfg["struct"], cfg["gate"]}:
            if cache._pkey(mrd.PLATFORMS[plat]) in cache.panels:
                cache.slim(mrd.PLATFORMS[plat])
        print(f"[{a.panel}] universes prebuilt and panel slimmed in "
              f"{time.time() - t0:.0f}s", flush=True)
    meta = dict(commit=_commit(), python=sys.version.split()[0],
                pandas=pd.__version__, numpy=np.__version__)
    for kind, rung in todo:
        if not a.force and (out / "cells" / f"{a.panel}__{kind}__{rung}.json").exists():
            continue   # written by another process since this one started
        t0 = time.time()
        print(f"\n##### {a.panel} {kind} {rung}  "
              f"{spec_for(a.panel, kind, rung)}", flush=True)
        if kind == "struct":
            res = run_struct(a.panel, rung, a.reps)
        else:
            res = run_gate(a.panel, rung, a.reps, a.boot,
                           out / "logs" / f"{a.panel}__gate__{rung}.log")
        cache.check_unmutated()
        res.update(meta, seconds=round(time.time() - t0, 1),
                   finished=time.strftime("%Y-%m-%d %H:%M:%S"))
        _write_json(out / "cells" / f"{a.panel}__{kind}__{rung}.json", res)
        print(f"##### done {a.panel} {kind} {rung} in {res['seconds']:.0f}s",
              flush=True)
    print(f"[{a.panel}] ALL REQUESTED CELLS DONE", flush=True)


# --------------------------------------------------------------------------- #
# Indices
# --------------------------------------------------------------------------- #
def stat_error(name: str, kind: str, emp: float, sim: float, top_k: int) -> float:
    """Error of one macro statistic in tolerance units (capped)."""
    if not (np.isfinite(emp) and np.isfinite(sim)):
        return np.nan
    if kind == "ratio":
        if emp <= 0 or sim <= 0:
            return ERR_CAP
        e = abs(np.log(sim / emp)) / RATIO_TOL
    elif kind == "abs":
        e = abs(sim - emp) / ABS_TOL
    else:
        e = abs(sim - emp) / max(3, int(round(0.15 * top_k)))
    return float(min(e, ERR_CAP))


def macro_index(emp: dict, sim_mean: dict, top_k: int) -> tuple[float, dict]:
    blocks = {}
    for b, stats in MACRO_BLOCKS.items():
        errs = [stat_error(n, k, emp.get(n, np.nan), sim_mean.get(n, np.nan), top_k)
                for n, k in stats]
        errs = [e for e in errs if np.isfinite(e)]
        blocks[b] = float(np.mean(errs)) if errs else np.nan
    return float(np.nanmean(list(blocks.values()))), blocks


def _seed_mean(seeds: list, idx=None) -> dict:
    use = seeds if idx is None else [seeds[i] for i in idx]
    return {k: float(np.nanmean([d[k] for d in use])) for k in seeds[0]}


def panel_weights(scheme: str, present: list) -> dict:
    """Weight per panel.  System votes are split equally over the system's
    panels that are present."""
    if scheme == "A_one_vote_per_platform":
        w = {}
        fb = [p for p in present if PANELS[p]["platform"] == "facebook"]
        rd = [p for p in present if PANELS[p]["platform"] == "reddit"]
        for p in fb:
            w[p] = 0.5 / len(fb)
        systems = sorted({PANELS[p]["system"] for p in rd})
        for s in systems:
            members = [p for p in rd if PANELS[p]["system"] == s]
            for p in members:
                w[p] = 0.5 / len(systems) / len(members)
        return w
    core = [p for p in present if PANELS[p]["system"] != "ig"]
    if scheme == "B_one_vote_per_panel":
        return {p: 1.0 / len(core) for p in core}
    if scheme == "C_panel_length":
        return None  # filled by the caller, which knows T
    sysw = WEIGHTS[scheme]
    w = {}
    for s, ws in sysw.items():
        members = [p for p in present if PANELS[p]["system"] == s]
        for p in members:
            w[p] = ws / len(members)
    tot = sum(w.values())
    return {p: v / tot for p, v in w.items() if v > 0}


def cmd_summarize(a) -> None:
    out = Path(a.out)
    cells = {}
    for f in sorted((out / "cells").glob("*.json")):
        c = json.loads(f.read_text())
        cells[(c["panel"], c["kind"], c["rung"])] = c
    rng = np.random.default_rng(20261012)
    summ = dict(panels={}, design=dict(
        blocks={b: [n for n, _ in s] for b, s in MACRO_BLOCKS.items()},
        rung_label=RUNG_LABEL, seeds=SEEDS, n_splits=N_SPLITS))
    draws = {}
    B = a.draws
    for panel, cfg in PANELS.items():
        sp = [r for r in cfg["struct_path"] if (panel, "struct", r) in cells]
        gp = [r for r in cfg["gate_path"] if (panel, "gate", r) in cells]
        if sp != cfg["struct_path"] or gp != cfg["gate_path"]:
            print(f"[{panel}] incomplete: struct {sp} gate {gp} -- skipped")
            continue
        macro, move = {}, {}
        dm, dg = {}, {}
        n_seed = cells[(panel, "struct", "R0")]["reps"]
        n_split = len(cells[(panel, "gate", "R0")]["splits"])
        seed_idx = rng.integers(0, n_seed, size=(B, n_seed))
        split_idx = rng.integers(0, n_split, size=(B, n_split))
        for r in sp:
            c = cells[(panel, "struct", r)]
            idx, blocks = macro_index(c["emp"], _seed_mean(c["seeds"]), c["top_k"])
            boot = np.array([macro_index(c["emp"], _seed_mean(c["seeds"], i),
                                         c["top_k"])[0] for i in seed_idx])
            dm[r] = boot
            sm = _seed_mean(c["seeds"])
            macro[r] = dict(
                index=idx, blocks=blocks,
                lo=float(np.percentile(boot, 5)), hi=float(np.percentile(boot, 95)),
                card=c["card_5"], card_all=c["card_all"], params=c["params"],
                stats={n: dict(emp=c["emp"].get(n), sim=sm.get(n),
                               err=stat_error(n, k, c["emp"].get(n, np.nan),
                                              sm.get(n, np.nan), c["top_k"]))
                       for s in MACRO_BLOCKS.values() for n, k in s},
                fit={k: c["fit"][k] for k in ("temper_s", "t_df", "mix_b", "factor_rho")},
                seconds=c["seconds"])
        prev_me = None
        for r in gp:
            c = cells[(panel, "gate", r)]
            me = np.array([s["model"] for s in c["splits"]])
            be = np.array([s["baseline"] for s in c["splits"]])
            dg[r] = me[split_idx].mean(axis=1)
            # paired change from the rung below, split by split
            step = None if prev_me is None else dict(
                mean=float(np.mean(me - prev_me)), sd=float(np.std(me - prev_me)),
                n_better=int(np.sum(me < prev_me)), n=int(me.size))
            prev_me = me
            keys = [k for k in c["splits"][0]["emp"]]
            move[r] = dict(
                index=c["model_rel"], sd=c["model_rel_sd"], baseline=c["base_rel"],
                baseline_sd=c["base_rel_sd"], coverage=c["coverage"],
                per_split=[float(x) for x in me], base_per_split=[float(x) for x in be],
                scales=[s["scale"] for s in c["splits"]], origins=c["origins"],
                test_len=c["test_len"], crps_skill=c["crps_skill"],
                params=c["params"], step=step,
                n_below_baseline=int(np.sum(me < be)),
                moments={k: dict(emp=float(np.mean([s["emp"][k] for s in c["splits"]])),
                                 sim=float(np.mean([s["sim"][k] for s in c["splits"]])),
                                 base=float(np.mean([s["base"][k] for s in c["splits"]])))
                         for k in keys},
                seconds=c["seconds"], log=c["log"], commit=c["commit"],
                finished=c["finished"], spec=c["spec"], reps=c["reps"],
                platform=c["platform"])
        # unified sequence: core, then structure-only, then movement-only
        seq = CORE + [r for r in sp if r.startswith("S")] + \
            [r for r in gp if r.startswith("G")]
        comb = {}
        cdraw = {}
        for r in seq:
            rm = r if r in macro else sp[-1]              # movement step: macro stays
            rg = r if r in move else "R5"                 # structure step: gate stays
            if r.startswith("G"):
                rm = sp[-1]
            lm = np.log(macro[rm]["index"] / macro["R0"]["index"])
            lg = np.log(move[rg]["index"] / move["R0"]["index"])
            d = 0.5 * np.log(dm[rm] / dm["R0"]) + 0.5 * np.log(dg[rg] / dg["R0"])
            cdraw[r] = d
            comb[r] = dict(macro_rung=rm, gate_rung=rg, log_macro=float(lm),
                           log_move=float(lg), index=float(np.exp(0.5 * lm + 0.5 * lg)),
                           lo=float(np.exp(np.percentile(d, 5))),
                           hi=float(np.exp(np.percentile(d, 95))))
        draws[panel] = cdraw
        summ["panels"][panel] = dict(
            label=cfg["label"], system=cfg["system"], platform=cfg["platform"],
            K=cfg["K"], T=cells[(panel, "struct", "R0")]["T"], struct_path=sp,
            gate_path=gp, sequence=seq, macro=macro, move=move, combined=comb)

    present = list(summ["panels"])
    schemes = {}
    names = ["primary", "A_one_vote_per_platform", "B_one_vote_per_panel",
             "C_panel_length", "D_instagram_as_fourth_system"]
    # composite on the common sequence: core, then "all structure steps",
    # then "all movement steps" (each panel's own final rungs)
    for name in names:
        w = panel_weights(name, present)
        if name == "C_panel_length":
            core = [p for p in present if PANELS[p]["system"] != "ig"]
            tot = sum(summ["panels"][p]["T"] for p in core)
            w = {p: summ["panels"][p]["T"] / tot for p in core}
        if not w:
            continue
        steps = {}
        for step in CORE + ["S", "G"]:
            logs, dsum = 0.0, 0.0
            lmac, lmov = 0.0, 0.0
            for p, wp in w.items():
                P = summ["panels"][p]
                if step == "S":
                    r = ([x for x in P["sequence"] if x.startswith("S")] or ["R5"])[-1]
                elif step == "G":
                    r = P["sequence"][-1]
                else:
                    r = step
                logs += wp * np.log(P["combined"][r]["index"])
                lmac += wp * P["combined"][r]["log_macro"]
                lmov += wp * P["combined"][r]["log_move"]
                dsum = dsum + wp * draws[p][r]
            steps[step] = dict(index=float(np.exp(logs)), macro=float(np.exp(lmac)),
                               move=float(np.exp(lmov)),
                               lo=float(np.exp(np.percentile(dsum, 5))),
                               hi=float(np.exp(np.percentile(dsum, 95))))
        schemes[name] = dict(weights=w, steps=steps)
    summ["composite"] = schemes
    _write_json(out / "summary.json", summ)

    # GATE_LEDGER rows, one per gate cell, oldest first
    rows = []
    for (panel, kind, rung), c in sorted(cells.items(), key=lambda kv: kv[1]["finished"]):
        if kind != "gate":
            continue
        s = c["spec"]
        flags = [f"ladder {rung}", f"partition={s['partition']}"]
        flags += [k for k in ("temper", "t_tails", "mix_hetero", "spec_b") if s[k]]
        if s["conditional"]:
            flags.append("conditional=state")
        if PANELS[panel].get("member_ids_file"):
            flags.append("exact per-origin membership")
        rows.append(
            f"| {c['finished'][:10]} | Claude Opus 5.5 | {c['commit']} | "
            f"`{c['platform']}` K={c['K']} | exploratory | {'; '.join(flags)} | "
            f"{len(c['splits'])} × {c['reps']} | {c['model_rel']:.3f} ± "
            f"{c['model_rel_sd']:.3f} | {c['base_rel']:.3f} ± {c['base_rel_sd']:.3f} | "
            f"{100 * c['coverage']:.0f}% | `{c['log']}`; §2z-ap | ladder explainer; "
            f"not a candidate |")
    (out / "gate_ledger_rows.md").write_text("\n".join(rows) + "\n")
    print(f"summary.json written for {present}; {len(rows)} gate-ledger rows")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--panel", required=True, choices=sorted(PANELS))
    r.add_argument("--kinds", nargs="+", default=["struct", "gate"],
                   choices=("struct", "gate"))
    r.add_argument("--rungs", nargs="+", default=None)
    r.add_argument("--reps", type=int, default=SEEDS)
    r.add_argument("--boot", type=int, default=2000)
    r.add_argument("--out", required=True)
    r.add_argument("--force", action="store_true")
    r.set_defaults(fn=cmd_run)
    s = sub.add_parser("summarize")
    s.add_argument("--out", required=True)
    s.add_argument("--draws", type=int, default=2000)
    s.set_defaults(fn=cmd_summarize)
    a = ap.parse_args()
    for st in (sys.stdout, sys.stderr):
        st.reconfigure(encoding="utf-8", errors="replace")
    a.fn(a)


if __name__ == "__main__":
    main()
