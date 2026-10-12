#!/usr/bin/env python3
"""V1: exact frozen-gate wrapper and finite-bank Monte Carlo instrument study.

No protected model file is edited. Sparse per-seed displacement histograms
retain the gate's pooled medians exactly; RACF is a median over seed-level
values, collisions are means. See handoff/V1_gate_instrument/DESIGN.md.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from unittest.mock import patch

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'llm_fitting'))
import minimal_rankdiff as mrd
import rankdiff_kalman as rk
import ladder_explainer as le
sys.path.insert(0, str(ROOT / 'scripts'))
from sealed_data_guard import require_released

SCALES = (1.0, .7, .5, .35, .25, .15, .10, .05, 0.0)
PANELS = ('facebook_a', 'reddit_comments', 'reddit', 'reddit_submissions_long')
REPS_GRID = (3, 5, 10, 20, 40, 80)
REFERENCE = {
    'facebook_a': (.123, .033, .145, .031, .60),
    'reddit_comments': (.171, .046, .165, .062, .60),
    'reddit': (.164, .053, .168, .004, .80),
    'reddit_submissions_long': (.194, .061, .163, .048, .80),
}


def write_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(obj, indent=2, allow_nan=True) + '\n')
    os.replace(tmp, path)


def sha_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(8 * 1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def source_manifest():
    return {str(p.relative_to(ROOT)): sha_file(p) for p in (
        ROOT / 'llm_fitting/minimal_rankdiff.py',
        ROOT / 'llm_fitting/rankdiff_kalman.py', Path(__file__))}


def pack_rep(dist, racf, collisions):
    out = {'racf': np.array(racf), 'collisions': np.array(
        [collisions.get(f'coll{c}', np.nan) for c in (1, 5, 20)])}
    for h, d in dist.items():
        values, counts = np.unique(d, return_counts=True)
        out[f'v{h}'], out[f'n{h}'] = values, counts
    return out


def pool_records(records, horizons):
    # Used by the original gate, which also needs arrays for its CIs/W1.
    ds = {h: np.concatenate([np.repeat(r[f'v{h}'], r[f'n{h}'])
                             for r in records]) for h in horizons}
    rf = float(np.nanmedian([r['racf'] for r in records]))
    cc = np.nanmean([r['collisions'] for r in records], axis=0)
    return ds, rf, dict(zip((1, 5, 20), cc))


def histogram_quantile(values, counts, q=.5):
    """Exactly numpy's default linear quantile of an expanded count vector."""
    values, counts = np.asarray(values), np.asarray(counts, dtype=np.int64)
    total = counts.sum(axis=-1)
    if counts.ndim == 1:
        if total <= 0:
            return np.nan
        at = (total - 1) * q
        lo, hi = int(np.floor(at)), int(np.ceil(at))
        cum = counts.cumsum()
        return float((1 - (at-lo)) * values[np.searchsorted(cum, lo, side='right')]
                     + (at-lo) * values[np.searchsorted(cum, hi, side='right')])
    return np.array([histogram_quantile(values, row, q) for row in counts])


def gate_components(emp, sim, horizons):
    keys = [f'dRank{h}' for h in horizons] + ['RACF1', 'coll1', 'coll5', 'coll20']
    active = [k for k in keys if np.isfinite(emp.get(k, np.nan))
              and np.isfinite(sim.get(k, np.nan)) and abs(emp[k]) >= rk.MOM_FLOOR]
    out = {k: (abs(sim[k]-emp[k])/abs(emp[k])/len(active) if k in active else 0.)
           for k in keys}
    return out, active


def variance_shares(contributions):
    """Covariance allocation, not a sum of marginal variances."""
    c = np.asarray(contributions, dtype=float)
    total = c.sum(axis=1)
    var = np.var(total, ddof=1)
    if var == 0:
        return np.zeros(c.shape[1])
    return ((c-c.mean(axis=0)) * (total-total.mean())[:, None]).sum(axis=0) / (len(c)-1) / var


class BankWrapper:
    """Collect primitive draws around the unchanged original gate call."""
    def __init__(self, out, seed_start, collect_all=False, input_hashes=None):
        self.out, self.seed_start = Path(out), seed_start
        self.collect_all = collect_all
        self.input_hashes = input_hashes or {}
        self.state, self.splits = {}, []
        self.in_cal = False
        self.originals = {n: getattr(rk, n) for n in
            ('_estimate_fast', '_calibrate_scale', 'sim_dist', 'sim_dist_cond',
             '_filtered_levels', '_moments')}
        self.filters = {}

    def fit(self, df_tr, *args, **kwargs):
        p = self.originals['_estimate_fast'](df_tr, *args, **kwargs)
        origin = int(df_tr['period'].max()) + 1
        self.state = dict(origin=origin, p=p, df_tr=df_tr, calls=[], cal_index=0)
        self.filters = {}
        meta = le._param_summary(p)
        meta['origin'] = origin
        meta['train_fingerprint'] = le._fingerprint(df_tr)
        write_json(self.out / str(origin) / 'parameters.json', meta)
        return p

    def filtered(self, df_tr, p):
        key = hashlib.sha256(np.asarray(p.sigma_obs).tobytes()).hexdigest()
        if key not in self.filters:
            self.filters[key] = self.originals['_filtered_levels'](df_tr, p)
        # Caller converts to a copied array; return a copy to prevent mutation.
        return self.filters[key].copy()

    def bank(self, p, T, horizons, reps, mode, scale, df_tr=None, **kwargs):
        dest = self.out / str(self.state['origin']) / mode / f'scale_{scale:g}'
        dest.mkdir(parents=True, exist_ok=True)
        cfg = dict(origin=self.state['origin'], mode=mode, scale=scale, T=T,
                   horizons=list(horizons), params=le._param_summary(p),
                   source_hashes=source_manifest(), input_hashes=self.input_hashes)
        # Cache is reusable only for exactly these inputs, code and mode.
        spec = dest / 'spec.json'
        if spec.exists() and json.loads(spec.read_text()) != json.loads(json.dumps(cfg)):
            # JSON NaN cannot compare equal: compare serialized forms instead.
            if spec.read_text().strip() != json.dumps(cfg, indent=2):
                raise RuntimeError(f'cache specification changed: {dest}')
        if not spec.exists():
            write_json(spec, cfg)
        records = []
        for seed in range(self.seed_start, self.seed_start + reps):
            f = dest / f'seed_{seed:04d}.npz'
            if f.exists():
                with np.load(f, allow_pickle=False) as z:
                    rec = dict(z)
            else:
                if mode == 'train':
                    cr, ti = rk.sim_cohort(p, T, kwargs.get('kappa', 0.), seed=seed)
                else:
                    cr, ti = rk.sim_cohort_conditional(
                        p, df_tr, T, kwargs.get('kappa', 0.), seed=seed,
                        vhat=kwargs.get('vhat'), cond_home=kwargs.get('cond_home', 'state'))
                d, rf = rk._rank_dist(cr, horizons)
                cc = rk._collisions_from_topids(ti, (1, 5, 20))
                rec = pack_rep(d, rf, cc)
                tmp = f.with_suffix('.tmp')
                with open(tmp, 'wb') as out:
                    np.savez_compressed(out, **rec)
                os.replace(tmp, f)
            records.append(rec)
        print(f"bank complete: origin={self.state['origin']} {mode} scale={scale:g} "
              f'seeds={self.seed_start}..{self.seed_start+reps-1}', flush=True)
        return pool_records(records, horizons)

    def sim_train(self, p, T, horizons, reps=3, **kwargs):
        idx = self.state['cal_index']
        self.state['cal_index'] += 1
        return self.bank(p, T, horizons, reps, 'train', SCALES[idx], **kwargs)

    def calibrate(self, p, df_tr, horizons, Ltr, reps=3):
        self.in_cal = True
        self.state['cal_index'] = 0
        try:
            scale = self.originals['_calibrate_scale'](p, df_tr, horizons, Ltr, reps=reps)
        finally:
            self.in_cal = False
        self.state['scale'] = float(scale)
        self.state['hor'] = list(horizons)
        return scale

    def sim_test(self, p, df_tr, T, horizons, reps=3, **kwargs):
        selected = self.state['scale']
        result = self.bank(p, T, horizons, reps, 'test', selected, df_tr=df_tr, **kwargs)
        if self.collect_all:
            for scale in SCALES:
                if scale != selected:
                    self.bank(rk.replace_obs(self.state['p'], scale), T, horizons,
                              reps, 'test', scale, df_tr=df_tr, **kwargs)
        return result

    def moments(self, *args, **kwargs):
        result = self.originals['_moments'](*args, **kwargs)
        if self.in_cal:
            self.state['train_moments'] = dict(result) if self.state['cal_index'] == 0 else self.state.get('train_moments')
        else:
            self.state['calls'].append(dict(result))
            if len(self.state['calls']) == 3:
                emp, base, sim = self.state['calls']
                components, active = gate_components(emp, sim, self.state['hor'])
                split = dict(origin=self.state['origin'], scale=self.state['scale'],
                             horizons=self.state['hor'], emp=emp, base=base, sim=sim,
                             components=components, active=active,
                             train_calibration=self.state['train_moments'])
                self.splits.append(split)
                write_json(self.out / str(self.state['origin']) / 'split.json', split)
        return result

    @contextlib.contextmanager
    def install(self):
        with contextlib.ExitStack() as s:
            for name, fn in (
                ('_estimate_fast', self.fit), ('_calibrate_scale', self.calibrate),
                ('sim_dist', self.sim_train), ('sim_dist_cond', self.sim_test),
                ('_filtered_levels', self.filtered), ('_moments', self.moments)):
                s.enter_context(patch.object(rk, name, fn))
            yield self


def run_panel(panel, phase, output):
    cfg = le.PANELS[panel]
    root = Path(output)
    dest = root / panel / phase
    complete = dest / 'COMPLETE.json'
    if complete.exists():
        print(f'already complete: {panel} {phase}')
        return
    if phase == 'bank':
        for name in PANELS:
            ref = root / name / 'reference' / 'COMPLETE.json'
            if not ref.exists() or not json.loads(ref.read_text()).get('reference_pass'):
                raise RuntimeError(f'all four references must pass before banks: {name}')
    seed_start, reps, boot = ((1000, 80, 2000) if phase == 'bank' else
                             (0, 20, 2000) if panel == 'reddit_submissions_long' else
                             (0, 3, 400))
    dest.mkdir(parents=True, exist_ok=True)
    paths = [mrd.PLATFORMS[panel]['path']]
    if panel == 'facebook_a':
        paths.append(mrd.PLATFORMS[panel]['daily_path'])
    for path in paths:
        require_released(path)
        if not Path(path).exists():
            raise FileNotFoundError(f'required development input unavailable: {path}')
    manifest = dict(panel=panel, phase=phase, seed_start=seed_start, reps=reps,
                    boot=boot, commit=subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip(),
                    sources=source_manifest(), inputs={p:sha_file(p) for p in paths},
                    python=sys.version, numpy=np.__version__, started=time.time())
    write_json(dest / 'STARTED.json', manifest)
    cache = le.PanelCache()
    full = mrd.load_panel(mrd.PLATFORMS[panel])
    T = int(full['period'].max()) + 1
    origins, L = rk._gate_windows(T, 5)
    if cfg['slim']:
        for t0 in origins:
            mrd.restrict_universe(full, cfg['K'], buffer_mult=4, member_window=t0)
        cache.slim(mrd.PLATFORMS[panel])
        del full
    wrapper = BankWrapper(dest, seed_start, collect_all=phase=='bank', input_hashes=manifest['inputs'])
    with (dest/'gate.log').open('w') as log, contextlib.redirect_stdout(le._Tee(sys.stdout,log)), wrapper.install():
        result = rk.oos_movement(panel, n_splits=5, reps=reps, boot=boot,
            top_k=cfg['K'], buffer_mult=4, temper=True, min_knot_n=8,
            md_lags=6, t_tails=True, spec_b=panel=='facebook_a',
            conditional='state', mix_hetero=panel=='reddit_comments', dist_scores=False)
    cache.check_unmutated()
    actual = tuple(result[k] for k in ('model_rel','model_rel_sd','base_rel','base_rel_sd','coverage'))
    passed = all(round(a,3)==round(b,3) for a,b in zip(actual,REFERENCE[panel])) if phase=='reference' else None
    write_json(dest/'RESULT.json', dict(**manifest, result=result, splits=wrapper.splits,
               seconds=time.time()-manifest['started'], reference_pass=passed))
    if phase=='reference' and not passed:
        raise RuntimeError(f'REFERENCE MISMATCH: {actual}, recorded {REFERENCE[panel]}; STOP V1')
    write_json(complete,dict(**manifest, result=result, reference_pass=passed,
                            seconds=time.time()-manifest['started']))
    print(f'COMPLETE {panel} {phase}: {result}')


def main():
    a = argparse.ArgumentParser(description=__doc__)
    sub=a.add_subparsers(dest='command',required=True)
    r=sub.add_parser('run'); r.add_argument('--panel',choices=PANELS,required=True)
    r.add_argument('--phase',choices=['reference','bank'],required=True)
    r.add_argument('--out',default='llm_fitting/runs/2026-10-12_V1_gate_instrument')
    args=a.parse_args()
    if args.command=='run':
        dest=Path(args.out)/args.panel/args.phase
        dest.mkdir(parents=True,exist_ok=True)
        attempt=dict(time=time.time(), command=sys.argv, status='STARTED')
        journal=dest/'ATTEMPTS.jsonl'
        with journal.open('a') as f: f.write(json.dumps(attempt)+'\n')
        try:
            run_panel(args.panel,args.phase,args.out)
        except BaseException as exc:
            with journal.open('a') as f:
                f.write(json.dumps(dict(attempt, status='RAN-FAILED', error=repr(exc)))+'\n')
            raise
        else:
            with journal.open('a') as f:
                f.write(json.dumps(dict(attempt, status='RAN-OK', finished=time.time()))+'\n')

if __name__=='__main__':
    main()
