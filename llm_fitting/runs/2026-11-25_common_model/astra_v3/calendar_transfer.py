"""Supplement the frozen row-104 benchmark with the calendar dates in the brief."""
import gc
import json
import sys
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import common_rankdiff_astra_r7 as candidate

workspace = Path(__file__).resolve().parent
sys.path.insert(0, str(workspace.parent))
import round3_gold_benchmark as gold

frozen_stats = gold.structure_stats
frozen_aggregate = gold._agg_structure_sims


def calendar_stats(X, R, K, cfg, ids=None):
    stats = frozen_stats(X, R, K, cfg, ids)
    # Same slot-based block turnover convention as the official two-block score.
    # Annual starts are 52 weeks apart. Both blocks have 51 weeks of observations.
    orders = [gold._order_desc(gold.wmean(X, t, t + 51, cfg['pres'])) for t in (0, 52)]
    for n in (10, K // 4):
        stats[f'calendar_turn_{n}'] = 1 - np.intersect1d(orders[0][:n], orders[1][:n]).size / n
    return stats


def calendar_aggregate(rows):
    return {**frozen_aggregate(rows), **{k: gold._band_agg(rows, k)
            for k in rows[0] if k.startswith('calendar_turn_')}}


def main():
    gold.structure_stats = calendar_stats
    gold._agg_structure_sims = calendar_aggregate
    path = workspace / 'calendar_transfer.json'
    out = json.loads(path.read_text()) if path.exists() else {}
    original = SimpleNamespace(**vars(candidate))
    original.estimate_common = partial(candidate.estimate_common, scale_moment='meta')
    for panel in ('reddit_comments_ext', 'reddit_submissions_long'):
        data, _ = gold.load_cached(panel)
        dates = data['dates'].astype('datetime64[D]')
        select = (dates >= np.datetime64('2019-01-01')) & (dates < np.datetime64('2023-01-01'))
        calendar = dates[select]
        assert (calendar < np.datetime64('2021-01-01')).sum() == 104
        assert np.all(np.diff(calendar) == np.timedelta64(7, 'D'))
        assert calendar.size == 207
        row = out.setdefault(panel, dict(dates=[str(calendar[i]) for i in (0, 103, 104, -1)],
                                          frozen_test_start=str(dates[104])))
        x, r, floors = (data[key][select].astype(float) for key in ('X', 'R', 'floors'))
        for label, model in [('r6_source', original), ('r7', candidate)]:
            if label in row:
                continue
            gold.cr = model
            row[label] = gold.out_of_period_structure_test(
                panel, x, r, floors, int(data['K']), gold.PANELS[panel], n_seeds=20)
            path.write_text(json.dumps(out, indent=2))
            print(panel, label, row['dates'], flush=True)
            gc.collect()


if __name__ == '__main__':
    main()
