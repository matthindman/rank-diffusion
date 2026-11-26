"""Independent Gaussian-mixture plants and a surge coactivation stress test."""
import json
from pathlib import Path

import numpy as np
import common_rankdiff_astra_r7 as cr


from test_common_rankdiff_astra_r7 import _independent_jump_panel


def main():
    out = []
    for mode in ('null', 'rising', 'falling', 'both'):
        for seed in (431, 853, 1289):
            X, R, K = _independent_jump_panel(seed, mode)
            p = cr.estimate_common(X, R, K, refine_drift=False, innov_mode='gaussian', asymmetry=mode == 'both')
            j = p.diag['jumps']
            allocation = j['proposed_weekly_variance'] if j['active'] else [0.] * 7
            row = dict(plant=mode, seed=seed, active=j['active'], fraction=j['gap_share'],
                       head=allocation[0], body=allocation[-1], surge=p.surge_lam,
                       surge_scale=p.surge_mu, surge_half_life=-np.log(2)/np.log(p.surge_rho) if p.surge_rho else 0)
            out.append(row)
            print(row, flush=True)
    Path(__file__).with_suffix('.json').write_text(json.dumps(out, indent=2))


if __name__ == '__main__':
    main()
