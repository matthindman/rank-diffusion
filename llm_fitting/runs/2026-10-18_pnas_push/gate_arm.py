"""A5 of PLAN.md: Gate v1 at 20 reps and 2,000 draws for one arm of the 2x2 ablation on one panel.

    python llm_fitting/runs/2026-10-18_pnas_push/gate_arm.py <platform> <top_k> <lasting>
    lasting: home (A) | movinghome (RDM) | movinghome_nodrift (a) | homedrift (b)
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import rankdiff_rdm as rdm  # noqa: E402

platform, k, lasting = sys.argv[1], int(sys.argv[2]), sys.argv[3]
r = rdm.oos_rdm(platform, k, reps=20, boot=2000, lasting=lasting)
Path(__file__).with_name(f"gate_{lasting}__{platform}.json").write_text(json.dumps(r, indent=1, default=float))
