"""Gate v1 for the frozen moving-home RDM at recommendation grade (20 reps, 2,000 bootstrap draws).

    python llm_fitting/runs/2026-10-18_rdm/gate_runs.py <platform> <top_k>
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import rankdiff_rdm as rdm  # noqa: E402

platform, k = sys.argv[1], int(sys.argv[2])
r = rdm.oos_rdm(platform, k, reps=20, boot=2000)
Path(__file__).with_name(f"gate__{platform}.json").write_text(json.dumps(r, indent=1, default=float))
