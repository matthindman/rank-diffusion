"""G2 of DOSSIER.md: model A's dynamics (fixed home) in the same harness as the RDM gate, 20 reps and 2,000 draws,
for A's per-moment errors (the recorded 20-rep reproductions give only the aggregate).

    python llm_fitting/runs/2026-10-18_rdm/gate_runs_A.py <platform> <top_k>
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import rankdiff_rdm as rdm  # noqa: E402

platform, k = sys.argv[1], int(sys.argv[2])
r = rdm.oos_rdm(platform, k, reps=20, boot=2000, lasting="home")
Path(__file__).with_name(f"gate_A__{platform}.json").write_text(json.dumps(r, indent=1, default=float))
