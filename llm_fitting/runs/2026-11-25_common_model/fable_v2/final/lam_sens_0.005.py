"""Sensitivity wrapper: the final module with JUMP_LAM_FIXED = 0.005 (diagnostic only)."""
import importlib.util, sys
from pathlib import Path
_spec = importlib.util.spec_from_file_location("cr_base_0_005", str(Path(__file__).resolve().parents[1] / "common_rankdiff_fable.py"))
_m = importlib.util.module_from_spec(_spec); sys.modules["cr_base_0_005"] = _m; _spec.loader.exec_module(_m)
_m.JUMP_LAM_FIXED = 0.005
globals().update({k: v for k, v in vars(_m).items() if not k.startswith("__")})
