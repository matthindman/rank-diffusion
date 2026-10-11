#!/usr/bin/env python3
"""
Build the ladder explainer page from summary.json (see ladder_explainer.py).

    python llm_fitting/ladder_explainer_page.py --out llm_fitting/runs/2026-10-12_ladder

Writes <out>/explainer.html (a build product; *.html is git-ignored here).
Every number on the page is read from
summary.json; the prose blocks live in <out>/explainer_text.json so that the
interpretation is a separate, reviewable file.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

HERE = Path(__file__).resolve().parent

STEP_COPY = {
    "R0": ("Rank diffusion", "Log activity is a random walk. Weekly volatility depends on the endpoint's permanent rank.",
           "x = h    h ← h + σ(z)·η"),
    "R1": ("+ noise", "Part of each weekly change does not carry over to the next week.",
           "x = h + ε"),
    "R2": ("+ home", "The lasting level is pulled back toward the endpoint's own long-run level.",
           "h ← h − κ(z)·(h − home) + σ(z)·η"),
    "R3": ("+ short-lived swings", "The passing part splits into pure noise and a swing that fades over a few weeks.",
           "x = h + ξ + ε    ξ ← φ·ξ + ν"),
    "R4": ("+ endpoint amplitude", "Some endpoints are persistently more volatile than others at the same rank.",
           "ξ, ε scaled by √vᵢ    ln vᵢ ~ N(−s²/2, s²)"),
    "R5": ("+ fat tails", "Swings occasionally arrive as large shocks.",
           "ν ~ Student-t(df)"),
    "S": ("+ long-horizon structure", "Full-panel refinements of the structure stack: 26- and 52-week moments, a stationary common level, a medium-length swing, amplitude on the lasting part.",
          "κ from D(26), D(52)    + ξ₂    h scaled by vᵢ^(b/2)"),
    "G": ("+ forecast from where each endpoint is", "The held-out forecast starts from each endpoint's filtered state at the end of training. Then one panel-specific step: noise pinned from daily data on Facebook, amplitude on the lasting part on comments.",
          "h(T₀) = filtered state    σ_obs from the daily floor"),
    "S1": ("+ long-horizon moments", "26- and 52-week change variances join the fit and identify the pull toward home.", ""),
    "S2": ("+ stationary common level", "The platform-wide level mean-reverts and no longer accumulates.", ""),
    "S3": ("+ medium component", "A second swing lasting one to three months.", ""),
    "S4": ("+ amplitude on the lasting part", "The endpoint amplitude also scales the lasting level's volatility.", ""),
    "G1": ("+ forecast from the actual state", "The forecast starts from each endpoint's filtered end-of-training state.", ""),
    "G2": ("+ panel-specific identification", "Facebook: noise pinned from the daily floor. Comments: amplitude on the lasting part.", ""),
}


def _clean(o):
    """JSON-safe copy: NaN and infinities become null."""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    return o


def build(out: Path) -> Path:
    summ = json.loads((out / "summary.json").read_text())
    text_path = out / "explainer_text.json"
    text = json.loads(text_path.read_text()) if text_path.exists() else {}
    data = dict(summary=_clean(summ), copy=STEP_COPY, text=text)
    blob = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    # .tmpl, not .html: the repository ignores *.html, and this file is source
    html = (HERE / "ladder_explainer_template.tmpl").read_text()
    html = html.replace("/*__DATA__*/null", blob)
    for key, val in text.items():
        if isinstance(val, str):
            html = html.replace("<!--@" + key + "@-->", val)
    dest = out / "explainer.html"
    dest.write_text(html)
    return dest


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    print(build(Path(a.out)))
