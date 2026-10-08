"""Pytest bootstrap: make the src-layout package importable without install.

Allows running the package tests from any working directory, e.g.
`python -m pytest Python/rankdiff/tests -q` from the repo root.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
