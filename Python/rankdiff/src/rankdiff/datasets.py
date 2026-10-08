"""
Bundled example data.

The package ships a small synthetic toy panel (entity x week x metric) that
mirrors the expected input schema; see the README for the column contract.
"""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path


def toy_data_path() -> Path:
    """Return the filesystem path of the bundled toy panel parquet."""
    return Path(str(files("rankdiff").joinpath("data/toy_rank_data.parquet")))
