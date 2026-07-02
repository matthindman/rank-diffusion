"""
rankdiff

Tools for preparing panel data, fitting rank-diffusion style models,
and running diagnostics.
"""

__version__ = "0.1.0"

from .types import Config
from .pipeline import run_pipeline

__all__ = [
    "Config",
    "run_pipeline",
]