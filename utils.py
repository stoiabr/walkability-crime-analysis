# utils.py
"""Shared helpers for data pipeline scripts."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


def ensure_dirs(data_dir: str | Path) -> tuple[Path, Path, Path]:
    """Create and return (bronze, silver, gold) directories under *data_dir*."""
    data_dir = Path(data_dir)
    bronze = data_dir / "bronze"
    silver = data_dir / "silver"
    gold = data_dir / "gold"
    for d in (bronze, silver, gold):
        d.mkdir(parents=True, exist_ok=True)
    return bronze, silver, gold


def require_cols(df: pd.DataFrame, cols: set[str], label: str = "DataFrame") -> None:
    """Raise if *df* is missing any of *cols*."""
    missing = sorted(cols - set(df.columns))
    if missing:
        raise ValueError(f"{label}: missing required columns: {missing}")


def zfill_col(series: pd.Series, width: int) -> pd.Series:
    """Zero-pad a Series of codes to a fixed *width*."""
    return series.astype("string").str.zfill(width)


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def get_env_key(var: str) -> str:
    """Load .env and return the value of *var*, or raise."""
    load_dotenv()
    key = (os.getenv(var) or "").strip()
    if not key:
        raise RuntimeError(f"Missing env variable: {var}")
    return key