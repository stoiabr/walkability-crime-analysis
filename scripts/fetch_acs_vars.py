# fetch_acs_vars.py
"""Download the ACS 5-year variable catalog and save as parquet."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

from utils import ensure_dirs

ACS_VARS_URL = "https://api.census.gov/data/2021/acs/acs5/variables.html"


def run(
    data_dir: str | Path = "data",
    *,
    force: bool = False,
    timeout: int = 60,
) -> Path:
    bronze, silver, _ = ensure_dirs(data_dir)
    out = silver / "acs5_2021_variables_catalog.parquet"

    if out.exists() and not force:
        return out

    # --- bronze ---------------------------------------------------------
    html_name = Path(urlparse(ACS_VARS_URL).path).name or "acs5_variables.html"
    bronze_html = bronze / html_name

    if force or not bronze_html.exists():
        r = requests.get(ACS_VARS_URL, timeout=timeout)
        r.raise_for_status()
        bronze_html.write_bytes(r.content)

    # --- silver ---------------------------------------------------------
    tables = pd.read_html(bronze_html)
    if not tables:
        raise ValueError("No tables found in ACS variables HTML.")

    df = tables[0]
    if df.empty:
        raise ValueError("ACS variables table is empty.")
    if df.columns.duplicated().any():
        raise ValueError("Duplicate columns in ACS variables table.")

    df.to_parquet(out, index=False)
    return out


if __name__ == "__main__":
    print(run())