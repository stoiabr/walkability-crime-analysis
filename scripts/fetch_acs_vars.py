from __future__ import annotations

from pathlib import Path
import pandas as pd

ACS_VARS_URL = "https://api.census.gov/data/2023/acs/acs5/variables.html"


def build_acs_variables_catalog(artifacts_dir: str | Path = "artifacts") -> Path:
    artifacts_dir = Path(artifacts_dir)
    outdir = artifacts_dir / "raw"
    outdir.mkdir(parents=True, exist_ok=True)

    tables = pd.read_html(ACS_VARS_URL)
    df = tables[0]

    outpath = outdir / "acs5_2023_variables_catalog.parquet"
    df.to_parquet(outpath, index=False)
    return outpath


if __name__ == "__main__":
    print(build_acs_variables_catalog())