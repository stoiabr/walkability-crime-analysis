# from __future__ import annotations

# from pathlib import Path
# import pandas as pd

# ACS_VARS_URL = "https://api.census.gov/data/2023/acs/acs5/variables.html"


# def build_acs_variables_catalog(artifacts_dir: str | Path = "artifacts") -> Path:
#     artifacts_dir = Path(artifacts_dir)
#     outdir = artifacts_dir / "raw"
#     outdir.mkdir(parents=True, exist_ok=True)

#     tables = pd.read_html(ACS_VARS_URL)
#     df = tables[0]

#     outpath = outdir / "acs5_2023_variables_catalog.parquet"
#     df.to_parquet(outpath, index=False)
#     return outpath


# if __name__ == "__main__":
#     print(build_acs_variables_catalog())

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
import hashlib
import pandas as pd
import requests

ACS_VARS_URL = "https://api.census.gov/data/2021/acs/acs5/variables.html"


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def build_acs_variables_catalog(
    data_dir: str | Path = "data",
    *,
    force_download: bool = False,
    timeout: int = 60,
) -> Path:
    data_dir = Path(data_dir)
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    html_name = Path(urlparse(ACS_VARS_URL).path).name or "acs5_variables.html"
    bronze_html = bronze_dir / html_name

    if force_download or not bronze_html.exists():
        r = requests.get(ACS_VARS_URL, timeout=timeout)
        r.raise_for_status()
        bronze_html.write_bytes(r.content)

    tables = pd.read_html(bronze_html)
    if not tables:
        raise ValueError("No tables found in ACS variables HTML.")

    df = tables[0]
    if df.empty:
        raise ValueError("ACS variables table is empty.")

    if df.columns.duplicated().any():
        raise ValueError("Duplicate columns detected in ACS variables table.")

    outpath = silver_dir / "acs5_2021_variables_catalog.parquet"
    df.to_parquet(outpath, index=False)

    meta = {
        "source_url": ACS_VARS_URL,
        "bronze_html": str(bronze_html),
        "bronze_sha256": _sha256_file(bronze_html),
        "rows_silver": int(len(df)),
        "columns_silver": list(map(str, df.columns)),
    }
    pd.Series(meta).to_json(silver_dir / "acs5_2021_variables_catalog.meta.json")

    return outpath


if __name__ == "__main__":
    print(build_acs_variables_catalog())