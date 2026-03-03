# from __future__ import annotations

# from pathlib import Path
# import re
# import pandas as pd
# import requests

# FCC_FIPS_URL = "https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt"


# def build_fips_tables(artifacts_dir: str | Path = "artifacts") -> tuple[Path, Path]:
#     artifacts_dir = Path(artifacts_dir)
#     outdir = artifacts_dir
#     outdir.mkdir(parents=True, exist_ok=True)

#     r = requests.get(FCC_FIPS_URL, timeout=60)
#     r.raise_for_status()

#     lines = r.text.splitlines()
#     data_lines = [line.strip() for line in lines if re.match(r"^\d", line.strip())]
#     parsed = [re.split(r"\s{2,}", line) for line in data_lines]

#     state_rows = [row for row in parsed if len(row[0]) == 2]
#     state_df = pd.DataFrame(state_rows, columns=["FIPS", "State Name"])
#     state_df["FIPS"] = state_df["FIPS"].astype(str).str.zfill(2)

#     county_rows = [row if len(row) == 2 else row[:2] for row in parsed if len(row[0]) == 5]
#     county_df = pd.DataFrame(county_rows, columns=["FIPS", "County Name"])
#     county_df["FIPS"] = county_df["FIPS"].astype(str).str.zfill(5)

#     state_out = outdir / "fips_states.parquet"
#     county_out = outdir / "fips_counties.parquet"
#     state_df.to_parquet(state_out, index=False)
#     county_df.to_parquet(county_out, index=False)
#     return state_out, county_out


# if __name__ == "__main__":
#     print(build_fips_tables())

from __future__ import annotations

from pathlib import Path
import hashlib
import re
from urllib.parse import urlparse

import pandas as pd
import requests

FCC_FIPS_URL = "https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt"

_STATE_RE = re.compile(r"^\d{2}$")
_COUNTY_RE = re.compile(r"^\d{5}$")


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def build_fips_tables(
    data_dir: str | Path = "data",
    *,
    force_download: bool = False,
    timeout: int = 60,
) -> tuple[Path, Path]:
    data_dir = Path(data_dir)
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    raw_name = Path(urlparse(FCC_FIPS_URL).path).name or "fips.txt"
    bronze_txt = bronze_dir / raw_name

    if force_download or not bronze_txt.exists():
        r = requests.get(FCC_FIPS_URL, timeout=timeout)
        r.raise_for_status()
        bronze_txt.write_text(r.text, encoding="utf-8")

    text = bronze_txt.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    data_lines = [line.strip() for line in lines if re.match(r"^\d", line.strip())]
    parsed = [re.split(r"\s{2,}", line) for line in data_lines]
    parsed = [row for row in parsed if len(row) >= 2]

    state_rows = [row[:2] for row in parsed if _STATE_RE.match(row[0].strip())]
    if not state_rows:
        raise ValueError("No state rows parsed from FCC FIPS text.")

    state_df = pd.DataFrame(state_rows, columns=["FIPS", "State Name"])
    state_df["FIPS"] = state_df["FIPS"].astype("string").str.zfill(2)
    state_df["State Name"] = state_df["State Name"].astype("string").str.strip()

    county_rows = [row[:2] for row in parsed if _COUNTY_RE.match(row[0].strip())]
    if not county_rows:
        raise ValueError("No county rows parsed from FCC FIPS text.")

    county_df = pd.DataFrame(county_rows, columns=["FIPS", "County Name"])
    county_df["FIPS"] = county_df["FIPS"].astype("string").str.zfill(5)
    county_df["County Name"] = county_df["County Name"].astype("string").str.strip()

    if state_df["FIPS"].duplicated().any():
        raise ValueError("Duplicate state FIPS detected.")
    if county_df["FIPS"].duplicated().any():
        raise ValueError("Duplicate county FIPS detected.")

    state_out = silver_dir / "fips_states.parquet"
    county_out = silver_dir / "fips_counties.parquet"
    state_df.to_parquet(state_out, index=False)
    county_df.to_parquet(county_out, index=False)

    meta = {
        "source_url": FCC_FIPS_URL,
        "bronze_txt": str(bronze_txt),
        "bronze_sha256": _sha256_file(bronze_txt),
        "rows_states": int(len(state_df)),
        "rows_counties": int(len(county_df)),
    }
    pd.Series(meta).to_json(silver_dir / "fips.meta.json")

    return state_out, county_out


if __name__ == "__main__":
    print(build_fips_tables())