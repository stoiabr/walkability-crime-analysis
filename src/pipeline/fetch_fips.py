from __future__ import annotations

from pathlib import Path
import re
import pandas as pd
import requests

FCC_FIPS_URL = "https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt"


def build_fips_tables(artifacts_dir: str | Path = "artifacts") -> tuple[Path, Path]:
    artifacts_dir = Path(artifacts_dir)
    outdir = artifacts_dir / "curated"
    outdir.mkdir(parents=True, exist_ok=True)

    r = requests.get(FCC_FIPS_URL, timeout=60)
    r.raise_for_status()

    lines = r.text.splitlines()
    data_lines = [line.strip() for line in lines if re.match(r"^\d", line.strip())]
    parsed = [re.split(r"\s{2,}", line) for line in data_lines]

    state_rows = [row for row in parsed if len(row[0]) == 2]
    state_df = pd.DataFrame(state_rows, columns=["FIPS", "State Name"])
    state_df["FIPS"] = state_df["FIPS"].astype(str).str.zfill(2)

    county_rows = [row if len(row) == 2 else row[:2] for row in parsed if len(row[0]) == 5]
    county_df = pd.DataFrame(county_rows, columns=["FIPS", "County Name"])
    county_df["FIPS"] = county_df["FIPS"].astype(str).str.zfill(5)

    state_out = outdir / "fips_states.parquet"
    county_out = outdir / "fips_counties.parquet"
    state_df.to_parquet(state_out, index=False)
    county_df.to_parquet(county_out, index=False)
    return state_out, county_out


if __name__ == "__main__":
    print(build_fips_tables())