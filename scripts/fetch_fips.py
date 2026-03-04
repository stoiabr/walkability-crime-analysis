# fetch_fips.py
"""Build a county FIPS lookup table with state and county names.

State names come from the `us` package. County names come from the Census
Bureau's national county FIPS file.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests
import us

from utils import ensure_dirs

COUNTY_FIPS_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"


def run(
    data_dir: str | Path = "data",
    *,
    force: bool = False,
) -> Path:
    bronze, silver, _ = ensure_dirs(data_dir)
    out = silver / "fips_lookup.parquet"

    if out.exists() and not force:
        return out

    # --- state names from `us` package ----------------------------------
    state_rows = []
    for s in us.states.STATES_AND_TERRITORIES:
        state_rows.append({"STATEFP": s.fips, "state_abbr": s.abbr, "state_name": s.name})
    state_rows.append({"STATEFP": "11", "state_abbr": "DC", "state_name": "District of Columbia"})
    states_df = pd.DataFrame(state_rows)

    # --- county names from Census file ----------------------------------
    bronze_csv = bronze / "national_county2020.txt"

    if force or not bronze_csv.exists():
        r = requests.get(COUNTY_FIPS_URL, timeout=60)
        r.raise_for_status()
        bronze_csv.write_bytes(r.content)

    counties = pd.read_csv(
        bronze_csv,
        dtype=str,
        sep="|",
        header=0,
    )
    counties = counties.rename(columns={
        "STATEFP": "STATEFP",
        "COUNTYFP": "COUNTYFP",
        "COUNTYNAME": "county_name",
    })
    counties = counties[["STATEFP", "COUNTYFP", "county_name"]].copy()
    counties["STATEFP"] = counties["STATEFP"].str.zfill(2)
    counties["COUNTYFP"] = counties["COUNTYFP"].str.zfill(3)
    counties["GEO_ID"] = counties["STATEFP"] + counties["COUNTYFP"]

    # --- merge state names onto counties --------------------------------
    fips = counties.merge(states_df, on="STATEFP", how="left")

    fips.to_parquet(out, index=False)
    return out


if __name__ == "__main__":
    print(run())