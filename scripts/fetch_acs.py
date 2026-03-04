# fetch_acs.py
"""Fetch ACS 5-year county-level data from the Census API."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from census import Census

from utils import ensure_dirs, get_env_key, zfill_col

ACS_VARS = {
    "B01003_001E": "total_pop",
    "B01001_002E": "total_male_pop",
    "B01001_026E": "total_female_pop",
    "B09001_001E": "total_pop_under_18",
    "B01002_001E": "median_age",
    "B25002_003E": "vacant_houses",
    "B25001_001E": "housing_units",
    "B19013_001E": "median_income",
    "B25058_001E": "median_rent",
    "B23025_005E": "unemployed",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    "B08131_001E": "travel_time_to_work",
    "B08301_001E": "means_of_transport",
    "B992701_001E": "health_insurance_coverage",
    "B99172_001E": "poverty_status_for_families",
    "B29003_001E": "poverty_status",
    "B99281_001E": "household_internet_access",
    "B15003_001E": "edu_total_pop_25plus",
    "B15003_017E": "edu_high_school_grad",
    "B15003_022E": "edu_bachelor_degree",
    "B15003_023E": "edu_masters_degree",
    "B15003_024E": "edu_professional_degree",
    "B15003_025E": "edu_doctorate_degree",
}


def run(
    data_dir: str | Path = "data",
    *,
    force: bool = False,
    year: int = 2021,
) -> Path:
    bronze, silver, _ = ensure_dirs(data_dir)
    out = silver / f"acs_county_{year}.parquet"

    if out.exists() and not force:
        return out

    # --- bronze ---------------------------------------------------------
    api_key = get_env_key("ACS_KEY")
    c = Census(api_key)

    data: list[dict[str, Any]] = c.acs5.state_county(
        list(ACS_VARS.keys()),
        state_fips="*",
        county_fips="*",
        year=year,
    )
    if not data:
        raise ValueError("ACS API returned no data.")

    bronze_json = bronze / f"acs5_county_{year}.json"
    pd.DataFrame(data).to_json(bronze_json, orient="records")

    # --- silver ---------------------------------------------------------
    df = pd.DataFrame(data)

    for col in ("state", "county"):
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in ACS response.")

    df["year"] = year
    df["state"] = zfill_col(df["state"], 2)
    df["county"] = zfill_col(df["county"], 3)
    df["GEO_ID"] = df["state"] + df["county"]

    df.rename(columns=ACS_VARS, inplace=True)
    for v in ACS_VARS.values():
        if v in df.columns:
            df[v] = pd.to_numeric(df[v], errors="coerce")

    assert not df["GEO_ID"].isna().any(), "Null GEO_ID"
    assert not df.duplicated(subset=["GEO_ID"]).any(), "Duplicate GEO_ID"

    df.to_parquet(out, index=False)
    return out


if __name__ == "__main__":
    print(run())