from __future__ import annotations

from pathlib import Path
import os
import pandas as pd
from census import Census
from dotenv import load_dotenv


load_dotenv()  # loads .env into environment


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
    "B992701_001E": "health_insance_coverage",
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


def fetch_acs_data(
    year: int = 2023,
    artifacts_dir: str | Path = "artifacts",
) -> Path:
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    census_api_key = os.getenv("CENSUS_API_KEY")
    if not census_api_key:
        raise RuntimeError("CENSUS_API_KEY not found in environment")

    c = Census(census_api_key)

    data = c.acs5.state_county(
        list(ACS_VARS.keys()),
        state_fips="*",
        county_fips="*",
        year=year,
    )

    df = pd.DataFrame(data)
    df["year"] = year
    df["GEO_ID"] = (
        df["state"].astype(str).str.zfill(2)
        + df["county"].astype(str).str.zfill(3)
    )
    df.rename(columns=ACS_VARS, inplace=True)

    outpath = artifacts_dir / f"acs_county_{year}.parquet"
    df.to_parquet(outpath, index=False)
    return outpath


if __name__ == "__main__":
    print(fetch_acs_data())