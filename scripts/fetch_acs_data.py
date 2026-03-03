# from __future__ import annotations

# from pathlib import Path
# import os
# import pandas as pd
# from census import Census
# from dotenv import load_dotenv


# load_dotenv()  # loads .env into environment


# ACS_VARS = {
#     "B01003_001E": "total_pop",
#     "B01001_002E": "total_male_pop",
#     "B01001_026E": "total_female_pop",
#     "B09001_001E": "total_pop_under_18",
#     "B01002_001E": "median_age",
#     "B25002_003E": "vacant_houses",
#     "B25001_001E": "housing_units",
#     "B19013_001E": "median_income",
#     "B25058_001E": "median_rent",
#     "B23025_005E": "unemployed",
#     "B25003_002E": "owner_occupied",
#     "B25003_003E": "renter_occupied",
#     "B08131_001E": "travel_time_to_work",
#     "B08301_001E": "means_of_transport",
#     "B992701_001E": "health_insance_coverage",
#     "B99172_001E": "poverty_status_for_families",
#     "B29003_001E": "poverty_status",
#     "B99281_001E": "household_internet_access",
#     "B15003_001E": "edu_total_pop_25plus",
#     "B15003_017E": "edu_high_school_grad",
#     "B15003_022E": "edu_bachelor_degree",
#     "B15003_023E": "edu_masters_degree",
#     "B15003_024E": "edu_professional_degree",
#     "B15003_025E": "edu_doctorate_degree",
# }


# def fetch_acs_data(
#     year: int = 2021,
#     artifacts_dir: str | Path = "artifacts",
# ) -> Path:
#     artifacts_dir = Path(artifacts_dir)
#     artifacts_dir.mkdir(parents=True, exist_ok=True)

#     census_api_key = os.getenv("ACS_KEY")
#     if not census_api_key:
#         raise RuntimeError("ACS_KEY not found in environment")

#     c = Census(census_api_key)

#     data = c.acs5.state_county(
#         list(ACS_VARS.keys()),
#         state_fips="*",
#         county_fips="*",
#         year=year,
#     )

#     df = pd.DataFrame(data)
#     df["year"] = year
#     df["GEO_ID"] = (
#         df["state"].astype(str).str.zfill(2)
#         + df["county"].astype(str).str.zfill(3)
#     )
#     df.rename(columns=ACS_VARS, inplace=True)

#     outpath = artifacts_dir / f"acs_county_{year}.parquet"
#     df.to_parquet(outpath, index=False)
#     return outpath


# if __name__ == "__main__":
#     print(fetch_acs_data())

from __future__ import annotations

import os
import time
import hashlib
from pathlib import Path
from typing import Any

import pandas as pd
from census import Census
from dotenv import load_dotenv

ENV_KEY_NAME = "ACS_KEY"

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


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_api_key() -> str:
    load_dotenv()
    key = (os.getenv(ENV_KEY_NAME) or "").strip()
    if not key:
        raise RuntimeError(f"{ENV_KEY_NAME} not found in environment or .env")
    return key


def fetch_acs_county(
    year: int = 2021,
    data_dir: str | Path = "data",
    *,
    force_download: bool = False,
    sleep_s: float = 0.0,
) -> Path:
    data_dir = Path(data_dir)
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    bronze_json = bronze_dir / f"acs5_county_{year}.json"
    silver_out = silver_dir / f"acs_county_{year}.parquet"

    if silver_out.exists() and not force_download:
        return silver_out

    api_key = _get_api_key()
    c = Census(api_key)

    data: list[dict[str, Any]] = c.acs5.state_county(
        list(ACS_VARS.keys()),
        state_fips="*",
        county_fips="*",
        year=year,
    )

    if not data:
        raise ValueError("ACS API returned no data.")

    raw_bytes = (pd.DataFrame(data)).to_json(orient="records").encode("utf-8")
    bronze_json.write_bytes(raw_bytes)

    df = pd.DataFrame(data)

    for col in ["state", "county"]:
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in ACS response.")

    df["year"] = year
    df["state"] = df["state"].astype("string").str.zfill(2)
    df["county"] = df["county"].astype("string").str.zfill(3)
    df["GEO_ID"] = df["state"] + df["county"]

    df.rename(columns=ACS_VARS, inplace=True)

    for v in ACS_VARS.values():
        if v in df.columns:
            df[v] = pd.to_numeric(df[v], errors="coerce")

    if df["GEO_ID"].isna().any():
        raise ValueError("Null GEO_ID found.")
    if df.duplicated(subset=["GEO_ID"]).any():
        raise ValueError("Duplicate GEO_ID found (expected 1 row per county).")

    df.to_parquet(silver_out, index=False)

    meta = {
        "source": "US Census ACS5 (via python-census)",
        "year": int(year),
        "bronze_json": str(bronze_json),
        "bronze_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "rows_silver": int(len(df)),
        "columns_silver": list(df.columns),
    }
    pd.Series(meta).to_json(silver_dir / f"acs_county_{year}.meta.json")

    if sleep_s:
        time.sleep(sleep_s)

    return silver_out


if __name__ == "__main__":
    print(fetch_acs_county())