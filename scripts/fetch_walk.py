# fetch_walk.py
"""Download EPA Smart Location walkability data and aggregate to county level."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from utils import ensure_dirs, require_cols, zfill_col
import ssl 
ssl._create_default_https_context = ssl._create_unverified_context
EPA_WALK_CSV_URL = (
    "http://edg.epa.gov/EPADataCommons/public/OA/"
    "EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv"
)

EPA_COLS = [
    "STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE",
    "CSA", "CSA_Name", "CBSA", "CBSA_Name", "CBSA_POP", "TotPop",
    "Ac_Total", "Ac_Land", "Ac_Water", "Ac_Unpr", "CountHU", "HH",
    "NatWalkInd", "Shape_Length", "Shape_Area",
]

_REQ = {"STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "NatWalkInd",
        "Ac_Total", "Ac_Land", "Ac_Water", "Ac_Unpr"}


def run(data_dir: str | Path = "data", *, force: bool = False) -> Path:
    bronze, silver, _ = ensure_dirs(data_dir)

    csv_name = Path(urlparse(EPA_WALK_CSV_URL).path).name or "epa_walkability.csv"
    bronze_csv = bronze / csv_name

    # --- bronze ---------------------------------------------------------
    if force or not bronze_csv.exists():
        raw = pd.read_csv(EPA_WALK_CSV_URL, usecols=EPA_COLS, low_memory=False)
        require_cols(raw, _REQ, "epa_walkability")
        raw.to_csv(bronze_csv, index=False)

    # --- silver ---------------------------------------------------------
    walk = pd.read_csv(bronze_csv, usecols=EPA_COLS, low_memory=False)
    require_cols(walk, _REQ, "epa_walkability")

    walk["STATEFP"] = zfill_col(walk["STATEFP"], 2)
    walk["COUNTYFP"] = zfill_col(walk["COUNTYFP"], 3)
    walk["TRACTCE"] = zfill_col(walk["TRACTCE"], 6)
    walk["BLKGRPCE"] = walk["BLKGRPCE"].astype("string")

    numeric_cols = ["NatWalkInd", "Ac_Total", "Ac_Land", "Ac_Unpr", "Ac_Water"]
    for c in numeric_cols:
        walk[c] = pd.to_numeric(walk[c], errors="coerce")

    before = len(walk)
    walk = walk.dropna(subset=["STATEFP", "COUNTYFP", "NatWalkInd"])
    if len(walk) == 0:
        raise ValueError("No rows left after dropping nulls.")
    if len(walk) < before * 0.5:
        raise ValueError(f"Unexpected row drop: {before} -> {len(walk)}.")

    county = (
        walk.groupby(["STATEFP", "COUNTYFP"], dropna=False)
        .agg(
            walkability_min=("NatWalkInd", "min"),
            walkability_max=("NatWalkInd", "max"),
            walkability_mean=("NatWalkInd", "mean"),
            walkability_median=("NatWalkInd", "median"),
            Ac_Total=("Ac_Total", "sum"),
            Ac_Land=("Ac_Land", "sum"),
            Ac_Unpr=("Ac_Unpr", "sum"),
            Ac_Water=("Ac_Water", "sum"),
        )
        .reset_index()
    )

    county["GEO_ID"] = county["STATEFP"] + county["COUNTYFP"]
    county["walkability_range"] = county["walkability_max"] - county["walkability_min"]

    assert not county["GEO_ID"].isna().any(), "GEO_ID has nulls"
    assert not county.duplicated(subset=["STATEFP", "COUNTYFP"]).any(), "Duplicate counties"
    assert (county["walkability_min"] <= county["walkability_max"]).all(), "min > max"

    out = silver / "epa_walkability_county.parquet"
    county.to_parquet(out, index=False)
    return out


if __name__ == "__main__":
    print(run())