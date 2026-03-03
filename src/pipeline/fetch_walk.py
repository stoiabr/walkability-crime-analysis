from __future__ import annotations

from pathlib import Path
import pandas as pd

EPA_WALK_CSV_URL = "http://edg.epa.gov/EPADataCommons/public/OA/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv"

EPA_COLS = [
    "STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE",
    "CSA", "CSA_Name", "CBSA", "CBSA_Name", "CBSA_POP", "TotPop",
    "Ac_Total", "Ac_Land", "Ac_Water", "Ac_Unpr", "CountHU", "HH",
    "NatWalkInd", "Shape_Length", "Shape_Area",
]


def zfill_column(series: pd.Series, width: int) -> pd.Series:
    return series.astype(str).str.zfill(width)


def build_walkability_county(
    artifacts_dir: str | Path = "artifacts",
) -> Path:
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    walk = pd.read_csv(EPA_WALK_CSV_URL, usecols=EPA_COLS)

    walk["STATEFP"] = zfill_column(walk["STATEFP"], 2)
    walk["COUNTYFP"] = zfill_column(walk["COUNTYFP"], 3)
    walk["TRACTCE"] = zfill_column(walk["TRACTCE"], 6)
    walk["BLKGRPCE"] = walk["BLKGRPCE"].astype(str)

    walk_county = (
        walk.groupby(["STATEFP", "COUNTYFP"])
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

    walk_county["GEO_ID"] = walk_county["STATEFP"] + walk_county["COUNTYFP"]
    walk_county["walkability_range"] = walk_county["walkability_max"] - walk_county["walkability_min"]

    outpath = artifacts_dir / "epa_walkability_county.parquet"
    walk_county.to_parquet(outpath, index=False)
    return outpath


if __name__ == "__main__":
    out = build_walkability_county()
    print(out)