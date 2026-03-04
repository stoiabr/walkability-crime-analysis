# # fetch_walk.py
# from __future__ import annotations

# from pathlib import Path
# from urllib.parse import urlparse
# import hashlib
# import pandas as pd

# EPA_WALK_CSV_URL = "http://edg.epa.gov/EPADataCommons/public/OA/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv"

# EPA_COLS = [
#     "STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE",
#     "CSA", "CSA_Name", "CBSA", "CBSA_Name", "CBSA_POP", "TotPop",
#     "Ac_Total", "Ac_Land", "Ac_Water", "Ac_Unpr", "CountHU", "HH",
#     "NatWalkInd", "Shape_Length", "Shape_Area",
# ]

# REQ_COLS = {"STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "NatWalkInd", "Ac_Total", "Ac_Land", "Ac_Water", "Ac_Unpr"}


# def zfill_column(series: pd.Series, width: int) -> pd.Series:
#     return series.astype("string").str.zfill(width)


# def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
#     h = hashlib.sha256()
#     with path.open("rb") as f:
#         for chunk in iter(lambda: f.read(chunk_size), b""):
#             h.update(chunk)
#     return h.hexdigest()


# def _assert_required_columns(df: pd.DataFrame) -> None:
#     missing = sorted(REQ_COLS.difference(df.columns))
#     if missing:
#         raise ValueError(f"Missing required columns: {missing}")


# def build_walkability_county(
#     data_dir: str | Path = "data",
#     *,
#     force_download: bool = False,
# ) -> Path:
#     data_dir = Path(data_dir)
#     bronze_dir = data_dir / "bronze"
#     silver_dir = data_dir / "silver"
#     bronze_dir.mkdir(parents=True, exist_ok=True)
#     silver_dir.mkdir(parents=True, exist_ok=True)

#     csv_name = Path(urlparse(EPA_WALK_CSV_URL).path).name or "epa_walkability.csv"
#     bronze_csv = bronze_dir / csv_name

#     if force_download or not bronze_csv.exists():
#         walk_raw = pd.read_csv(EPA_WALK_CSV_URL, usecols=EPA_COLS, low_memory=False)
#         _assert_required_columns(walk_raw)
#         walk_raw.to_csv(bronze_csv, index=False)

#     walk = pd.read_csv(bronze_csv, usecols=EPA_COLS, low_memory=False)
#     _assert_required_columns(walk)

#     walk["STATEFP"] = zfill_column(walk["STATEFP"], 2)
#     walk["COUNTYFP"] = zfill_column(walk["COUNTYFP"], 3)
#     walk["TRACTCE"] = zfill_column(walk["TRACTCE"], 6)
#     walk["BLKGRPCE"] = walk["BLKGRPCE"].astype("string")

#     for c in ["NatWalkInd", "Ac_Total", "Ac_Land", "Ac_Unpr", "Ac_Water"]:
#         walk[c] = pd.to_numeric(walk[c], errors="coerce")

#     before = len(walk)
#     walk = walk.dropna(subset=["STATEFP", "COUNTYFP", "NatWalkInd"])
#     if len(walk) == 0:
#         raise ValueError("No rows left after dropping null STATEFP/COUNTYFP/NatWalkInd.")
#     if len(walk) < before * 0.5:
#         raise ValueError(f"Unexpected row drop: {before} -> {len(walk)}.")

#     walk_county = (
#         walk.groupby(["STATEFP", "COUNTYFP"], dropna=False)
#         .agg(
#             walkability_min=("NatWalkInd", "min"),
#             walkability_max=("NatWalkInd", "max"),
#             walkability_mean=("NatWalkInd", "mean"),
#             walkability_median=("NatWalkInd", "median"),
#             Ac_Total=("Ac_Total", "sum"),
#             Ac_Land=("Ac_Land", "sum"),
#             Ac_Unpr=("Ac_Unpr", "sum"),
#             Ac_Water=("Ac_Water", "sum"),
#         )
#         .reset_index()
#     )

#     walk_county["GEO_ID"] = walk_county["STATEFP"] + walk_county["COUNTYFP"]
#     walk_county["walkability_range"] = walk_county["walkability_max"] - walk_county["walkability_min"]

#     if walk_county["GEO_ID"].isna().any():
#         raise ValueError("GEO_ID has nulls.")
#     if walk_county.duplicated(subset=["STATEFP", "COUNTYFP"]).any():
#         raise ValueError("Duplicate (STATEFP, COUNTYFP) detected after aggregation.")
#     if (walk_county["walkability_min"] > walk_county["walkability_max"]).any():
#         raise ValueError("walkability_min > walkability_max detected.")

#     outpath = silver_dir / "epa_walkability_county.parquet"
#     walk_county.to_parquet(outpath, index=False)

#     meta_path = outpath.with_suffix(".meta.json")
#     meta = {
#         "source_url": EPA_WALK_CSV_URL,
#         "bronze_csv": str(bronze_csv),
#         "bronze_sha256": _sha256_file(bronze_csv),
#         "rows_silver": int(len(walk_county)),
#         "columns_silver": list(walk_county.columns),
#     }
#     pd.Series(meta).to_json(meta_path)

#     return outpath


# if __name__ == "__main__":
#     out = build_walkability_county()
#     print(out)

"""Download EPA Smart Location walkability data and aggregate to county level."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from _utils import ensure_dirs, require_cols, zfill_col

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