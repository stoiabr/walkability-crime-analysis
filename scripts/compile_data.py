# # # compile_data.py

# # from pathlib import Path

# # import pandas as pd
# # import geopandas as gpd
# # import shapely


# # def compile_data(artifacts_dir: Path) -> pd.DataFrame:
# #     walk = pd.read_parquet(artifacts_dir / "epa_walkability_county.parquet")
# #     acs = pd.read_parquet(artifacts_dir / "acs_county_2021.parquet")
# #     shapes = pd.read_parquet(artifacts_dir / "us_counties_shapes.parquet")
# #     countyfips = pd.read_parquet(artifacts_dir / "fips_counties.parquet")
# #     statefips = pd.read_parquet(artifacts_dir / "fips_states.parquet")
# #     agencies = pd.read_parquet(artifacts_dir / "fbi_agencies.parquet")
# #     crime = pd.read_parquet(artifacts_dir / "crime_by_ori.parquet")

# #     shapes = gpd.GeoDataFrame(
# #         shapes,
# #         geometry=shapely.from_wkb(shapes["geometry"]),
# #         crs="EPSG:4326",
# #     )

# #     ag = agencies.dropna(subset=["latitude", "longitude"]).copy()
# #     ag = gpd.GeoDataFrame(
# #         ag,
# #         geometry=gpd.points_from_xy(ag["longitude"], ag["latitude"]),
# #         crs="EPSG:4326",
# #     )

# #     agc = (
# #         gpd.sjoin(ag, shapes, how="left", predicate="within", lsuffix="agency", rsuffix="county")
# #         .drop(columns=["GEO_ID"], errors="ignore")
# #     )

# #     agc["ORI9"] = agc["ori"].astype(str).str.strip().str.upper()

# #     crime["ORI7"] = crime["ORI"].astype(str).str.strip().str.upper()
# #     crime["ORI9"] = crime["ORI7"].str.ljust(9, "0")

# #     merged = agc.merge(crime, how="inner", on="ORI9")

# #     wip = (
# #         walk.merge(
# #             acs,
# #             left_on=["STATEFP", "COUNTYFP", "GEO_ID"],
# #             right_on=["state", "county", "GEO_ID"],
# #         )
# #         .drop(columns=["state", "county", "year"], errors="ignore")
# #     )

# #     crime_by_county = (
# #         merged.groupby(["STATE", "COUNTY"], as_index=False)[["Person", "Property", "Society"]].sum()
# #     )

# #     fin = wip.merge(
# #         crime_by_county,
# #         left_on=["STATEFP", "COUNTYFP"],
# #         right_on=["STATE", "COUNTY"],
# #         how="inner",
# #     )

# #     return fin


# # def main() -> None:
# #     artifacts_dir = (Path(__file__).resolve().parent / ".." / "artifacts").resolve()
# #     out_path = artifacts_dir / "compiled_county_data.parquet"

# #     fin = compile_data(artifacts_dir)

# #     out_path.parent.mkdir(parents=True, exist_ok=True)
# #     fin.to_parquet(out_path, index=False)


# # if __name__ == "__main__":
# #     main()

# # compile_data.py

# from pathlib import Path

# import pandas as pd
# import geopandas as gpd
# import shapely


# def compile_data(artifacts_dir: Path) -> pd.DataFrame:
#     walk = pd.read_parquet(artifacts_dir / "epa_walkability_county.parquet")
#     acs = pd.read_parquet(artifacts_dir / "acs_county_2021.parquet")
#     shapes = pd.read_parquet(artifacts_dir / "us_counties_shapes.parquet")
#     countyfips = pd.read_parquet(artifacts_dir / "fips_counties.parquet")  # noqa: F841
#     statefips = pd.read_parquet(artifacts_dir / "fips_states.parquet")  # noqa: F841
#     agencies = pd.read_parquet(artifacts_dir / "fbi_agencies.parquet")
#     crime = pd.read_parquet(artifacts_dir / "crime_by_ori.parquet")

#     shapes = gpd.GeoDataFrame(
#         shapes,
#         geometry=shapely.from_wkb(shapes["geometry"]),
#         crs="EPSG:4326",
#     )

#     ag = agencies.dropna(subset=["latitude", "longitude"]).copy()
#     ag = gpd.GeoDataFrame(
#         ag,
#         geometry=gpd.points_from_xy(ag["longitude"], ag["latitude"]),
#         crs="EPSG:4326",
#     )

#     agc = (
#         gpd.sjoin(
#             ag,
#             shapes,
#             how="left",
#             predicate="within",
#             lsuffix="agency",
#             rsuffix="county",
#         )
#         .drop(columns=["GEO_ID"], errors="ignore")
#     )

#     agc["ORI9"] = agc["ori"].astype(str).str.strip().str.upper()
#     crime["ORI7"] = crime["ORI"].astype(str).str.strip().str.upper()
#     crime["ORI9"] = crime["ORI7"].str.ljust(9, "0")

#     merged = agc.merge(crime, how="inner", on="ORI9")

#     wip = (
#         walk.merge(
#             acs,
#             left_on=["STATEFP", "COUNTYFP", "GEO_ID"],
#             right_on=["state", "county", "GEO_ID"],
#             how="inner",
#         )
#         .drop(columns=["state", "county", "year"], errors="ignore")
#     )

#     crime_by_county = (
#         merged.groupby(["STATE", "COUNTY"], as_index=False)[["Person", "Property", "Society"]]
#         .sum()
#     )

#     fin = wip.merge(
#         crime_by_county,
#         left_on=["STATEFP", "COUNTYFP"],
#         right_on=["STATE", "COUNTY"],
#         how="inner",
#     )

#     return fin


# def main() -> None:
#     ROOT = Path(__file__).resolve().parents[2]
#     artifacts_dir = ROOT / "artifacts"
#     out_path = artifacts_dir / "compiled_county_data.parquet"

#     fin = compile_data(artifacts_dir)
#     fin.to_parquet(out_path, index=False)

#     print(f"Wrote {len(fin):,} rows -> {out_path}")


# if __name__ == "__main__":
#     main()

# compile_data.py
from __future__ import annotations

from pathlib import Path
import json
import hashlib

import pandas as pd
import geopandas as gpd
import shapely


REQ_WALK = {"STATEFP", "COUNTYFP", "GEO_ID"}
REQ_ACS = {"state", "county", "GEO_ID"}
REQ_SHAPES = {"geometry", "GEO_ID", "STATE", "COUNTY"}
REQ_AGENCIES = {"ori", "latitude", "longitude"}
REQ_CRIME = {"ORI", "Person", "Property", "Society"}


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _require_cols(df: pd.DataFrame, cols: set[str], name: str) -> None:
    missing = sorted(cols.difference(df.columns))
    if missing:
        raise ValueError(f"{name}: missing required columns: {missing}")


def compile_data(data_dir: Path) -> pd.DataFrame:
    bronze = data_dir / "bronze"
    silver = data_dir / "silver"
    gold = data_dir / "gold"
    silver.mkdir(parents=True, exist_ok=True)
    gold.mkdir(parents=True, exist_ok=True)

    walk = pd.read_parquet(silver / "epa_walkability_county.parquet")
    acs = pd.read_parquet(silver / "acs_county_2021.parquet")
    shapes = pd.read_parquet(silver / "us_counties_shapes.parquet")
    agencies = pd.read_parquet(silver / "fbi_agencies.parquet")
    crime = pd.read_parquet(silver / "crime_by_ori.parquet")

    _require_cols(walk, REQ_WALK, "walk")
    _require_cols(acs, REQ_ACS, "acs")
    _require_cols(shapes, REQ_SHAPES, "shapes")
    _require_cols(agencies, REQ_AGENCIES, "agencies")
    _require_cols(crime, REQ_CRIME, "crime")

    shapes = gpd.GeoDataFrame(
        shapes,
        geometry=shapely.from_wkb(shapes["geometry"]),
        crs="EPSG:4326",
    )
    if shapes["geometry"].isna().any():
        raise ValueError("shapes: null geometries found")

    ag = agencies.dropna(subset=["latitude", "longitude"])
    if ag.empty:
        raise ValueError("agencies: no rows with lat/long")

    ag = gpd.GeoDataFrame(
        ag,
        geometry=gpd.points_from_xy(ag["longitude"], ag["latitude"]),
        crs="EPSG:4326",
    )

    agc = gpd.sjoin(
        ag,
        shapes[["GEO_ID", "STATE", "COUNTY", "geometry"]],
        how="left",
        predicate="within",
        lsuffix="agency",
        rsuffix="county",
    )

    # if agc["GEO_ID"].isna().any():
    #     raise ValueError("spatial join produced null GEO_IDs (some agencies not within any county polygon)")

    agc["ORI9"] = agc["ori"].astype("string").str.strip().str.upper()
    crime["ORI7"] = crime["ORI"].astype("string").str.strip().str.upper()
    crime["ORI9"] = crime["ORI7"].str.ljust(9, "0")

    merged = agc.merge(crime, how="inner", on="ORI9")
    if merged.empty:
        raise ValueError("No rows after merging agencies with crime on ORI9")

    wip = (
        walk.merge(
            acs,
            left_on=["STATEFP", "COUNTYFP", "GEO_ID"],
            right_on=["state", "county", "GEO_ID"],
            how="inner",
        )
        .drop(columns=["state", "county", "year"], errors="ignore")
    )
    if wip.empty:
        raise ValueError("No rows after merging walkability with ACS")

    crime_by_county = (
        merged.groupby(["STATE", "COUNTY"], as_index=False)[["Person", "Property", "Society"]]
        .sum()
    )

    fin = wip.merge(
        crime_by_county,
        left_on=["STATEFP", "COUNTYFP"],
        right_on=["STATE", "COUNTY"],
        how="inner",
    )
    if fin.empty:
        raise ValueError("No rows after merging county crime totals into wip")

    if fin.duplicated(subset=["STATEFP", "COUNTYFP", "GEO_ID"]).any():
        raise ValueError("Duplicate county keys found in final dataset")

    return fin


def main() -> None:
    data_dir =  Path("data")

    out_path = data_dir / "gold" / "compiled_county_data.parquet"
    fin = compile_data(data_dir)
    fin.to_parquet(out_path, index=False)

    meta = {
        "inputs": {
            "walk": str((data_dir / "silver" / "epa_walkability_county.parquet")),
            "acs": str((data_dir / "silver" / "acs_county_2021.parquet")),
            "shapes": str((data_dir / "silver" / "us_counties_shapes.parquet")),
            "agencies": str((data_dir / "silver" / "fbi_agencies.parquet")),
            "crime": str((data_dir / "silver" / "crime_by_ori.parquet")),
        },
        "rows_gold": int(len(fin)),
        "columns_gold": list(fin.columns),
    }
    (out_path.with_suffix(".meta.json")).write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"Wrote {len(fin):,} rows -> {out_path}")


if __name__ == "__main__":
    main()