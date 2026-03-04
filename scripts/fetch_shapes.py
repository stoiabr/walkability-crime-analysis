# fetch_shapes.py
"""Download US county GeoJSON and save as a clean parquet with WKB geometry."""
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import geopandas as gpd
import pandas as pd

from utils import ensure_dirs, require_cols

COUNTY_GEOJSON_URL = (
    "https://raw.githubusercontent.com/holtzy/"
    "The-Python-Graph-Gallery/master/static/data/US-counties.geojson"
)

STATES_TO_REMOVE = {"02", "15", "72"}  # AK, HI, PR


def run(data_dir: str | Path = "data", *, force: bool = False) -> Path:
    bronze, silver, _ = ensure_dirs(data_dir)

    geojson_name = Path(urlparse(COUNTY_GEOJSON_URL).path).name or "us-counties.geojson"
    bronze_geojson = bronze / geojson_name

    # --- bronze ---------------------------------------------------------
    if force or not bronze_geojson.exists():
        gdf_raw = gpd.read_file(COUNTY_GEOJSON_URL)
        gdf_raw.to_file(bronze_geojson, driver="GeoJSON")

    # --- silver ---------------------------------------------------------
    gdf = gpd.read_file(bronze_geojson)
    require_cols(gdf, {"STATE", "COUNTY"}, "county_shapes")

    gdf = gdf[~gdf["STATE"].astype(str).isin(STATES_TO_REMOVE)].copy()

    gdf["STATE"] = gdf["STATE"].astype(str).str.zfill(2)
    gdf["COUNTY"] = gdf["COUNTY"].astype(str).str.zfill(3)
    gdf["GEO_ID"] = gdf["STATE"] + gdf["COUNTY"]

    assert not gdf["geometry"].isna().any(), "Null geometries found"
    assert not gdf.duplicated(subset=["GEO_ID"]).any(), "Duplicate GEO_ID values"
    assert gdf["GEO_ID"].str.len().eq(5).all(), "Invalid GEO_ID length"

    out = silver / "us_counties_shapes.parquet"
    gdf.to_parquet(out, index=False)
    return out


if __name__ == "__main__":
    print(run())