from __future__ import annotations

from pathlib import Path
import geopandas as gpd

COUNTY_GEOJSON_URL = "https://raw.githubusercontent.com/holtzy/The-Python-Graph-Gallery/master/static/data/US-counties.geojson"
STATES_TO_REMOVE = {"02", "15", "72"}  # AK, HI, PR


def build_county_shapes(artifacts_dir: str | Path = "artifacts") -> Path:
    artifacts_dir = Path(artifacts_dir)
    outdir = artifacts_dir
    outdir.mkdir(parents=True, exist_ok=True)

    gdf = gpd.read_file(COUNTY_GEOJSON_URL)
    gdf = gdf[~gdf["STATE"].isin(STATES_TO_REMOVE)].copy()

    # Standardize join key
    gdf["STATE"] = gdf["STATE"].astype(str).str.zfill(2)
    gdf["COUNTY"] = gdf["COUNTY"].astype(str).str.zfill(3)
    gdf["GEO_ID"] = gdf["STATE"] + gdf["COUNTY"]

    outpath = outdir / "us_counties_shapes.parquet"
    gdf.to_parquet(outpath, index=False)
    return outpath


if __name__ == "__main__":
    print(build_county_shapes())