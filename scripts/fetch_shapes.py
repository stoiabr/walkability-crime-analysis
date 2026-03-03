# from __future__ import annotations

# from pathlib import Path
# import geopandas as gpd

# COUNTY_GEOJSON_URL = "https://raw.githubusercontent.com/holtzy/The-Python-Graph-Gallery/master/static/data/US-counties.geojson"
# STATES_TO_REMOVE = {"02", "15", "72"}  # AK, HI, PR


# def build_county_shapes(artifacts_dir: str | Path = "artifacts") -> Path:
#     artifacts_dir = Path(artifacts_dir)
#     outdir = artifacts_dir
#     outdir.mkdir(parents=True, exist_ok=True)

#     gdf = gpd.read_file(COUNTY_GEOJSON_URL)
#     gdf = gdf[~gdf["STATE"].isin(STATES_TO_REMOVE)].copy()

#     # Standardize join key
#     gdf["STATE"] = gdf["STATE"].astype(str).str.zfill(2)
#     gdf["COUNTY"] = gdf["COUNTY"].astype(str).str.zfill(3)
#     gdf["GEO_ID"] = gdf["STATE"] + gdf["COUNTY"]

#     outpath = outdir / "us_counties_shapes.parquet"
#     gdf.to_parquet(outpath, index=False)
#     return outpath


# if __name__ == "__main__":
#     print(build_county_shapes())

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
import hashlib
import pandas as pd
import geopandas as gpd

COUNTY_GEOJSON_URL = "https://raw.githubusercontent.com/holtzy/The-Python-Graph-Gallery/master/static/data/US-counties.geojson"
STATES_TO_REMOVE = {"02", "15", "72"}  # AK, HI, PR


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def build_county_shapes(
    data_dir: str | Path = "data",
    *,
    force_download: bool = False,
) -> Path:
    data_dir = Path(data_dir)
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    geojson_name = Path(urlparse(COUNTY_GEOJSON_URL).path).name or "us-counties.geojson"
    bronze_geojson = bronze_dir / geojson_name

    if force_download or not bronze_geojson.exists():
        gdf_raw = gpd.read_file(COUNTY_GEOJSON_URL)
        gdf_raw.to_file(bronze_geojson, driver="GeoJSON")

    gdf = gpd.read_file(bronze_geojson)

    for col in ["STATE", "COUNTY"]:
        if col not in gdf.columns:
            raise ValueError(f"Missing required column: {col}")

    gdf = gdf[~gdf["STATE"].astype(str).isin(STATES_TO_REMOVE)].copy()

    gdf["STATE"] = gdf["STATE"].astype(str).str.zfill(2)
    gdf["COUNTY"] = gdf["COUNTY"].astype(str).str.zfill(3)
    gdf["GEO_ID"] = gdf["STATE"] + gdf["COUNTY"]

    if gdf["geometry"].isna().any():
        raise ValueError("Null geometries found.")
    if gdf.duplicated(subset=["GEO_ID"]).any():
        raise ValueError("Duplicate GEO_ID values found.")
    if gdf["GEO_ID"].str.len().ne(5).any():
        raise ValueError("Invalid GEO_ID length detected (expected 5).")

    outpath = silver_dir / "us_counties_shapes.parquet"
    gdf.to_parquet(outpath, index=False)

    meta_path = outpath.with_suffix(".meta.json")
    meta = {
        "source_url": COUNTY_GEOJSON_URL,
        "bronze_geojson": str(bronze_geojson),
        "bronze_sha256": _sha256_file(bronze_geojson),
        "rows_silver": int(len(gdf)),
        "columns_silver": list(gdf.columns),
        "crs": str(gdf.crs),
    }
    pd.Series(meta).to_json(meta_path)

    return outpath


if __name__ == "__main__":
    print(build_county_shapes())