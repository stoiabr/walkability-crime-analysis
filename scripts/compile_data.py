# compile_data.py
"""Join all silver-layer datasets into a single gold-layer county parquet."""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely

from utils import ensure_dirs, require_cols


### note: ensure caution when using an unverified context
### if errors while trying to query, uncomment the following section: 
# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context


def run(data_dir: str | Path = "data") -> Path:
    _, silver, gold = ensure_dirs(data_dir)

    # --- load silver layers ---------------------------------------------
    walk = pd.read_parquet(silver / "epa_walkability_county.parquet")
    acs = pd.read_parquet(silver / "acs_county_2021.parquet")
    shapes = pd.read_parquet(silver / "us_counties_shapes.parquet")
    agencies = pd.read_parquet(silver / "fbi_agencies.parquet")
    crime = pd.read_parquet(silver / "crime_by_ori.parquet")
    fips = pd.read_parquet(silver / "fips_lookup.parquet")

    require_cols(walk, {"STATEFP", "COUNTYFP", "GEO_ID"}, "walk")
    require_cols(acs, {"state", "county", "GEO_ID"}, "acs")
    require_cols(shapes, {"geometry", "GEO_ID", "STATE", "COUNTY"}, "shapes")
    require_cols(agencies, {"ori", "latitude", "longitude"}, "agencies")
    require_cols(crime, {"ORI", "Person", "Property", "Society"}, "crime")
    require_cols(fips, {"GEO_ID", "state_name", "state_abbr", "county_name"}, "fips")

    # --- spatial join: agencies -> counties -----------------------------
    shapes_gdf = gpd.GeoDataFrame(
        shapes,
        geometry=shapely.from_wkb(shapes["geometry"]),
        crs="EPSG:4326",
    )
    assert not shapes_gdf["geometry"].isna().any(), "Null geometries in shapes"

    ag = agencies.dropna(subset=["latitude", "longitude"])
    if ag.empty:
        raise ValueError("No agencies with lat/long")

    ag = gpd.GeoDataFrame(
        ag,
        geometry=gpd.points_from_xy(ag["longitude"], ag["latitude"]),
        crs="EPSG:4326",
    )

    agc = gpd.sjoin(
        ag,
        shapes_gdf[["GEO_ID", "STATE", "COUNTY", "geometry"]],
        how="left",
        predicate="within",
        lsuffix="agency",
        rsuffix="county",
    )

    # --- merge crime onto agencies by ORI -------------------------------
    agc["ORI9"] = agc["ori"].astype("string").str.strip().str.upper()
    crime["ORI7"] = crime["ORI"].astype("string").str.strip().str.upper()
    crime["ORI9"] = crime["ORI7"].str.ljust(9, "0")

    merged = agc.merge(crime, how="inner", on="ORI9").merge(
        fips[["GEO_ID", "state_name", "state_abbr", "county_name"]],
        on="GEO_ID",
        how="left",
    )
    if merged.empty:
        raise ValueError("No rows after agency–crime merge")

    crime_county = (
        merged.groupby(["STATE", "COUNTY"], as_index=False)
        [["Person", "Property", "Society"]].sum()
    )

    # --- walk + acs -----------------------------------------------------
    wip = walk.merge(
        acs,
        left_on=["STATEFP", "COUNTYFP", "GEO_ID"],
        right_on=["state", "county", "GEO_ID"],
        how="inner",
    ).drop(columns=["state", "county", "year"], errors="ignore")

    if wip.empty:
        raise ValueError("No rows after walk–acs merge")

    # --- final join -----------------------------------------------------
    fin = wip.merge(
        crime_county,
        left_on=["STATEFP", "COUNTYFP"],
        right_on=["STATE", "COUNTY"],
        how="inner",
    ).drop(columns=["STATE", "COUNTY", "year"], errors="ignore")

    if fin.empty:
        raise ValueError("No rows in final dataset")
    assert not fin.duplicated(subset=["STATEFP", "COUNTYFP", "GEO_ID"]).any(), \
        "Duplicate county keys in final dataset"

    out = gold / "compiled_county_data.parquet"
    fin.to_parquet(out, index=False)
    print(f"Wrote {len(fin):,} rows -> {out}")
    return out


if __name__ == "__main__":
    run()