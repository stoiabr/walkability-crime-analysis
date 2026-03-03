# # compile_data.py

# from pathlib import Path

# import pandas as pd
# import geopandas as gpd
# import shapely


# def compile_data(artifacts_dir: Path) -> pd.DataFrame:
#     walk = pd.read_parquet(artifacts_dir / "epa_walkability_county.parquet")
#     acs = pd.read_parquet(artifacts_dir / "acs_county_2021.parquet")
#     shapes = pd.read_parquet(artifacts_dir / "us_counties_shapes.parquet")
#     countyfips = pd.read_parquet(artifacts_dir / "fips_counties.parquet")
#     statefips = pd.read_parquet(artifacts_dir / "fips_states.parquet")
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
#         gpd.sjoin(ag, shapes, how="left", predicate="within", lsuffix="agency", rsuffix="county")
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
#         )
#         .drop(columns=["state", "county", "year"], errors="ignore")
#     )

#     crime_by_county = (
#         merged.groupby(["STATE", "COUNTY"], as_index=False)[["Person", "Property", "Society"]].sum()
#     )

#     fin = wip.merge(
#         crime_by_county,
#         left_on=["STATEFP", "COUNTYFP"],
#         right_on=["STATE", "COUNTY"],
#         how="inner",
#     )

#     return fin


# def main() -> None:
#     artifacts_dir = (Path(__file__).resolve().parent / ".." / "artifacts").resolve()
#     out_path = artifacts_dir / "compiled_county_data.parquet"

#     fin = compile_data(artifacts_dir)

#     out_path.parent.mkdir(parents=True, exist_ok=True)
#     fin.to_parquet(out_path, index=False)


# if __name__ == "__main__":
#     main()

# compile_data.py

from pathlib import Path

import pandas as pd
import geopandas as gpd
import shapely


def compile_data(artifacts_dir: Path) -> pd.DataFrame:
    walk = pd.read_parquet(artifacts_dir / "epa_walkability_county.parquet")
    acs = pd.read_parquet(artifacts_dir / "acs_county_2021.parquet")
    shapes = pd.read_parquet(artifacts_dir / "us_counties_shapes.parquet")
    countyfips = pd.read_parquet(artifacts_dir / "fips_counties.parquet")  # noqa: F841
    statefips = pd.read_parquet(artifacts_dir / "fips_states.parquet")  # noqa: F841
    agencies = pd.read_parquet(artifacts_dir / "fbi_agencies.parquet")
    crime = pd.read_parquet(artifacts_dir / "crime_by_ori.parquet")

    shapes = gpd.GeoDataFrame(
        shapes,
        geometry=shapely.from_wkb(shapes["geometry"]),
        crs="EPSG:4326",
    )

    ag = agencies.dropna(subset=["latitude", "longitude"]).copy()
    ag = gpd.GeoDataFrame(
        ag,
        geometry=gpd.points_from_xy(ag["longitude"], ag["latitude"]),
        crs="EPSG:4326",
    )

    agc = (
        gpd.sjoin(
            ag,
            shapes,
            how="left",
            predicate="within",
            lsuffix="agency",
            rsuffix="county",
        )
        .drop(columns=["GEO_ID"], errors="ignore")
    )

    agc["ORI9"] = agc["ori"].astype(str).str.strip().str.upper()
    crime["ORI7"] = crime["ORI"].astype(str).str.strip().str.upper()
    crime["ORI9"] = crime["ORI7"].str.ljust(9, "0")

    merged = agc.merge(crime, how="inner", on="ORI9")

    wip = (
        walk.merge(
            acs,
            left_on=["STATEFP", "COUNTYFP", "GEO_ID"],
            right_on=["state", "county", "GEO_ID"],
            how="inner",
        )
        .drop(columns=["state", "county", "year"], errors="ignore")
    )

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

    return fin


def main() -> None:
    ROOT = Path(__file__).resolve().parents[2]
    artifacts_dir = ROOT / "artifacts"
    out_path = artifacts_dir / "compiled_county_data.parquet"

    fin = compile_data(artifacts_dir)
    fin.to_parquet(out_path, index=False)

    print(f"Wrote {len(fin):,} rows -> {out_path}")


if __name__ == "__main__":
    main()