from pathlib import Path
import pandas as pd


def compile_data(artifacts_dir: str | Path = "artifacts") -> Path:
    artifacts_dir = Path(artifacts_dir)

    walk = pd.read_parquet(artifacts_dir / "epa_walkability_county.parquet")
    acs = pd.read_parquet(artifacts_dir / "acs_county_2021.parquet")

    wip = walk.merge(acs, left_on=['STATEFP', 'COUNTYFP'], right_on=['state', 'county'])
    #fin = wip.drop(['state', 'county'], axis=1).merge(nibrs_crimes_against, left_on=['STATEFP', 'COUNTYFP'], right_on=['STATE', 'COUNTY']).drop(columns=['STATE', 'COUNTY'])

    outdir = artifacts_dir / "model"
    outdir.mkdir(parents=True, exist_ok=True)

    outpath = outdir / "county_model_table.parquet"
    wip.to_parquet(outpath, index=False)

    return outpath


if __name__ == "__main__":
    print(compile_data())