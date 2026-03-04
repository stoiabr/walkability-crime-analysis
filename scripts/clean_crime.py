# clean_crime.py
"""Transform raw FBI crime TSV into per-ORI crime totals by category."""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from utils import ensure_dirs, require_cols

# Maps FBI offense codes -> human labels + broad category
CRIMEMAP = pd.DataFrame([
    {"value": "011", "label": "Murder and Non-Negligent Manslaughter",            "crime_against": "Person"},
    {"value": "012", "label": "Manslaughter by Negligence",                       "crime_against": "Person"},
    {"value": "020", "label": "Forcible Rape",                                    "crime_against": "Person"},
    {"value": "030", "label": "Robbery",                                          "crime_against": "Person"},
    {"value": "040", "label": "Aggravated Assault",                               "crime_against": "Person"},
    {"value": "050", "label": "Burglary - Breaking or Entering",                  "crime_against": "Property"},
    {"value": "060", "label": "Larceny - Theft (except motor vehicle)",            "crime_against": "Property"},
    {"value": "070", "label": "Motor Vehicle Theft",                              "crime_against": "Property"},
    {"value": "080", "label": "Other Assaults",                                   "crime_against": "Person"},
    {"value": "090", "label": "Arson",                                            "crime_against": "Property"},
    {"value": "100", "label": "Forgery and Counterfeiting",                       "crime_against": "Property"},
    {"value": "110", "label": "Fraud",                                            "crime_against": "Property"},
    {"value": "120", "label": "Embezzlement",                                     "crime_against": "Property"},
    {"value": "130", "label": 'Stolen property "Buying, Receiving, Poss."',       "crime_against": "Property"},
    {"value": "140", "label": "Vandalism",                                        "crime_against": "Property"},
    {"value": "150", "label": "Weapons - Carrying, Possessing, etc.",             "crime_against": "Society"},
    {"value": "160", "label": "Prostitution and Commercialized Vice Total",       "crime_against": "Society"},
    {"value": "161", "label": "Prostitution",                                     "crime_against": "Society"},
    {"value": "162", "label": "Assisting or Promoting Prostitution",              "crime_against": "Society"},
    {"value": "163", "label": "Purchasing Prostitution",                          "crime_against": "Society"},
    {"value": "170", "label": "Sex Offenses (except rape & prostitution)",        "crime_against": "Person"},
    {"value": "18",  "label": "Drug Abuse Violations (Total)",                    "crime_against": "Society"},
    {"value": "180", "label": "Sale/Manufacturing (Subtotal)",                    "crime_against": "Society"},
    {"value": "181", "label": "Opium/Cocaine derivatives",                        "crime_against": "Society"},
    {"value": "182", "label": "Marijuana",                                        "crime_against": "Society"},
    {"value": "183", "label": "Synthetic Narcotics",                              "crime_against": "Society"},
    {"value": "184", "label": "Other Dangerous Non-Narcotic Drugs",               "crime_against": "Society"},
    {"value": "185", "label": "Possession (Subtotal)",                            "crime_against": "Society"},
    {"value": "186", "label": "same as 181",                                      "crime_against": "Society"},
    {"value": "187", "label": "same as 182",                                      "crime_against": "Society"},
    {"value": "188", "label": "same as 183",                                      "crime_against": "Society"},
    {"value": "189", "label": "same as 184",                                      "crime_against": "Society"},
    {"value": "19",  "label": "Gambling (Total)",                                 "crime_against": "Society"},
    {"value": "191", "label": "Bookmaking (Horse and Sport Book)",                "crime_against": "Society"},
    {"value": "192", "label": "Number and Lottery",                               "crime_against": "Society"},
    {"value": "193", "label": "All Other Gambling",                               "crime_against": "Society"},
    {"value": "200", "label": "Offenses Against Family and Children",             "crime_against": "Person"},
    {"value": "210", "label": "Driving Under the Influence",                      "crime_against": "Society"},
    {"value": "220", "label": "Liquor Laws",                                      "crime_against": "Society"},
    {"value": "230", "label": "Drunkenness",                                      "crime_against": "Society"},
    {"value": "240", "label": "Disorderly Conduct",                               "crime_against": "Society"},
    {"value": "250", "label": "Vagrancy",                                         "crime_against": "Society"},
    {"value": "260", "label": "All Other Offenses (except traffic)",              "crime_against": "Society"},
    {"value": "270", "label": "Suspicion",                                        "crime_against": "Society"},
    {"value": "280", "label": "Curfew and Loitering Law Violations",              "crime_against": "Society"},
    {"value": "290", "label": "Runaways",                                         "crime_against": "Society"},
    {"value": "301", "label": "Human Trafficking - Commercial Sex Acts",          "crime_against": "Person"},
    {"value": "302", "label": "Human Trafficking - Involuntary Servitude",        "crime_against": "Person"},
    {"value": "990", "label": "Juvenile Disposition (should be zero)",            "crime_against": "Society"},
    {"value": "998", "label": "Not applicable",                                   "crime_against": None},
    {"value": "Total", "label": "Total",                                          "crime_against": None},
])


def _transform(tsv_path: Path) -> pd.DataFrame:
    """Read raw TSV, aggregate monthly counts, pivot by crime category."""
    df = pd.read_csv(tsv_path, sep="\t", low_memory=False, usecols=range(77))
    require_cols(df, {"ORI", "OFFENSE"}, "crime_tsv")

    month_cols = df.columns[33:77]
    juv_cols = df.columns[22:31]

    # Clean monthly count columns
    df[month_cols] = (
        df[month_cols]
        .astype(str)
        .apply(lambda x: x.str.strip().str.replace(" ", "", regex=False))
        .replace({"": np.nan, "99999": np.nan, "99998": np.nan})
        .apply(pd.to_numeric, errors="coerce")
    )
    df["TOTAL"] = df[month_cols].sum(axis=1, skipna=True)

    df["OFFENSE"] = (
        df["OFFENSE"].astype(str).str.zfill(3)
        .replace({"018": "18", "019": "19"})
    )

    df = df.drop(columns=list(juv_cols) + list(month_cols), errors="ignore")
    df = df.groupby(["ORI", "OFFENSE"], as_index=False)["TOTAL"].sum()

    df = (
        df.merge(CRIMEMAP, left_on="OFFENSE", right_on="value", how="left")
        .pivot_table(
            index="ORI",
            columns="crime_against",
            values="TOTAL",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    df.columns.name = None

    assert not df["ORI"].isna().any(), "Null ORI after transform"
    assert not df.duplicated(subset=["ORI"]).any(), "Duplicate ORI after pivot"

    for c in ("Person", "Property", "Society"):
        if c not in df.columns:
            df[c] = 0

    return df[["ORI", "Person", "Property", "Society"]]


def run(
    input_tsv: str | Path,
    data_dir: str | Path = "data",
    *,
    force: bool = False,
) -> Path:
    bronze, silver, _ = ensure_dirs(data_dir)

    in_path = Path(input_tsv).expanduser().resolve()
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    bronze_tsv = bronze / in_path.name
    out = silver / "crime_by_ori.parquet"

    if not force and out.exists():
        return out

    if force or not bronze_tsv.exists():
        shutil.copy2(in_path, bronze_tsv)

    result = _transform(bronze_tsv)
    result.to_parquet(out, index=False)
    return out


def _cli() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Path to source .tsv")
    p.add_argument("--data-dir", default="data")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()
    print(run(args.input, args.data_dir, force=args.force))


if __name__ == "__main__":
    _cli()