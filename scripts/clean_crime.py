# clean_crime.py
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


CRIMEMAP = pd.DataFrame(
    [
        {"value": "011", "label": "Murder and Non-Negligent Manslaughter", "crime_against": "Person"},
        {"value": "012", "label": "Manslaughter by Negligence", "crime_against": "Person"},
        {"value": "020", "label": "Forcible Rape", "crime_against": "Person"},
        {"value": "030", "label": "Robbery", "crime_against": "Person"},
        {"value": "040", "label": "Aggravated Assault", "crime_against": "Person"},
        {"value": "050", "label": "Burglary - Breaking or Entering", "crime_against": "Property"},
        {"value": "060", "label": "Larceny - Theft (except motor vehicle)", "crime_against": "Property"},
        {"value": "070", "label": "Motor Vehicle Theft", "crime_against": "Property"},
        {"value": "080", "label": "Other Assaults", "crime_against": "Person"},
        {"value": "090", "label": "Arson", "crime_against": "Property"},
        {"value": "100", "label": "Forgery and Counterfeiting", "crime_against": "Property"},
        {"value": "110", "label": "Fraud", "crime_against": "Property"},
        {"value": "120", "label": "Embezzlement", "crime_against": "Property"},
        {"value": "130", "label": "Stolen property \"Buying, Receiving, Poss.\"", "crime_against": "Property"},
        {"value": "140", "label": "Vandalism", "crime_against": "Property"},
        {"value": "150", "label": "Weapons - Carrying, Possessing, etc.", "crime_against": "Society"},
        {"value": "160", "label": "Prostitution and Commercialized Vice Total", "crime_against": "Society"},
        {"value": "161", "label": "Prostitution and Commercialized Vice - Prostitution", "crime_against": "Society"},
        {"value": "162", "label": "Prostitution and Commercialized Vice - Assisting or Promoting Prostitution", "crime_against": "Society"},
        {"value": "163", "label": "Prostitution and Commercialized Vice - Purchasing Prostitution", "crime_against": "Society"},
        {"value": "170", "label": "Sex Offenses (except forcible rape and prostitution)", "crime_against": "Person"},
        {"value": "18", "label": "Drug Abuse Violations (Total)", "crime_against": "Society"},
        {"value": "180", "label": "Sale/Manufacturing (Subtotal)", "crime_against": "Society"},
        {"value": "181", "label": "Opium and Cocaine, and their derivatives (Morphine, Heroin)", "crime_against": "Society"},
        {"value": "182", "label": "Marijuana", "crime_against": "Society"},
        {"value": "183", "label": "Synthetic Narcotics - Manufactured Narcotics which can cause true drug addiction (Demerol, Methadones)", "crime_against": "Society"},
        {"value": "184", "label": "Other Dangerous Non-Narcotic Drugs (Barbiturates, Benzedrine)", "crime_against": "Society"},
        {"value": "185", "label": "Possession (Subtotal)", "crime_against": "Society"},
        {"value": "186", "label": "same as 181", "crime_against": "Society"},
        {"value": "187", "label": "same as 182", "crime_against": "Society"},
        {"value": "188", "label": "same as 183", "crime_against": "Society"},
        {"value": "189", "label": "same as 184", "crime_against": "Society"},
        {"value": "19", "label": "Gambling (Total)", "crime_against": "Society"},
        {"value": "191", "label": "Bookmaking (Horse and Sport Book)", "crime_against": "Society"},
        {"value": "192", "label": "Number and Lottery", "crime_against": "Society"},
        {"value": "193", "label": "All Other Gambling", "crime_against": "Society"},
        {"value": "200", "label": "Offenses Against Family and Children", "crime_against": "Person"},
        {"value": "210", "label": "Driving Under the Influence", "crime_against": "Society"},
        {"value": "220", "label": "Liquor Laws", "crime_against": "Society"},
        {"value": "230", "label": "Drunkenness", "crime_against": "Society"},
        {"value": "240", "label": "Disorderly Conduct", "crime_against": "Society"},
        {"value": "250", "label": "Vagrancy", "crime_against": "Society"},
        {"value": "260", "label": "All Other Offenses (except traffic)", "crime_against": "Society"},
        {"value": "270", "label": "Suspicion", "crime_against": "Society"},
        {"value": "280", "label": "Curfew and Loitering Law Violations", "crime_against": "Society"},
        {"value": "290", "label": "Runaways", "crime_against": "Society"},
        {"value": "301", "label": "Human Trafficking - Commercial Sex Acts (300)", "crime_against": "Person"},
        {"value": "302", "label": "Human Trafficking - Involuntary Servitude (310)", "crime_against": "Person"},
        {"value": "990", "label": "(99) is assigned to Juvenile Disposition data. If included should be zero.", "crime_against": "Society"},
        {"value": "998", "label": "Not applicable", "crime_against": None},
        {"value": "Total", "label": "Total", "crime_against": None},
    ]
)


def transform(tsv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        tsv_path,
        sep="\t",
        low_memory=False,
        usecols=range(77),
    )

    cols = df.columns[33:77]
    jcols = df.columns[22:31]

    df[cols] = (
        df[cols]
        .astype(str)
        .apply(lambda x: x.str.strip().str.replace(" ", "", regex=False))
        .replace({"": np.nan, "99999": np.nan, "99998": np.nan})
        .apply(pd.to_numeric, errors="coerce")
    )

    df["TOTAL"] = df[cols].sum(axis=1, skipna=True)

    df["OFFENSE"] = (
        df["OFFENSE"]
        .astype(str)
        .str.zfill(3)
        .replace({"018": "18", "019": "19"})
    )

    df = df.drop(columns=list(jcols) + list(cols))

    df = (
        df.groupby(["ORI", "OFFENSE"], as_index=False)["TOTAL"]
        .sum()
    )

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
    return df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Path to source .tsv")
    p.add_argument("--output", default="artifacts/ucr_by_ori.parquet", help="Parquet output path")
    args = p.parse_args()

    in_path = Path(args.input).expanduser().resolve()
    out_path = Path(args.output).expanduser()

    out_path.parent.mkdir(parents=True, exist_ok=True)

    result = transform(in_path)
    result.to_parquet(out_path, index=False)


if __name__ == "__main__":
    main()