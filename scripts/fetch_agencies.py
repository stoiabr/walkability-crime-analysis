# fetch_agencies.py
"""Fetch FBI CDE agency data by state and save NIBRS-participating agencies."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests
import us

from utils import ensure_dirs, get_env_key

BASE_URL = "https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr"


def _state_abbrs(include_territories: bool = True) -> list[str]:
    states = us.states.STATES_AND_TERRITORIES if include_territories else us.states.STATES
    return [s.abbr for s in states] + ["DC"]


def _fetch_state(abbr: str, api_key: str, timeout: int = 30) -> list[dict]:
    resp = requests.get(
        f"{BASE_URL}/{abbr}",
        params={"API_KEY": api_key},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()

    rows: list[dict] = []
    for county, agencies in data.items():
        for ag in agencies:
            row = {"state": abbr, "county": county}
            if isinstance(ag, dict):
                row.update(ag)
            else:
                row["agency"] = ag
            rows.append(row)
    return rows


def run(
    data_dir: str | Path = "data",
    *,
    force: bool = False,
    include_territories: bool = True,
    verbose: bool = True,
) -> Path:
    _, silver, _ = ensure_dirs(data_dir)
    out = silver / "fbi_agencies.parquet"

    if out.exists() and not force:
        return out

    api_key = get_env_key("CDE_KEY")
    abbrs = _state_abbrs(include_territories)

    rows: list[dict] = []
    for abbr in abbrs:
        if verbose:
            print(f"  {abbr}", end="", flush=True)
        rows.extend(_fetch_state(abbr, api_key))
    if verbose:
        print()

    df = pd.DataFrame(rows)

    if "is_nibrs" not in df.columns:
        raise KeyError("Expected column 'is_nibrs' not found")
    df = df[df["is_nibrs"] == 1].copy()
    df = df.drop(columns=["agency"], errors="ignore")

    df.to_parquet(out, index=False)
    return out


if __name__ == "__main__":
    print(run())