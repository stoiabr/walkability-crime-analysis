# # fetch_agencies.py

# import os
# import requests
# import pandas as pd
# from dotenv import load_dotenv
# from pathlib import Path
# import us

# load_dotenv()

# api_key = os.getenv("CDE_KEY")

# abbr_to_state = {state.abbr: state.name for state in us.states.STATES_AND_TERRITORIES}
# abbr_to_state["DC"] = "District of Columbia"


# rows = []
# print("Starting fetch...")
# for abbr in abbr_to_state:
#     print(f"Fetching {abbr}...", flush=True)
#     url = f"https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/{abbr}"
#     resp = requests.get(url, params={"API_KEY": api_key}, timeout=30)
#     resp.raise_for_status()
#     data = resp.json()

#     for county, agencies_list in data.items():
#         for ag in agencies_list:
#             if isinstance(ag, dict):
#                 rows.append({
#                     "state": abbr,
#                     "county": county,
#                     **ag
#                 })
#             else:
#                 rows.append({
#                     "state": abbr,
#                     "county": county,
#                     "agency": ag
#                 })

# agencies = pd.DataFrame(rows)
# agencies = agencies[agencies["is_nibrs"] == 1].drop('agency', axis=1)

# artifacts_path = Path("artifacts")
# artifacts_path.mkdir(parents=True, exist_ok=True)

# agencies.to_parquet(artifacts_path / "fbi_agencies.parquet", index=False)

# print("Finished requests")
# print("Rows collected:", len(rows))

# fetch_agencies.py

import os
from pathlib import Path

import pandas as pd
import requests
import us
from dotenv import load_dotenv


BASE_URL = "https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr"


def get_api_key(env_var: str = "CDE_KEY") -> str:
    load_dotenv()
    key = os.getenv(env_var)
    if not key:
        raise RuntimeError(f"Missing API key: set {env_var} in your environment/.env")
    return key


def get_state_abbrs(include_territories: bool = True) -> dict[str, str]:
    states = us.states.STATES_AND_TERRITORIES if include_territories else us.states.STATES
    abbr_to_state = {s.abbr: s.name for s in states}
    abbr_to_state["DC"] = "District of Columbia"
    return abbr_to_state


def fetch_state_agencies(abbr: str, api_key: str, timeout: int = 30) -> list[dict]:
    url = f"{BASE_URL}/{abbr}"
    resp = requests.get(url, params={"API_KEY": api_key}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    rows: list[dict] = []
    for county, agencies_list in data.items():
        for ag in agencies_list:
            if isinstance(ag, dict):
                rows.append({"state": abbr, "county": county, **ag})
            else:
                rows.append({"state": abbr, "county": county, "agency": ag})
    return rows


def fetch_all_agencies(
    api_key: str,
    abbrs: list[str] | None = None,
    include_territories: bool = True,
    timeout: int = 30,
    verbose: bool = True,
) -> pd.DataFrame:
    if abbrs is None:
        abbrs = list(get_state_abbrs(include_territories=include_territories).keys())

    rows: list[dict] = []
    if verbose:
        print("Starting fetch...")

    for abbr in abbrs:
        if verbose:
            print(f"Fetching {abbr}...", flush=True)
        rows.extend(fetch_state_agencies(abbr, api_key=api_key, timeout=timeout))

    df = pd.DataFrame(rows)
    return df


def filter_nibrs(df: pd.DataFrame) -> pd.DataFrame:
    if "is_nibrs" not in df.columns:
        raise KeyError("Expected column 'is_nibrs' not found in agencies data")
    out = df[df["is_nibrs"] == 1].copy()
    if "agency" in out.columns:
        out = out.drop(columns=["agency"])
    return out


def save_parquet(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def main() -> None:
    api_key = get_api_key("CDE_KEY")
    agencies = fetch_all_agencies(api_key=api_key, include_territories=True, verbose=True)
    agencies = filter_nibrs(agencies)

    save_parquet(agencies, Path("data") / "silver" / "fbi_agencies.parquet")

    print("Finished requests")
    print("Rows collected:", len(agencies))


if __name__ == "__main__":
    main()