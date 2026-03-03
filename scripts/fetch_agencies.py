# fetch_agencies.py

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import us

load_dotenv()

api_key = os.getenv("CDE_KEY")

abbr_to_state = {state.abbr: state.name for state in us.states.STATES_AND_TERRITORIES}
abbr_to_state["DC"] = "District of Columbia"


rows = []
print("Starting fetch...")
for abbr in abbr_to_state:
    print(f"Fetching {abbr}...", flush=True)
    url = f"https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/{abbr}"
    resp = requests.get(url, params={"API_KEY": api_key}, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    for county, agencies_list in data.items():
        for ag in agencies_list:
            if isinstance(ag, dict):
                rows.append({
                    "state": abbr,
                    "county": county,
                    **ag
                })
            else:
                rows.append({
                    "state": abbr,
                    "county": county,
                    "agency": ag
                })

agencies = pd.DataFrame(rows)
agencies = agencies[agencies["is_nibrs"] == 1].drop('agency', axis=1)

artifacts_path = Path("artifacts")
artifacts_path.mkdir(parents=True, exist_ok=True)

agencies.to_parquet(artifacts_path / "fbi_agencies.parquet", index=False)

print("Finished requests")
print("Rows collected:", len(rows))