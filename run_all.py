# run_all.py
"""Run the full data pipeline: bronze -> silver -> gold.

Usage:
    python run_all.py                  # only downloads what's missing
    python run_all.py --force          # re-downloads and rebuilds everything
    python run_all.py --crime-tsv path/to/file.tsv   # specify crime input file
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from scripts import fetch_walk, fetch_shapes, fetch_agencies, fetch_acs, fetch_acs_vars, fetch_fips, clean_crime, compile_data


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", default="data", help="Base data directory")
    p.add_argument("--force", action="store_true", help="Re-download / rebuild everything")
    p.add_argument("--crime-tsv", required=True, help="Path to raw FBI crime .tsv file")
    args = p.parse_args()

    data_dir = args.data_dir
    force = args.force

    steps = [
            ("EPA walkability",  lambda: fetch_walk.run(data_dir, force=force)),
            ("County shapes",    lambda: fetch_shapes.run(data_dir, force=force)),
            ("FBI agencies",     lambda: fetch_agencies.run(data_dir, force=force)),
            ("ACS variable catalog", lambda: fetch_acs_vars.run(data_dir, force=force)),
            ("FIPS lookup",          lambda: fetch_fips.run(data_dir, force=force)),
            ("ACS census data",  lambda: fetch_acs.run(data_dir, force=force)),
            ("Crime by ORI",     lambda: clean_crime.run(args.crime_tsv, data_dir, force=force)),
            ("Compile gold",     lambda: compile_data.run(data_dir)),
        ]

    for name, step in steps:
        print(f"\n{'='*50}")
        print(f"  {name}")
        print(f"{'='*50}")
        try:
            out = step()
            print(f"  -> {out}")
        except Exception as e:
            print(f"  FAILED: {e}", file=sys.stderr)
            sys.exit(1)

    print(f"\nDone! Gold dataset at: {data_dir}/gold/compiled_county_data.parquet")


if __name__ == "__main__":
    main()