#!/usr/bin/env python3
"""Download NYC TLC taxi data from DataTalksClub releases for given years/months.

Usage example:
  python download_nyc_taxi.py --years 2019 2020 --out data/
"""
import argparse
import os
import subprocess
from pathlib import Path


def build_url(taxi: str, year: int, month: str) -> str:
    filename = f"{taxi}_tripdata_{year}-{month}.csv.gz"
    return f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{filename}", filename


def download_file(url: str, dest: Path, retries: int = 3) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["wget", "-q", "-O", str(dest), url]
    for attempt in range(1, retries + 1):
        try:
            print(f"Downloading (attempt {attempt}): {url}")
            rc = subprocess.call(cmd)
            if rc == 0:
                print(f"Saved: {dest}")
                return True
            else:
                print(f"wget failed with exit {rc}")
        except Exception as e:
            print("Download error:", e)
    print(f"Failed to download: {url}")
    return False


def main():
    parser = argparse.ArgumentParser(description="Download NYC TLC taxi data (DataTalksClub releases)")
    parser.add_argument("--years", type=int, nargs="+", default=[2019, 2020], help="Years to download")
    parser.add_argument("--months", nargs="+", default=[f"{i:02d}" for i in range(1, 13)], help="Months to download (MM)")
    parser.add_argument("--taxis", nargs="+", default=["yellow", "green"], help="Taxi types: yellow, green")
    parser.add_argument("--out", default="./nyc_taxi_data", help="Output directory")
    parser.add_argument("--skip-existing", action="store_true", help="Skip files that already exist")
    args = parser.parse_args()

    out_dir = Path(args.out)

    for taxi in args.taxis:
        for year in args.years:
            for month in args.months:
                url, filename = build_url(taxi, year, month)
                dest = out_dir / taxi / str(year) / filename
                if args.skip_existing and dest.exists():
                    print(f"Skipping existing: {dest}")
                    continue
                success = download_file(url, dest)
                if not success:
                    print(f"Warning: failed to download {filename}")


if __name__ == "__main__":
    main()
