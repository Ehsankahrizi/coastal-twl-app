#!/usr/bin/env python3
"""
NWM Coastal TWL Forecast Pipeline
==================================
Downloads SHEF forecast data from NOAA's National Water Model (NWM),
parses it, matches station metadata, and exports JSON files for the iOS app.

Runs automatically via GitHub Actions every 6 hours.

Outputs:
    data/stations.json   — Station metadata (id, name, lat, lon, etc.)
    data/twl_data.json   — TWL time-series grouped by station ID
    data/metadata.json   — Pipeline run metadata (timestamp, counts, etc.)
"""

import os
import sys
import json
import datetime as dt
import shutil
import subprocess
import tempfile

import pandas as pd
from google.cloud import storage
import requests
from io import StringIO

# ── Configuration ──────────────────────────────────────────────────────────
REGIONS = ["atlgulf"]  # Add "pacific" later if needed
CYCLES = ["00", "06", "12", "18"]
BUCKET_NAME = "national-water-model"
IEM_URL = "https://mesonet.agron.iastate.edu/sites/networks.php?format=csv&nohtml=&special=alldcp"

# Output directory (relative to repo root)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")


def get_today_str():
    """Get today's date string in YYYY-MM-DD format (UTC)."""
    return dt.datetime.utcnow().strftime("%Y-%m-%d")


def get_latest_available_date():
    """
    Try today first, then yesterday.
    NWM data may not be available immediately, so we check both.
    """
    today = dt.datetime.utcnow().date()
    for offset in [0, 1]:
        d = today - dt.timedelta(days=offset)
        yield d.strftime("%Y-%m-%d")


def download_twl_shef(date_str, dest_folder, region="atlgulf", cycle="00"):
    """Download one SHEF file from NOAA's GCS bucket."""
    d = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    ymd = d.strftime("%Y%m%d")
    prefix = f"nwm.{ymd}/"

    os.makedirs(dest_folder, exist_ok=True)

    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(BUCKET_NAME)

    target = f"nwm.t{cycle}z.short_range_coastal.total_water.{region}.shef"
    local_path = os.path.join(dest_folder, target)

    print(f"  Searching: {prefix} for {target}")
    for blob in client.list_blobs(bucket_or_name=bucket, prefix=prefix):
        if blob.name.endswith("/" + target) or blob.name.endswith(target):
            print(f"  Downloading: {blob.name}")
            blob.download_to_filename(local_path)
            return local_path

    return None  # Not found


def parse_shef(input_path, output_path):
    """Parse .shef file to whitespace-delimited .txt using shefParser."""
    exe = shutil.which("shefParser")
    if not exe:
        print("ERROR: shefParser not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "shef-parser"])
        exe = shutil.which("shefParser")
        if not exe:
            raise FileNotFoundError("shefParser still not found after install")

    res = subprocess.run(
        [exe, "-i", input_path, "-o", output_path, "-f", "1"],
        capture_output=True, text=True
    )

    if res.returncode != 0:
        print(f"  shefParser error: {res.stderr[:500]}")
        return False

    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        print("  shefParser produced empty output")
        return False

    return True


def load_and_process_shef(txt_path):
    """Load parsed SHEF text file into a DataFrame."""
    df = pd.read_csv(txt_path, sep=r'\s+', header=None)
    df.columns = [
        "station_id", "valid_date", "valid_time",
        "creation_date", "creation_time",
        "pe_code", "value", "tz", "duration",
        "qualifier", "revision", "product_id", "source"
    ]

    df["valid_time_utc"] = pd.to_datetime(
        df["valid_date"] + "T" + df["valid_time"] + "Z",
        utc=True, errors="coerce"
    )
    df["creation_time_utc"] = pd.to_datetime(
        df["creation_date"] + "T" + df["creation_time"] + "Z",
        utc=True, errors="coerce"
    )

    return df


def fetch_station_metadata():
    """Fetch station metadata from Iowa Environmental Mesonet."""
    print("Fetching station metadata from IEM...")
    try:
        response = requests.get(IEM_URL, verify=False, timeout=30)
        meta = pd.read_csv(StringIO(response.text))
        print(f"  Loaded {len(meta)} stations from IEM")
        return meta
    except Exception as e:
        print(f"  WARNING: Failed to fetch IEM metadata: {e}")
        # Try loading cached metadata
        cached = os.path.join(DATA_DIR, "stations_cache.json")
        if os.path.exists(cached):
            print("  Using cached station metadata")
            with open(cached) as f:
                return pd.DataFrame(json.load(f))
        return None


def build_json(df, meta):
    """Build stations.json and twl_data.json from DataFrame + metadata."""

    # Match stations
    station_ids = set(df["station_id"].unique())
    if meta is not None:
        matched = meta[meta["stid"].isin(station_ids)].copy()
    else:
        matched = pd.DataFrame()

    # stations.json
    stations = []
    for _, row in matched.iterrows():
        stations.append({
            "id": row["stid"],
            "name": str(row["station_name"]) if pd.notna(row.get("station_name")) else row["stid"],
            "latitude": round(float(row["lat"]), 6),
            "longitude": round(float(row["lon"]), 6),
            "elevation": round(float(row["elev"]), 2) if pd.notna(row.get("elev")) else None,
            "network": str(row["iem_network"]) if pd.notna(row.get("iem_network")) else None,
        })

    # twl_data.json (grouped by station)
    twl_data = {}
    for sid, grp in df.groupby("station_id"):
        readings = []
        for _, r in grp.iterrows():
            if pd.notna(r["valid_time_utc"]) and pd.notna(r["creation_time_utc"]):
                readings.append({
                    "validTime": r["valid_time_utc"].isoformat(),
                    "creationTime": r["creation_time_utc"].isoformat(),
                    "value": round(float(r["value"]), 4),
                    "peCode": r["pe_code"],
                })
        readings.sort(key=lambda x: x["validTime"])
        if readings:
            twl_data[sid] = readings

    return stations, twl_data


def main():
    print("=" * 60)
    print("NWM Coastal TWL Forecast Pipeline")
    print(f"Run time: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 60)

    os.makedirs(DATA_DIR, exist_ok=True)

    all_dfs = []
    successful_downloads = []

    with tempfile.TemporaryDirectory() as tmpdir:
        for region in REGIONS:
            for date_str in get_latest_available_date():
                # Try latest cycle first (descending)
                for cycle in reversed(CYCLES):
                    print(f"\nTrying: {date_str} / {region} / t{cycle}z")

                    shef_path = download_twl_shef(date_str, tmpdir, region, cycle)
                    if shef_path is None:
                        print(f"  Not found, trying next...")
                        continue

                    # Parse
                    txt_path = shef_path.replace(".shef", ".txt")
                    if not parse_shef(shef_path, txt_path):
                        print(f"  Parse failed, trying next...")
                        continue

                    # Load
                    df = load_and_process_shef(txt_path)
                    print(f"  Loaded {len(df)} records, {df['station_id'].nunique()} stations")
                    all_dfs.append(df)
                    successful_downloads.append({
                        "date": date_str,
                        "region": region,
                        "cycle": cycle,
                        "records": len(df),
                        "stations": int(df["station_id"].nunique())
                    })

                    # Use the first successful cycle for each date/region
                    break
                else:
                    continue
                break  # Found data for this region, stop trying dates

    if not all_dfs:
        print("\nERROR: No data could be downloaded!")
        sys.exit(1)

    # Combine all DataFrames
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal records: {len(combined_df)}")
    print(f"Total unique stations: {combined_df['station_id'].nunique()}")

    # Fetch metadata
    meta = fetch_station_metadata()

    # Build JSON
    stations, twl_data = build_json(combined_df, meta)

    # Write output files
    stations_path = os.path.join(DATA_DIR, "stations.json")
    with open(stations_path, "w") as f:
        json.dump(stations, f, indent=2)
    print(f"\nWrote {len(stations)} stations → {stations_path}")

    twl_path = os.path.join(DATA_DIR, "twl_data.json")
    with open(twl_path, "w") as f:
        json.dump(twl_data, f, indent=2)
    print(f"Wrote {len(twl_data)} station time-series → {twl_path}")

    # Write metadata
    run_metadata = {
        "lastUpdated": dt.datetime.utcnow().isoformat() + "Z",
        "stationsCount": len(stations),
        "stationsWithData": len(twl_data),
        "totalReadings": len(combined_df),
        "downloads": successful_downloads,
        "regions": REGIONS,
    }
    meta_path = os.path.join(DATA_DIR, "metadata.json")
    with open(meta_path, "w") as f:
        json.dump(run_metadata, f, indent=2)
    print(f"Wrote metadata → {meta_path}")

    print("\n✅ Pipeline completed successfully!")


if __name__ == "__main__":
    main()
