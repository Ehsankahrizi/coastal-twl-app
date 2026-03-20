#!/usr/bin/env python3
"""
HTF Threshold Processor
========================
For each HTF threshold point, finds NWM stations within a configurable
radius, computes the mean TWL forecast from those neighbors, and outputs
JSON files that the iOS app can display.

Generates TWO output files per run:
  - data/nwm_htf.json       → results using the 5 km search radius (default)
  - data/nwm_htf_10km.json  → results using the 10 km search radius

Uses MHHW-converted TWL data (twl_data_mhhw.json) so that the forecast
values are in the same datum as the HTF thresholds.

The output pairs each HTF point with:
  - The averaged NWM time series (mean of neighbors) in MHHW
  - The HTF MidThreshold value (horizontal threshold line) in MHHW
  - List of matched NWM station IDs and distances

Runs after the main fetch_and_parse.py pipeline.
"""

import os
import sys
import json
import math

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")

# Both search radii to generate output for
RADII_KM = [5.0, 10.0]


def haversine_km(lat1, lon1, lat2, lon2):
    """Compute great-circle distance between two points in km."""
    R = 6371.0  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"ERROR: {path} not found")
        return None
    with open(path) as f:
        return json.load(f)


def process_htf_for_radius(radius_km, htf_thresholds, station_coords, twl_data):
    """
    Process all HTF threshold points for a given search radius.

    Returns:
        nwm_htf (dict): keyed by HTF point ID string
        matched_count (int): number of HTF points that found NWM neighbors
        no_match_count (int): number of HTF points with no nearby stations
    """
    nwm_htf = {}
    matched_count = 0
    no_match_count = 0

    for htf in htf_thresholds:
        htf_id = str(htf["name"])
        htf_lat = htf["lat"]
        htf_lon = htf["lon"]
        threshold = htf["HTF MidThreshold"]

        # Find NWM stations within radius
        neighbors = []
        for sid, coords in station_coords.items():
            dist = haversine_km(htf_lat, htf_lon, coords["lat"], coords["lon"])
            if dist <= radius_km:
                neighbors.append({"id": sid, "distance_km": round(dist, 3)})

        if not neighbors:
            no_match_count += 1
            continue

        matched_count += 1

        # Collect all time series from neighbors
        # Build a time -> [values] mapping
        time_values = {}
        for nb in neighbors:
            readings = twl_data.get(nb["id"], [])
            for r in readings:
                t = r["validTime"]
                if t not in time_values:
                    time_values[t] = []
                time_values[t].append(r["value"])

        # Compute mean at each timestep
        mean_series = []
        for t in sorted(time_values.keys()):
            vals = time_values[t]
            mean_val = sum(vals) / len(vals)
            mean_series.append({
                "validTime": t,
                "value": round(mean_val, 4),
                "stationCount": len(vals),
            })

        # Get creation time from first neighbor's first reading
        creation_time = None
        for nb in neighbors:
            readings = twl_data.get(nb["id"], [])
            if readings and "creationTime" in readings[0]:
                creation_time = readings[0]["creationTime"]
                break

        nwm_htf[htf_id] = {
            "htfId": int(htf_id),
            "lat": htf_lat,
            "lon": htf_lon,
            "htfMidThreshold": round(threshold, 6),
            "htfRange": htf.get("HTF Range", ""),
            "datum": "MHHW",
            "radiusKm": radius_km,
            "matchedStations": sorted(neighbors, key=lambda x: x["distance_km"]),
            "meanForecast": mean_series,
            "creationTime": creation_time,
        }

    return nwm_htf, matched_count, no_match_count


def main():
    print("=" * 60)
    print("HTF Threshold Processor")
    print("=" * 60)

    # Load inputs — use MHHW-converted TWL data for consistency with HTF thresholds
    htf_thresholds = load_json("htf_threshold.json")
    stations = load_json("stations.json")
    twl_data = load_json("twl_data_mhhw.json")

    if not all([htf_thresholds, stations, twl_data]):
        print("ERROR: Missing required input files")
        sys.exit(1)

    print(f"  HTF threshold points: {len(htf_thresholds)}")
    print(f"  NWM stations: {len(stations)}")
    print(f"  Stations with TWL data (MHHW): {len(twl_data)}")

    # Build station lookup: id -> {lat, lon}
    station_coords = {}
    for s in stations:
        if s["id"] in twl_data:
            station_coords[s["id"]] = {
                "lat": s["latitude"],
                "lon": s["longitude"],
            }

    print(f"  Stations with coords + data: {len(station_coords)}")

    # Process each radius and write its output file
    for radius_km in RADII_KM:
        print(f"\n{'─' * 40}")
        print(f"  Processing radius: {radius_km} km")

        nwm_htf, matched_count, no_match_count = process_htf_for_radius(
            radius_km, htf_thresholds, station_coords, twl_data
        )

        print(f"  HTF points with NWM neighbors: {matched_count}")
        print(f"  HTF points with no match:      {no_match_count}")

        # Determine output filename:
        #   5 km  → nwm_htf.json        (original, iOS app default)
        #   10 km → nwm_htf_10km.json   (new)
        if radius_km == 5.0:
            output_filename = "nwm_htf.json"
        else:
            output_filename = f"nwm_htf_{int(radius_km)}km.json"

        output_path = os.path.join(DATA_DIR, output_filename)
        with open(output_path, "w") as f:
            json.dump(nwm_htf, f, indent=2)
        print(f"  Wrote {len(nwm_htf)} entries → {output_path}")

    print("\n✅ HTF processing completed!")


if __name__ == "__main__":
    main()
