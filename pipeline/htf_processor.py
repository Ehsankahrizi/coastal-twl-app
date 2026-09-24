#!/usr/bin/env python3
"""
HTF Threshold Processor
========================
For each HTF threshold point, finds NWM stations within a configurable
radius, computes the mean TWL forecast from those neighbors, and outputs
JSON files that the iOS app can display.

Generates TWO output files per run:
  - data/nwm_htf_5km.json   → results using the 5 km search radius
  - data/nwm_htf_10km.json  → results using the 10 km search radius

Uses MHHW-converted TWL data (twl_data_mhhw.json) so that the forecast
values are in the same datum as the HTF thresholds.

The output pairs each HTF point with:
  - The averaged NWM time series (mean of neighbors) in feet above MHHW
  - The HTF MidThreshold value (horizontal threshold line) in feet above MHHW
  - List of matched NWM station IDs and distances

Units: the HTF thresholds (htf_threshold.json, from Mahmoudi et al. 2024,
Nat. Commun. 15:4251) are in METERS above MHHW, while the NWM forecasts are in
FEET. Thresholds are converted to feet before any comparison.

Datum: only NWM stations whose TWL was actually converted to MHHW
(datumStatus "OK" in twl_data_mhhw.json) are averaged. Unconverted stations
still carry NAVD88 values and would make the comparison meaningless, so they
are left out and listed under "excludedStations".

Runs after the main fetch_and_parse.py pipeline.
"""

import os
import sys
import json
import math

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from timezones import timezone_for  # noqa: E402 - needs sys.path set first

REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")

# Both search radii to generate output for
RADII_KM = [5.0, 10.0]

FT_PER_M = 3.280839895  # HTF thresholds are in meters; forecasts are in feet


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


def parse_range_m(text):
    """'(0.49, 0.52)' → (0.49, 0.52); None if it can't be parsed."""
    try:
        lo, hi = (float(x) for x in str(text).strip("() ").split(","))
        return lo, hi
    except (TypeError, ValueError):
        return None


def process_htf_for_radius(radius_km, htf_thresholds, station_coords, twl_data, excluded_coords):
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
        threshold_m = htf["HTF MidThreshold"]
        threshold_ft = threshold_m * FT_PER_M
        range_m = parse_range_m(htf.get("HTF Range"))

        # Find NWM stations within radius
        neighbors = []
        for sid, coords in station_coords.items():
            dist = haversine_km(htf_lat, htf_lon, coords["lat"], coords["lon"])
            if dist <= radius_km:
                neighbors.append({"id": sid, "distance_km": round(dist, 3)})

        # Stations in range that could not be converted to MHHW (reported only)
        excluded = [
            {"id": sid, "distance_km": round(d, 3)}
            for sid, coords in excluded_coords.items()
            if (d := haversine_km(htf_lat, htf_lon, coords["lat"], coords["lon"])) <= radius_km
        ]

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
            "htfMidThreshold": round(threshold_ft, 4),      # feet above MHHW, same unit as meanForecast
            "htfMidThresholdM": round(threshold_m, 6),     # original value, meters above MHHW
            "htfRange": htf.get("HTF Range", ""),          # original text, meters
            "htfRangeFt": [round(v * FT_PER_M, 4) for v in range_m] if range_m else None,
            "units": "ft",
            "datum": "MHHW",
            "radiusKm": radius_km,
            "matchedStations": sorted(neighbors, key=lambda x: x["distance_km"]),
            "excludedStations": sorted(excluded, key=lambda x: x["distance_km"]),
            "meanForecast": mean_series,
            "creationTime": creation_time,
            # Lets the detail chart label times in the point's own local zone
            # instead of UTC. Omitted when unresolvable.
            "timeZone": timezone_for(htf_lat, htf_lon),
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

    # Build station lookup: id -> {lat, lon}, split by whether the TWL was
    # converted to MHHW. Only converted stations are averaged.
    station_coords, excluded_coords = {}, {}
    for s in stations:
        readings = twl_data.get(s["id"])
        if not readings:
            continue
        coords = {"lat": s["latitude"], "lon": s["longitude"]}
        if all(r.get("datumStatus") == "OK" for r in readings):
            station_coords[s["id"]] = coords
        else:
            excluded_coords[s["id"]] = coords

    print(f"  Stations with coords + MHHW data: {len(station_coords)}")
    print(f"  Stations left out (no MHHW conversion): {len(excluded_coords)}")

    # Process each radius and write its output file
    for radius_km in RADII_KM:
        print(f"\n{'─' * 40}")
        print(f"  Processing radius: {radius_km} km")

        nwm_htf, matched_count, no_match_count = process_htf_for_radius(
            radius_km, htf_thresholds, station_coords, twl_data, excluded_coords
        )

        print(f"  HTF points with NWM neighbors: {matched_count}")
        print(f"  HTF points with no match:      {no_match_count}")

        # Full time-series output: nwm_htf_5km.json, nwm_htf_10km.json
        # Minified (no indent) — these are large and served to the app.
        output_filename = f"nwm_htf_{int(radius_km)}km.json"
        output_path = os.path.join(DATA_DIR, output_filename)
        with open(output_path, "w") as f:
            json.dump(nwm_htf, f, separators=(",", ":"))
        print(f"  Wrote {len(nwm_htf)} entries → {output_path}")

        # Compact marker-status output: htf_status_5km.json, htf_status_10km.json
        # Maps HTF id → "exceeds" | "below" for points that have a forecast.
        # Points with no nearby NWM station are simply absent (app treats
        # absence as "no data"). A few KB vs. the ~1 MB full file, so the map
        # can color markers without downloading any time series.
        status = {}
        for htf_id, entry in nwm_htf.items():
            threshold = entry["htfMidThreshold"]
            exceeds = any(p["value"] >= threshold for p in entry["meanForecast"])
            status[htf_id] = "exceeds" if exceeds else "below"

        status_filename = f"htf_status_{int(radius_km)}km.json"
        status_path = os.path.join(DATA_DIR, status_filename)
        with open(status_path, "w") as f:
            json.dump(status, f, separators=(",", ":"))
        print(f"  Wrote {len(status)} statuses → {status_path}")

    print("\n✅ HTF processing completed!")


if __name__ == "__main__":
    main()
