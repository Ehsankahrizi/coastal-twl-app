#!/usr/bin/env python3
"""
NAVD88 → MHHW Datum Converter (VDatum API)
============================================
Converts NWM Total Water Level values from NAVD88 to MHHW using NOAA's
VDatum API (https://vdatum.noaa.gov/docs/services.html).

VDatum performs spatially-interpolated vertical datum transformations at
any lat/lon — no need to find a nearby tide station. This is more accurate
than the CO-OPS station-matching approach, especially for sites far from
tide gauges.

Strategy:
    For each station location, call VDatum once with s_z=0 to obtain the
    offset between NAVD88 and MHHW at that point. Then apply the offset
    to all TWL readings for that station:

        offset = VDatum(lat, lon, z=0, from=NAVD88, to=MHHW)  → t_z
        value_MHHW = value_NAVD88 + offset

    (When s_z=0 in NAVD88 is converted to MHHW, VDatum returns the shift
    as t_z. For any other z: t_z_new = z + offset. So value_MHHW = value + offset.)

Offsets are cached in data/datum_offsets.json so the API is only called
once per station across all pipeline runs.

Fallback:
    If VDatum fails for a station (e.g., outside coverage), the converter
    falls back to NOAA CO-OPS Metadata API to find the nearest tide station
    and compute the offset from its published datums.

VDatum API Reference:
    Endpoint:  https://vdatum.noaa.gov/vdatumweb/api/convert
    Docs:      https://vdatum.noaa.gov/docs/services.html

    Request parameters:
        s_x          Source longitude (decimal degrees, west negative)
        s_y          Source latitude (decimal degrees)
        s_z          Source height value
        s_h_frame    Source horizontal reference frame (NAD83_2011)
        s_v_frame    Source vertical reference frame (NAVD88)
        s_v_unit     Source vertical unit (us_ft or m)
        t_v_frame    Target vertical reference frame (MHHW)
        t_v_unit     Target vertical unit (us_ft or m)
        region       Region for tidal grids (auto = auto-detect)

    Response JSON fields:
        t_x          Transformed longitude
        t_y          Transformed latitude
        t_z          Transformed height (the converted value)
        message      Error/status message (if any)
"""

import json
import math
import os
import time

import requests

# ══════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════

# VDatum API (primary)
VDATUM_URL = "https://vdatum.noaa.gov/vdatumweb/api/convert"
VDATUM_DELAY = 0.3  # seconds between VDatum API calls (be polite)

# CO-OPS Metadata API (fallback)
COOPS_BASE = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi"
COOPS_STATIONS_URL = f"{COOPS_BASE}/stations.json"
COOPS_DATUMS_URL = f"{COOPS_BASE}/stations/{{station_id}}/datums.json"
COOPS_DELAY = 0.25
MAX_DISTANCE_KM = 50  # fallback: max km to nearest CO-OPS tide station

METERS_TO_FEET = 3.28084


# ══════════════════════════════════════════════════════════════════════════
# VDatum API (Primary Method)
# ══════════════════════════════════════════════════════════════════════════

def _vdatum_get_offset(lat, lon, input_units="feet"):
    """
    Call the VDatum API to get the NAVD88 → MHHW offset at a given lat/lon.

    Sends s_z=0 in NAVD88 and reads back t_z in MHHW. The returned t_z IS
    the offset to add to any NAVD88 value to get the MHHW equivalent.

    Parameters
    ----------
    lat : float
        Latitude (decimal degrees, positive north)
    lon : float
        Longitude (decimal degrees, negative west)
    input_units : str
        "feet" or "meters" — determines VDatum unit parameters.

    Returns
    -------
    offset : float or None
        The offset in the requested units. value_MHHW = value_NAVD88 + offset.
        Returns None if VDatum cannot compute the conversion.
    """
    v_unit = "us_ft" if input_units == "feet" else "m"

    params = {
        "s_x": lon,
        "s_y": lat,
        "s_z": 0.0,               # zero height in NAVD88
        "s_h_frame": "NAD83_2011",
        "s_v_frame": "NAVD88",
        "s_v_unit": v_unit,
        "t_v_frame": "MHHW",
        "t_v_unit": v_unit,
        "region": "auto",          # let VDatum auto-detect region
    }

    try:
        resp = requests.get(VDATUM_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        print(f"    [VDatum] Request error: {e}")
        return None

    # Check for errors in response
    message = data.get("message", "")
    if message and "error" in message.lower():
        print(f"    [VDatum] Error: {message}")
        return None

    # Extract converted height
    t_z = data.get("t_z")
    if t_z is None:
        print(f"    [VDatum] No t_z in response: {data}")
        return None

    try:
        offset = float(t_z)
    except (ValueError, TypeError):
        print(f"    [VDatum] Invalid t_z value: {t_z}")
        return None

    return round(offset, 6)


# ══════════════════════════════════════════════════════════════════════════
# CO-OPS Metadata API (Fallback Method)
# ══════════════════════════════════════════════════════════════════════════

def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points in km."""
    R = 6371.0
    rlat1, rlon1 = math.radians(lat1), math.radians(lon1)
    rlat2, rlon2 = math.radians(lat2), math.radians(lon2)
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _fetch_coops_stations():
    """Fetch list of all NOAA CO-OPS stations."""
    print("  [CO-OPS fallback] Fetching station list...")
    resp = requests.get(COOPS_STATIONS_URL, params={"units": "metric"}, timeout=30)
    resp.raise_for_status()
    stations = []
    for s in resp.json().get("stations", []):
        try:
            stations.append({
                "id": str(s["id"]),
                "name": s.get("name", ""),
                "lat": float(s["lat"]),
                "lon": float(s["lng"]),
            })
        except (KeyError, ValueError, TypeError):
            continue
    print(f"  [CO-OPS fallback] Found {len(stations)} stations")
    return stations


def _find_nearest_station(lat, lon, stations, max_km=MAX_DISTANCE_KM):
    """Find nearest CO-OPS station within max_km."""
    best, best_dist = None, float("inf")
    for s in stations:
        d = _haversine_km(lat, lon, s["lat"], s["lon"])
        if d < best_dist:
            best_dist = d
            best = s
    if best_dist > max_km:
        return None, best_dist
    return best, best_dist


def _coops_get_offset(station_id):
    """
    Fetch MHHW-NAVD88 offset from CO-OPS datums for one station.
    Returns offset in METERS, or None.
    """
    url = COOPS_DATUMS_URL.format(station_id=station_id)
    try:
        resp = requests.get(url, params={"units": "metric"}, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        return None

    datums = {}
    for d in resp.json().get("datums", []):
        try:
            datums[d["name"]] = float(d["value"])
        except (KeyError, ValueError, TypeError):
            continue

    mhhw = datums.get("MHHW")
    navd88 = datums.get("NAVD88")
    if mhhw is not None and navd88 is not None:
        return round(mhhw - navd88, 6)
    return None


# ══════════════════════════════════════════════════════════════════════════
# Offset Cache
# ══════════════════════════════════════════════════════════════════════════

def load_offset_cache(cache_path):
    """Load cached datum offsets from JSON file."""
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            cache = json.load(f)
        print(f"  [datum_converter] Loaded offset cache: {len(cache)} entries")
        return cache
    return {}


def save_offset_cache(cache, cache_path):
    """Save datum offset cache to JSON file."""
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)
    print(f"  [datum_converter] Saved offset cache: {len(cache)} entries → {cache_path}")


# ══════════════════════════════════════════════════════════════════════════
# Main Conversion Entry Point
# ══════════════════════════════════════════════════════════════════════════

def convert_twl_to_mhhw(stations_list, twl_data, cache_path,
                        max_distance_km=MAX_DISTANCE_KM,
                        input_units="feet"):
    """
    Convert TWL data from NAVD88 to MHHW datum.

    Uses VDatum API as the primary conversion method. Falls back to CO-OPS
    station datums if VDatum fails for a given location.

    Parameters
    ----------
    stations_list : list[dict]
        Station metadata from build_json(). Each dict must have:
        'id', 'latitude', 'longitude'
    twl_data : dict
        TWL time-series from build_json(). Keys are station IDs,
        values are lists of dicts with 'value' in NAVD88.
    cache_path : str
        Path to datum_offsets.json cache file.
    max_distance_km : float
        Max distance to nearest CO-OPS station (used only for fallback).
    input_units : str
        "feet" or "meters" — the unit of TWL 'value' field.

    Returns
    -------
    twl_data_mhhw : dict
        Same structure as twl_data, with 'valueMHHW' added to each reading.
    station_datums : dict
        Datum info per station: method used, offset, status.
    """
    print("\n── NAVD88 → MHHW Datum Conversion (VDatum API) ──")

    # Build lat/lon lookup from stations_list
    station_coords = {}
    for s in stations_list:
        station_coords[s["id"]] = (s["latitude"], s["longitude"])

    # Load cache
    cache = load_offset_cache(cache_path)

    # Identify stations that need lookup
    station_ids_in_twl = set(twl_data.keys())
    uncached = [sid for sid in station_ids_in_twl
                if sid not in cache and sid in station_coords]

    if uncached:
        print(f"  {len(uncached)} stations need datum lookup (not in cache)")
    else:
        print(f"  All {len(station_ids_in_twl)} stations found in cache")

    # ── VDatum lookups for uncached stations ──
    vdatum_failures = []

    for i, sid in enumerate(uncached):
        if sid not in station_coords:
            cache[sid] = {"status": "NO_COORDS", "offset_ft": None, "method": None}
            continue

        lat, lon = station_coords[sid]
        print(f"  [{i+1}/{len(uncached)}] VDatum lookup: {sid} ({lat:.4f}, {lon:.4f})")

        time.sleep(VDATUM_DELAY)
        offset = _vdatum_get_offset(lat, lon, input_units=input_units)

        if offset is not None:
            print(f"    ✓ VDatum offset = {offset:.4f} {'ft' if input_units == 'feet' else 'm'}")
            cache[sid] = {
                "status": "OK",
                "method": "vdatum",
                "latitude": lat,
                "longitude": lon,
                "offset_ft": round(offset, 6) if input_units == "feet" else None,
                "offset_m": round(offset, 6) if input_units == "meters" else None,
            }
        else:
            print(f"    ✗ VDatum failed — will try CO-OPS fallback")
            vdatum_failures.append(sid)

    # ── CO-OPS fallback for VDatum failures ──
    if vdatum_failures:
        print(f"\n  CO-OPS fallback for {len(vdatum_failures)} stations...")
        try:
            coops_stations = _fetch_coops_stations()
        except Exception as e:
            print(f"  ⚠  CO-OPS station fetch failed: {e}")
            coops_stations = []

        for sid in vdatum_failures:
            lat, lon = station_coords[sid]
            nearest, dist = _find_nearest_station(lat, lon, coops_stations, max_distance_km)

            if nearest is None:
                print(f"    ⚠  {sid}: no CO-OPS station within {max_distance_km}km "
                      f"(nearest: {dist:.1f}km)")
                cache[sid] = {
                    "status": "UNAVAILABLE",
                    "method": None,
                    "nearest_distance_km": round(dist, 2),
                    "offset_ft": None,
                    "offset_m": None,
                }
                continue

            time.sleep(COOPS_DELAY)
            offset_m = _coops_get_offset(nearest["id"])

            if offset_m is None:
                print(f"    ⚠  {sid}: CO-OPS {nearest['id']} has no MHHW/NAVD88 datums")
                cache[sid] = {
                    "status": "DATUM_UNAVAILABLE",
                    "method": None,
                    "coops_station_id": nearest["id"],
                    "coops_station_name": nearest["name"],
                    "distance_km": round(dist, 2),
                    "offset_ft": None,
                    "offset_m": None,
                }
                continue

            # CO-OPS returns offset in meters; convert to feet if needed
            if input_units == "feet":
                offset_ft = round(offset_m * METERS_TO_FEET, 6)
            else:
                offset_ft = None

            print(f"    ✓ CO-OPS {nearest['id']} ({nearest['name']}) "
                  f"dist={dist:.1f}km, offset={offset_m:.4f}m")

            # CO-OPS offset convention: MHHW(STND) - NAVD88(STND)
            # To convert: value_MHHW = value_NAVD88 - offset
            # But VDatum returns: value_MHHW = value_NAVD88 + vdatum_offset
            # So CO-OPS offset needs to be negated to match VDatum convention
            cache[sid] = {
                "status": "OK",
                "method": "coops_fallback",
                "coops_station_id": nearest["id"],
                "coops_station_name": nearest["name"],
                "distance_km": round(dist, 2),
                "offset_ft": -offset_ft if offset_ft is not None else None,
                "offset_m": round(-offset_m, 6),
            }

    # Save updated cache
    save_offset_cache(cache, cache_path)

    # ── Apply conversion to all readings ──
    twl_data_mhhw = {}
    converted_count = 0
    skipped_count = 0

    for sid, readings in twl_data.items():
        entry = cache.get(sid, {})

        # Determine offset in the correct units
        if input_units == "feet":
            offset = entry.get("offset_ft")
        else:
            offset = entry.get("offset_m")

        if offset is None:
            # Can't convert — keep original values, mark as unconverted
            twl_data_mhhw[sid] = []
            for r in readings:
                new_r = dict(r)
                new_r["valueMHHW"] = None
                new_r["datumStatus"] = entry.get("status", "UNKNOWN")
                twl_data_mhhw[sid].append(new_r)
            skipped_count += 1
            continue

        # Apply: value_MHHW = value_NAVD88 + offset
        # (VDatum convention: offset = what 0 NAVD88 equals in MHHW)
        twl_data_mhhw[sid] = []
        for r in readings:
            new_r = dict(r)
            new_r["valueMHHW"] = round(r["value"] + offset, 4)
            new_r["datumStatus"] = "OK"
            twl_data_mhhw[sid].append(new_r)
        converted_count += 1

    print(f"\n  Conversion results:")
    print(f"    Converted:  {converted_count} stations")
    print(f"    Skipped:    {skipped_count} stations (no datum available)")
    print(f"    Total:      {len(twl_data)} stations")

    # Build station_datums summary (for metadata / debugging)
    station_datums = {}
    for sid in twl_data.keys():
        entry = cache.get(sid, {})
        station_datums[sid] = {
            "status": entry.get("status", "UNKNOWN"),
            "method": entry.get("method"),
            "offset_ft": entry.get("offset_ft"),
            "offset_m": entry.get("offset_m"),
            "coops_station_id": entry.get("coops_station_id"),
            "coops_station_name": entry.get("coops_station_name"),
            "distance_km": entry.get("distance_km"),
        }

    return twl_data_mhhw, station_datums
