#!/usr/bin/env python3
"""
NAVD88 → MHHW Datum Converter (VDatum API)
============================================
Converts NWM Total Water Level values from NAVD88 to MHHW using NOAA's
VDatum API (https://vdatum.noaa.gov/docs/services.html).

VDatum performs spatially-interpolated vertical datum transformations at
any lat/lon — no need to find a nearby tide station.

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
import os
import time

import requests

# ══════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════

VDATUM_URL = "https://vdatum.noaa.gov/vdatumweb/api/convert"
VDATUM_DELAY = 0.3       # seconds between API calls (be polite)
VDATUM_MAX_RETRIES = 3   # retry on transient failures before giving up
VDATUM_RETRY_DELAY = 2   # seconds to wait between retries


# ══════════════════════════════════════════════════════════════════════════
# VDatum API
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

    base_params = {
        "s_x": lon,
        "s_y": lat,
        "s_z": 0.0,               # zero height in NAVD88
        "s_h_frame": "NAD83_2011",
        "s_v_frame": "NAVD88",
        "s_v_unit": v_unit,
        "t_v_frame": "MHHW",
        "t_v_unit": v_unit,
    }

    # "auto" region is failing on the NOAA API. Try regions sequentially.
    # contiguous: Lower 48, ak: Alaska, hi: Hawaii, prvi: Puerto Rico/Virgin Islands
    regions_to_try = ["contiguous", "ak", "hi", "prvi", "as", "gcnmi"]

    for region in regions_to_try:
        params = base_params.copy()
        params["region"] = region
        
        for attempt in range(1, VDATUM_MAX_RETRIES + 1):
            try:
                resp = requests.get(VDATUM_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, ValueError) as e:
                # Network error or timeout - retry
                if attempt < VDATUM_MAX_RETRIES:
                    time.sleep(VDATUM_RETRY_DELAY)
                    continue
                # If we max out retries, give up on this region
                break
                
            # If the region is invalid for this coordinate, VDatum returns 200 OK
            # but includes an 'errorCode' (e.g., 412) in the JSON
            if "errorCode" in data:
                break # Try the next region
                
            # Check for other errors in response
            message = data.get("message", "")
            if message and "error" in message.lower():
                break # Try the next region

            # Extract converted height
            t_z = data.get("t_z")
            if t_z is not None:
                try:
                    val = round(float(t_z), 6)
                    # VDatum returns -999999.0 for "no data" when a point is out of coverage
                    # but technically inside the bounding box of the valid region.
                    if abs(val) < 1000:
                        return val
                    else:
                        break # "No data" flag found, no point trying other regions
                except (ValueError, TypeError):
                    pass
            
            # If we get here, the response was valid JSON but no valid t_z. Try next region.
            break

    # If all regions fail or return no valid offset
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
                        input_units="feet"):
    """
    Convert TWL data from NAVD88 to MHHW datum using NOAA VDatum API.

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
        print(f"  {len(uncached)} stations need VDatum lookup (not in cache)")
    else:
        print(f"  All {len(station_ids_in_twl)} stations found in cache")

    # ── VDatum lookups for uncached stations ──
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
            print(f"    ✗ VDatum failed for {sid} — no conversion available")
            cache[sid] = {
                "status": "UNAVAILABLE",
                "method": None,
                "latitude": lat,
                "longitude": lon,
                "offset_ft": None,
                "offset_m": None,
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
                new_r["datumStatus"] = entry.get("status", "UNKNOWN")
                twl_data_mhhw[sid].append(new_r)
            skipped_count += 1
            continue

        # Apply: value_MHHW = value_NAVD88 + offset
        # Put converted value into 'value' field so the app reads it directly
        twl_data_mhhw[sid] = []
        for r in readings:
            new_r = dict(r)
            new_r["valueNAVD88"] = r["value"]  # preserve original
            new_r["value"] = round(r["value"] + offset, 4)  # overwrite with MHHW
            new_r["datumStatus"] = "OK"
            twl_data_mhhw[sid].append(new_r)
        converted_count += 1

    print(f"\n  Conversion results:")
    print(f"    Converted:  {converted_count} stations")
    print(f"    Skipped:    {skipped_count} stations (outside VDatum coverage)")
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
        }

    return twl_data_mhhw, station_datums
