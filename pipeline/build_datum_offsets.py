#!/usr/bin/env python3
"""
Build / repair the NAVD88 → MHHW offset table (data/datum_offsets.json)
=======================================================================
Run this by hand, not from the 6-hour workflow. It computes the offset for
every NWM station once and saves it; each pipeline run then only reads the
table (and looks up brand-new stations).

By default only stations that are missing or failed before are (re)computed,
so re-running is cheap. Use --all to recompute every station.

Each entry records how its offset was obtained:
    method "vdatum" + region   — VDatum grid that covered the point
    method "coops" + station   — nearest CO-OPS tide station (≤ 10 km)
    status "UNAVAILABLE"       — neither source covers the point (typically
                                 far up a river or in a non-tidal marsh); such
                                 stations are left out of HTF comparisons

Usage:
    python pipeline/build_datum_offsets.py          # fill gaps
    python pipeline/build_datum_offsets.py --all    # recompute everything
"""

import argparse
import collections
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from datum_converter import load_offset_cache, lookup_offset, save_offset_cache  # noqa: E402

DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "data")
CACHE_PATH = os.path.join(DATA_DIR, "datum_offsets.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--all", action="store_true", help="recompute every station, not just failed/missing ones")
    args = parser.parse_args()

    with open(os.path.join(DATA_DIR, "stations.json")) as f:
        stations = json.load(f)
    cache = load_offset_cache(CACHE_PATH)

    todo = [s for s in stations
            if args.all or cache.get(s["id"], {}).get("status") != "OK"]
    print(f"{len(stations)} stations, {len(todo)} to look up")

    for i, s in enumerate(todo, 1):
        entry = lookup_offset(s["latitude"], s["longitude"], input_units="feet")
        cache[s["id"]] = entry
        how = entry.get("region") or (f"CO-OPS {entry.get('coopsStation')} @ {entry.get('coopsDistanceKm')} km"
                                      if entry.get("method") == "coops" else "none")
        print(f"[{i}/{len(todo)}] {s['id']:6} {entry['status']:11} {how:28} {entry.get('offset_ft')}")
        if i % 25 == 0:
            save_offset_cache(cache, CACHE_PATH)  # keep progress if interrupted

    save_offset_cache(cache, CACHE_PATH)

    ids = {s["id"] for s in stations}
    summary = collections.Counter(
        (e.get("status"), e.get("method"), e.get("region")) for k, e in cache.items() if k in ids)
    print("\nSummary for current stations:")
    for (status, method, region), n in summary.most_common():
        print(f"  {n:4}  {status:11} {method or '-':7} {region or ''}")


if __name__ == "__main__":
    main()
