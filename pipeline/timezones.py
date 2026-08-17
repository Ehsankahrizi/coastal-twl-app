"""
Resolve IANA time-zone identifiers for coastal points.

Forecast times stay in UTC everywhere in the served JSON — that never changes.
What ships alongside them is the time zone *of the location*, so the app can
render "2:00 PM EDT" for a Florida point and "11:00 AM PDT" for a California
one out of the same UTC timestamp.

Why the zone is resolved here rather than on the device:

  * iOS has no offline coordinate-to-time-zone database. Reverse geocoding
    each point would mean thousands of network round-trips.
  * Longitude alone is not enough. The Florida panhandle is Central while the
    peninsula is Eastern, so the boundary has to come from real polygons.

An IANA identifier is shipped rather than a fixed UTC offset, because the
offset is not a property of a place — it changes at daylight-saving
transitions, and an 18-hour forecast series can straddle one.
"""

_finder = None


def _get_finder():
    """Lazily build the TimezoneFinder; it loads binary polygon data once."""
    global _finder
    if _finder is None:
        from timezonefinder import TimezoneFinder
        _finder = TimezoneFinder()
    return _finder


def timezone_for(lat, lon):
    """
    IANA time-zone identifier for a coordinate, e.g. "America/New_York".

    Returns None when the point matches no zone polygon — well offshore, for
    instance. Callers should omit the field in that case so the app falls back
    to labelling the time as UTC rather than guessing wrong.
    """
    try:
        return _get_finder().timezone_at(lat=float(lat), lng=float(lon))
    except Exception as e:  # noqa: BLE001 - never let this break a data run
        print(f"  WARNING: timezone lookup failed for ({lat}, {lon}): {e}")
        return None
