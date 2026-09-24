# Coastal TWL Forecast

Automated pipeline that fetches, parses, and serves NOAA National Water Model (NWM) short-range coastal Total Water Level (TWL) forecast data for an iOS app. Includes automatic datum conversion from NAVD88 to MHHW (Mean Higher High Water).

## How It Works

1. **GitHub Actions** runs every 6 hours (after NWM cycles t00z, t06z, t12z, t18z)
2. Downloads SHEF forecast files from NOAA's Google Cloud Storage bucket (`national-water-model`)
3. Parses the data and matches station metadata from IEM
4. Exports `stations.json` and `twl_data.json` (NAVD88) to the `data/` folder
5. Converts TWL values from NAVD88 to MHHW using the [NOAA VDatum API](https://vdatum.noaa.gov/docs/services.html) and exports `twl_data_mhhw.json`
6. Commits and pushes updated files
7. **GitHub Pages** serves the JSON files via HTTPS for the iOS app

## NAVD88 → MHHW Datum Conversion

The NWM outputs Total Water Level referenced to NAVD88, while the HTF thresholds are heights above MHHW. Every station therefore needs a NAVD88 → MHHW offset (`value_MHHW = value_NAVD88 + offset`).

**Offset table, computed once.** Offsets live in `data/datum_offsets.json`. They are computed once by hand with:

```bash
python pipeline/build_datum_offsets.py          # fill in missing / failed stations
python pipeline/build_datum_offsets.py --all    # recompute every station
```

The 6-hour pipeline only *reads* this table. It looks up a station only when the station is brand new, and it never retries failures.

**How an offset is found:**

1. **[NOAA VDatum API](https://vdatum.noaa.gov/docs/services.html).** VDatum covers the lower 48 with a general `contiguous` grid plus three regional tidal grids: `westcoast`, `chesapeak_delaware` and `wgom` (western Gulf). The regional grids only answer when the target horizontal frame is `IGS14`, so each region is tried with the frame it accepts. The table records which region produced each offset.
2. **Fallback: nearest NOAA CO-OPS tide station** within 10 km that publishes both NAVD88 and MHHW ([CO-OPS Metadata API](https://api.tidesandcurrents.noaa.gov/mdapi/prod/)). The offset is `NAVD88 − MHHW`, and the station ID and distance are recorded.
3. **Otherwise the station is marked `UNAVAILABLE`.** These are typically gauges far up rivers or in non-tidal marshes, where MHHW is not defined.

**Unconverted stations are never compared with HTF thresholds.** `htf_processor.py` averages only stations whose TWL was converted to MHHW. Stations within the radius that could not be converted are listed under `excludedStations`.

## Units

- NWM TWL values are in **feet**.
- The HTF thresholds (`data/htf_threshold.json`, from [Mahmoudi et al., 2024, *Nature Communications*](https://doi.org/10.1038/s41467-024-48545-1)) are in **meters above MHHW**.
- `htf_processor.py` converts the thresholds to feet before comparing. In `nwm_htf_*km.json`:
  - `htfMidThreshold` and `htfRangeFt` are in feet, the same unit as `meanForecast`.
  - `htfMidThresholdM` and `htfRange` keep the original meters.
  - `units` is `"ft"`.

## Output Files

| File | Description |
|------|-------------|
| `data/stations.json` | Station metadata (id, name, lat, lon, elevation, network) |
| `data/twl_data.json` | TWL time-series grouped by station ID — values in **feet, NAVD88** |
| `data/twl_data_mhhw.json` | TWL time-series with both `value` (NAVD88) and `valueMHHW` (MHHW) — **feet** |
| `data/metadata.json` | Pipeline run metadata (timestamp, counts, conversion stats) |
| `data/datum_offsets.json` | Cached VDatum/CO-OPS datum offsets per station (auto-generated) |
| `data/station_datums.json` | Datum lookup details per station (status, method, VDatum region or CO-OPS station, offset) |
| `data/htf_threshold.json` | HTF threshold points (meters above MHHW) |
| `data/nwm_htf_5km.json`, `data/nwm_htf_10km.json` | Per HTF point: mean MHHW forecast of converted NWM stations within 5 / 10 km, threshold in feet, matched and excluded stations |
| `data/htf_status_5km.json`, `data/htf_status_10km.json` | Per HTF point: `"exceeds"` or `"below"` (points without converted stations nearby are absent) |


## Data URLs (GitHub Pages)

Once GitHub Pages is enabled:
- Stations: `https://ehsankahrizi.github.io/coastal-twl-app/data/stations.json`
- TWL Data (NAVD88): `https://ehsankahrizi.github.io/coastal-twl-app/data/twl_data.json`
- TWL Data (MHHW): `https://ehsankahrizi.github.io/coastal-twl-app/data/twl_data_mhhw.json`
- Metadata: `https://ehsankahrizi.github.io/coastal-twl-app/data/metadata.json`

## Data Sources

- **TWL Forecasts**: NOAA NWM via `gs://national-water-model/`
- **Station Metadata**: [Iowa Environmental Mesonet (IEM)](https://mesonet.agron.iastate.edu/sites/networks.php)
- **Datum Offsets**: [NOAA VDatum API](https://vdatum.noaa.gov/docs/services.html) (primary, regional grids), [CO-OPS Metadata API](https://api.tidesandcurrents.noaa.gov/mdapi/prod/) (fallback, ≤ 10 km)
- **HTF Thresholds**: [Mahmoudi et al., 2024](https://doi.org/10.1038/s41467-024-48545-1), meters above MHHW
- **Units**: feet (forecasts and output thresholds); HTF source thresholds in meters
- **Datums**: NAVD88 (original) and MHHW (converted)
