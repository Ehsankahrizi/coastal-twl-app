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

The NWM outputs Total Water Level referenced to NAVD88. For many coastal applications, MHHW is the preferred datum. The pipeline handles this conversion automatically using the [NOAA VDatum API](https://vdatum.noaa.gov/docs/services.html):

- For each NWM station, the VDatum API is called with the station's lat/lon to compute the spatially-interpolated NAVD88→MHHW offset at that exact location
- VDatum performs the vertical datum transformation directly — no need to find a nearby tide gauge
- If VDatum is unavailable for a location (e.g., outside tidal grid coverage), the converter falls back to the [CO-OPS Metadata API](https://api.tidesandcurrents.noaa.gov/mdapi/prod/) to find the nearest tide station and compute the offset from its published datums
- Offsets are cached in `data/datum_offsets.json` so the API is only called once per station (persists across pipeline runs)

## Output Files

| File | Description |
|------|-------------|
| `data/stations.json` | Station metadata (id, name, lat, lon, elevation, network) |
| `data/twl_data.json` | TWL time-series grouped by station ID — values in **feet, NAVD88** |
| `data/twl_data_mhhw.json` | TWL time-series with both `value` (NAVD88) and `valueMHHW` (MHHW) — **feet** |
| `data/metadata.json` | Pipeline run metadata (timestamp, counts, conversion stats) |
| `data/datum_offsets.json` | Cached VDatum/CO-OPS datum offsets per station (auto-generated) |
| `data/station_datums.json` | Datum lookup details per station (method used, offset, status) |
| `data/htf_threshold.json` | HTF threshold details per station |


## Data URLs (GitHub Pages)

Once GitHub Pages is enabled:
- Stations: `https://ehsankahrizi.github.io/coastal-twl-app/data/stations.json`
- TWL Data (NAVD88): `https://ehsankahrizi.github.io/coastal-twl-app/data/twl_data.json`
- TWL Data (MHHW): `https://ehsankahrizi.github.io/coastal-twl-app/data/twl_data_mhhw.json`
- Metadata: `https://ehsankahrizi.github.io/coastal-twl-app/data/metadata.json`

## Data Sources

- **TWL Forecasts**: NOAA NWM via `gs://national-water-model/`
- **Station Metadata**: [Iowa Environmental Mesonet (IEM)](https://mesonet.agron.iastate.edu/sites/networks.php)
- **Datum Offsets**: [NOAA VDatum API](https://vdatum.noaa.gov/docs/services.html) (primary), [CO-OPS Metadata API](https://api.tidesandcurrents.noaa.gov/mdapi/prod/) (fallback)
- **Units**: feet
- **Datums**: NAVD88 (original) and MHHW (converted)
