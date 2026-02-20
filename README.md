# Coastal TWL Forecast

Automated pipeline that fetches, parses, and serves **NOAA National Water Model (NWM)** short-range coastal **Total Water Level (TWL)** forecast data for an iOS app.

## How It Works

1. **GitHub Actions** runs every 6 hours (after NWM cycles t00z, t06z, t12z, t18z)
2. Downloads SHEF forecast files from NOAA's Google Cloud Storage bucket
3. Parses the data and matches station metadata from IEM
4. Exports `stations.json` and `twl_data.json` to the `data/` folder
5. Commits and pushes updated files
6. **GitHub Pages** serves the JSON files via HTTPS for the iOS app

## Data URLs (GitHub Pages)

Once GitHub Pages is enabled:
- Stations: `https://ehsankahrizi.github.io/coastal-twl-app/data/stations.json`
- TWL Data: `https://ehsankahrizi.github.io/coastal-twl-app/data/twl_data.json`
- Metadata: `https://ehsankahrizi.github.io/coastal-twl-app/data/metadata.json`

## Data Sources

- **TWL Forecasts**: NOAA NWM via `gs://national-water-model/`
- **Station Metadata**: [Iowa Environmental Mesonet (IEM)](https://mesonet.agron.iastate.edu/sites/networks.php)
- **Units**: feet, NAVD88 vertical datum
