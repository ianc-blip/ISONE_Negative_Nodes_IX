# ISONE Negative-Price Node + Interconnection Queue Maps

Automated monthly pipeline that generates three interactive Folium maps tracking:
1. **Where prices go negative** — top DA & RT nodes by negative-hour count, 2022–present
2. **Where new capacity is entering the grid** — ISONE interconnection queue by year & technology

The overlay of negative nodes against the queue pipeline reveals where future negative pricing
is most likely to emerge as solar, wind, and storage additions accumulate in already-stressed areas.

---

## Maps produced each run

| Filename | What it shows |
|---|---|
| `isone_multiyear_nodes_map_YYYYMMDD.html` | Negative-price nodes only (DA + RT, 2022–present). Circle size & colour = # negative hours. |
| `isone_da_rt_plus_queue_totals_by_year_tech_YYYYMMDD.html` | Above + queue totals (all techs) for the next 4 years. |
| `isone_da_rt_plus_queue_year_and_tech_YYYYMMDD.html` | Above + queue split by **year AND technology** (Solar/Wind/Storage/Other). |

All three have a togglable layer panel (top-right). Open in any browser — no server needed.

---

## Project layout

```
isone_maps/
├── map_generator.py         # builds the three Folium maps from seed data
├── isone_data_fetcher.py    # pulls live ISONE LMP CSVs + queue XLSX
├── scheduler.py             # --run-now (cron) or --daemon (long-running)
├── requirements.txt
├── seed_data/
│   ├── seed_multiyear.json  # 392 neg-price node records (2022–2025)
│   ├── seed_totals.json     # same + queue totals layer
│   ├── seed_year_tech.json  # same + queue by year & tech (658 records)
│   └── node_geocodes.json   # 135 lat/lon lookups for ISONE nodes & counties
├── output/                  # generated HTML maps land here
└── logs/
    └── scheduler.log
```

---

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate maps immediately using seed data (no credentials needed)
python scheduler.py --run-now

# 3. Open any of the three maps
open output/isone_multiyear_nodes_map_*.html
```

The seed data was extracted from maps dated October 2025 and covers 2022–2025 negative-price
nodes plus the ISONE queue pipeline through 2029. Maps regenerate with fresh dates each run.

---

## Enabling live data refresh

### ISONE API credentials

ISONE requires free registration for their ISO-Express API:
- Register at: https://webservices.iso-ne.com/docs/v1.1/
- Then set environment variables:

```bash
export ISONE_USER="your@email.com"
export ISONE_PASS="yourpassword"
export ISONE_REFRESH=1           # triggers live data pull before map generation
```

### Data sources used

| Data | Source | Auth |
|---|---|---|
| DA/RT LMP (annual bulk CSV) | `iso-ne.com/static-assets/documents/{year}/hourly/` | None |
| Interconnection Queue (XLSX) | `iso-ne.com/static-assets/documents/.../isone_iq.xlsx` | None |
| Hourly LMP by node (ISO-Express API) | `webservices.iso-ne.com/api/v1.1/` | Required |

For negative-hour counting, the annual bulk CSVs (no auth) are sufficient and updated each month
for the prior year. The authenticated API gives current-year access in real time.

---

## Scheduling options

### Option 1: cron (recommended for a Linux/Mac server)

```cron
# Run at 06:00 on the 1st of every month
0 6 1 * * cd /path/to/isone_maps && \
  ISONE_USER=you@email.com \
  ISONE_PASS=secret \
  ISONE_REFRESH=1 \
  python scheduler.py --run-now >> logs/scheduler.log 2>&1
```

### Option 2: GitHub Actions (zero-infrastructure, free)

Create `.github/workflows/monthly_maps.yml` — a template is included in this repo.
Maps are saved as workflow artifacts, downloadable from the Actions tab.

```yaml
# Trigger: 06:00 UTC on the 1st of every month
on:
  schedule:
    - cron: '0 6 1 * *'
  workflow_dispatch:   # also allows manual trigger
```

### Option 3: long-running daemon

```bash
python scheduler.py --daemon
```

Checks every minute and fires on the 1st of the month at 06:00 local time.

---

## How the maps work

### Negative-price nodes

- **Data**: annual DA and RT LMP data from ISONE; one row per node per hour
- **Metric**: count of hours where LMP < 0 across the calendar year
- **Top-N**: top 48–50 nodes per year/market (matching ISONE's published methodology)
- **Colour**: viridis scale — dark purple = most negative hours, yellow = fewest
- **Size**: circle radius proportional to negative-hour count

### Queue additions

- **Data**: ISONE interconnection queue spreadsheet (updated ~monthly by ISONE)
- **Aggregation**: MW summed by county, state, technology, and expected COD year
- **Technology classification**: Solar / Wind / Storage / Nuclear / Gas / Hydro / Other
  (based on 'Fuel Type' column in ISONE's queue file)
- **Size**: circle radius proportional to total MW at that county

### Interpreting the overlay

A county that sits on top of (or near) existing negative-price nodes AND has large
queue additions in the next 1–3 years is a high-risk location for:
- Increasing frequency and depth of negative prices
- Basis risk for generators at or electrically near that node
- Potential revenue risk for offtake contracts priced at hub vs. node

---

## Extending the pipeline

### Add a new year of LMP data

```python
# In map_generator.py, update the years list:
years = [2022, 2023, 2024, 2025, 2026]   # add current year once data is available
```

### Add a new technology colour

```python
# In map_generator.py TECH_COLORS dict:
TECH_COLORS["Offshore Wind"] = "#0077b6"
```

### Add more queue years forward

```python
# In map_generator.py run():
queue_years = [2026, 2027, 2028, 2029, 2030]
```

### Change the top-N threshold

```python
TOP_N = 75   # increase from default 50
```

---

## Dependencies

```
folium>=0.14.0      # interactive Leaflet maps
pandas>=2.0.0       # data manipulation
requests>=2.31.0    # HTTP fetching
openpyxl>=3.1.0     # reading ISONE queue XLSX
schedule>=1.2.0     # daemon scheduler
```
