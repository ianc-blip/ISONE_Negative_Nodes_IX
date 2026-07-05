# EV Fast-Charger → Nodal Volatility (NYISO + ISO-NE)

A pipeline that (1) **calibrates** how much local nodal price volatility (and price
level) moves when a large (>4-port) DC-fast charging site energizes, and (2) **maps and
ranks** the NYISO and ISO-NE priced locations with the most EV fast-charging
infrastructure — so you can flag which nodes are most likely to see volatility change as
the buildout lands.

Station data comes from the **NREL Alternative Fuel Stations API** (real `open_date` +
`ev_dc_fast_num` per site), which lets the backtest run on *real dated openings* — the
last 3 years of large DCFC energizations — rather than a representative list. A
token-gated PlugShare scraper and an offline seed set are kept as fallbacks.

It sits alongside the [`isone_maps`](../isone_maps) negative-price/queue pipeline and
reuses its ISO-NE pnode geocodes.

---

## What it produces

| Output | What it is |
|---|---|
| `output/backtest_report.txt` / `.json` | Difference-in-differences event study: volatility change per charger site + headline calibration |
| `output/ev_node_ranking.txt` / `.json` | NYISO/ISO-NE zones ranked by planned DC-fast ports, with projected volatility uplift |
| `output/ev_planned_chargers_map_YYYYMMDD.html` | Interactive Folium map: planned stations + zone bubbles sized by planned DCFC ports |

---

## Quick start

```bash
pip install -r requirements.txt

# One-time: put your free NREL key where the pipeline can read it (git-ignored)
echo "NREL_API_KEY=your_key_here" > .env      # or export NREL_API_KEY=...

# Full run: NREL openings → backtest (real NYISO prices) → rank → map
python run_pipeline.py

# Faster: skip the network-heavy backtest, just rank + map
python run_pipeline.py --no-backtest

# Validate the NREL→backtest math offline (no NREL network needed)
python smoke_test.py
```

The backtest always uses NYISO's fully public Day-Ahead LBMP data. For the charger
side: with `NREL_API_KEY` set and `developer.nrel.gov` reachable, it pulls live NREL
stations; otherwise it falls back to the reproducible seed set so the pipeline still
runs. Get a free key at <https://developer.nrel.gov/signup/>.

> **Sandbox note:** some managed environments block `developer.nrel.gov` at the egress
> policy (the request never leaves the network). If you see "NREL fetch failed / host may
> be egress-blocked", run the pipeline where NREL is allow-listed (e.g. locally) or have
> the environment's network policy add `developer.nrel.gov`. `smoke_test.py` exercises the
> full NREL parsing + backtest path from a saved payload with no NREL network call.

---

## Part 1 — the volatility backtest

`volatility_backtest.py` runs a **difference-in-differences event study** on real NYISO
Day-Ahead zonal LBMP (`lmp_data.py`, source: `mis.nyiso.com`, no auth):

- For each large-DCFC event: **pre** = 90d→15d before energization, **post** = 15d→90d after
  (the 15-day gap drops commissioning noise).
- Four metrics per window: hourly LBMP std, mean daily price range, price-spike
  frequency (>95th percentile, threshold fixed from the pre window), and **mean price
  level** ($/MWh) — so we capture *"vol or price, or anything else."*
- **Treated** zone %Δ minus a **control** (median of all other NYISO zones) %Δ = the
  charger-attributable change, net of the market-wide seasonal move.

Events are the real NREL openings from `nrel_stations.historical_openings()` (last 3y,
>4 DCFC ports); with no NREL access it falls back to `seed_charger_events.json`.

What the data actually shows (NREL-shaped sample of 7 NYISO openings, real prices):

```
Hourly LBMP volatility (std)      : +7.7% mean DiD (median -1.9%, 29% of sites up)
Daily price range                 : +18.9% mean DiD (median -3.0%)
Price-spike frequency (>95th pct) : -19.6% mean DiD
Mean price level ($/MWh)          : +3.5% mean DiD (median -1.5%)
```

**Read honestly:** there is **no clean systematic volatility increase**. The positive
means are driven almost entirely by one Long Island site (+95% std); the medians hover
around zero or slightly negative. A single DCFC site (a few MW) is tiny against zonal
load, so this is the expected result at zonal granularity — the effect, if any, is
site-specific and easily confounded (a new generator/transmission change in the same
window). Re-run against the full live NREL opening set to get the population estimate.

## Part 2 — station data (NREL, PlugShare fallback)

`nrel_stations.py` is the primary source: it queries the NREL Alternative Fuel Stations
API for `ELEC` stations (all statuses, incl. `Planned`) across NY + the six New England
states, then normalizes each into `{iso, zone, dcfc_ports, l2_ports, open_date, status…}`
and snaps it to a load zone + nearest ISO-NE pnode. Two views:
`historical_openings()` (dated backtest events) and `planned_and_recent()` (ranking).

`plugshare_scraper.py` remains as a token-gated alternative (PlugShare's API returns
HTTP 401 without an `Authorization` token). Both fall back to
`seed_data/seed_ev_stations.json` (75 representative sites) when no source is reachable.
**Respect each provider's Terms of Service and rate limits.**

## Part 3 — ranking + map

`node_ranking.py` aggregates sites to each priced location, ranks by DC-fast ports, and
— using the Part-1 calibration — projects a per-zone volatility uplift
(`mean per-site DiD × √(large-site count)`, sqrt for in-zone overlap). The Folium map
layers individual stations (by ISO) over zone bubbles sized by DCFC ports, with the
projected uplift in each tooltip.

---

## Layout

```
ev_volatility/
├── run_pipeline.py          # orchestrator: NREL → backtest → rank → map
├── nrel_stations.py         # NREL Alt-Fuel-Stations fetch, parse, event/ranking views
├── volatility_backtest.py   # DiD event study on NYISO LBMP (vol + price)
├── lmp_data.py              # NYISO (live) + ISONE price fetchers, cached
├── plugshare_scraper.py     # token-gated PlugShare fallback + seed loader
├── node_ranking.py          # zone aggregation, calibration, Folium map
├── iso_regions.py           # bboxes, zone geocodes, geo + classification helpers
├── smoke_test.py            # offline NREL→backtest→ranking validation
├── .env                     # NREL_API_KEY (git-ignored, never committed)
├── seed_data/
│   ├── _generate_seed.py           # rebuilds the seed files (fixed RNG seed)
│   ├── seed_ev_stations.json       # 75 representative sites
│   ├── seed_charger_events.json    # fallback backtest events
│   └── nrel_sample_payload.json    # NREL-shaped fixture for smoke_test.py
└── output/                  # reports + map land here
```

## Data sources

| Data | Source | Auth |
|---|---|---|
| NYISO Day-Ahead zonal LBMP | `mis.nyiso.com/public/csv/damlbmp/` | None |
| EV stations (open_date, DCFC counts) | NREL Alt Fuel Stations API `developer.nrel.gov` | Key (`NREL_API_KEY`) |
| EV stations (fallback) | PlugShare region API | Token (`PLUGSHARE_TOKEN`) |
| ISO-NE bulk LMP (optional) | `iso-ne.com/static-assets/...` | None |
| ISO-NE pnode geocodes | `../isone_maps/seed_data/node_geocodes.json` | None |

## Notes & caveats

- **NREL gives real `open_date`s** — the backtest events are genuine dated openings when
  NREL is reachable. The committed report/ranking were produced from the seed fallback
  (NREL is egress-blocked in the build sandbox); re-run with NREL access for live numbers.
- **NYISO volatility is zonal** (the level NYISO prices zones). A single DCFC site is
  small vs. zonal load, so expect a weak/noisy signal at this granularity; the DiD control
  removes market-wide moves but not zone-specific confounders (a new generator or
  transmission upgrade in the same window). Treat it as an association, not causation.
- ISO-NE results also report the nearest **pnode** so they line up with the negative-price
  node maps in `isone_maps`. The backtest itself is NYISO-only today (NYISO LBMP is the
  live price feed); wiring ISO-NE zonal LMP into `lmp_data.py` extends it to New England.
