# EV Fast-Charger → Nodal Volatility (NYISO + ISO-NE)

A pipeline that (1) **calibrates** how much local nodal price volatility moves when a
large (>4-port) DC-fast charging site energizes, and (2) **maps and ranks** the NYISO
and ISO-NE priced locations with the most *planned* EV fast-charging infrastructure —
so you can flag which nodes are most likely to see volatility change as the buildout lands.

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

# Full run: backtest (real NYISO prices) + scrape/seed + rank + map
python run_pipeline.py

# Faster: skip the network-heavy backtest, just scrape + rank + map
python run_pipeline.py --no-backtest
```

No credentials are needed for a first run — the backtest uses NYISO's fully public
Day-Ahead LBMP data, and the charger side falls back to a reproducible seed set.

---

## Part 1 — the volatility backtest

`volatility_backtest.py` runs a **difference-in-differences event study** on real NYISO
Day-Ahead zonal LBMP (`lmp_data.py`, source: `mis.nyiso.com`, no auth):

- For each large-DCFC event: **pre** = 90d→15d before energization, **post** = 15d→90d after
  (the 15-day gap drops commissioning noise).
- Three volatility metrics per window: hourly LBMP std, mean daily price range, and
  price-spike frequency (>95th percentile, threshold fixed from the pre window).
- **Treated** zone %Δ minus a **control** (median of all other NYISO zones) %Δ = the
  charger-attributable change, net of the market-wide seasonal move.

Headline from the shipped event set (8 sites):

```
Hourly LBMP volatility (std)      : +13.9% mean DiD (median +3.6%, 62% of sites up)
Daily price range                 : +22.8% mean DiD (median -3.0%)
Price-spike frequency (>95th pct) : +67.1% mean DiD
```

The mean is pulled up by the Long Island site; the **median +3.6%** is the more robust
read. The direction is consistent with theory — added fast-charging load raises and
roughens local prices — but the shipped event *dates/locations are representative*
(`seed_data/seed_charger_events.json`). Swap in a scraped/known set of dated
energizations and the same machinery recomputes real DiD numbers.

## Part 2 — scraping planned chargers

`plugshare_scraper.py` sweeps PlugShare's region API across the NYISO and ISO-NE
bounding boxes, keeps **planned / under-construction** pins, classifies DC-fast vs L2
ports, and snaps each site to its load zone + nearest ISO-NE pnode.

PlugShare's API is **token-gated** (anonymous requests return HTTP 401), exactly like the
ISONE API in the sibling package. To go live:

```bash
export PLUGSHARE_TOKEN="Basic <token>"   # the Authorization header value the app uses
python run_pipeline.py
```

Without a token it uses `seed_data/seed_ev_stations.json` (75 representative planned
sites across all 19 zones) so the pipeline runs fully offline. **Respect PlugShare's
Terms of Service and rate limits when using a token.**

## Part 3 — ranking + map

`node_ranking.py` aggregates planned sites to each priced location, ranks by planned
DC-fast ports, and — using the Part-1 calibration — projects a per-zone volatility
uplift (`mean per-site DiD × √(large-site count)`, sqrt for in-zone overlap). The Folium
map layers individual stations (by ISO) over zone bubbles sized by planned DCFC ports,
with the projected uplift in each tooltip.

---

## Layout

```
ev_volatility/
├── run_pipeline.py          # orchestrator: backtest → scrape → rank → map
├── volatility_backtest.py   # DiD event study on NYISO LBMP
├── lmp_data.py              # NYISO (live) + ISONE price fetchers, cached
├── plugshare_scraper.py     # token-gated PlugShare scraper + seed fallback
├── node_ranking.py          # zone aggregation, calibration, Folium map
├── iso_regions.py           # bboxes, zone geocodes, geo + classification helpers
├── seed_data/
│   ├── _generate_seed.py        # rebuilds the two seed files (fixed RNG seed)
│   ├── seed_ev_stations.json    # 75 representative planned sites
│   └── seed_charger_events.json # 8 large-DCFC backtest events
└── output/                  # reports + map land here
```

## Data sources

| Data | Source | Auth |
|---|---|---|
| NYISO Day-Ahead zonal LBMP | `mis.nyiso.com/public/csv/damlbmp/` | None |
| ISO-NE bulk LMP (optional) | `iso-ne.com/static-assets/...` | None |
| ISO-NE pnode geocodes | `../isone_maps/seed_data/node_geocodes.json` | None |
| Planned EV chargers | PlugShare region API | Token (`PLUGSHARE_TOKEN`) |

## Notes & caveats

- **Charger event dates are representative**, not a scraped energization log. PlugShare
  gives *planned* pins (Part 2/3); pairing them with realized in-service dates for a
  bigger backtest is the natural next step once a token is available.
- NYISO volatility is measured at **zonal** granularity (the level NYISO prices zones);
  ISO-NE results also report the nearest **pnode** so they line up with the negative-price
  node maps in `isone_maps`.
- The DiD control removes market-wide moves but not zone-specific confounders (a new
  generator or transmission upgrade in the same window). Treat the calibration as an
  association, not a causal point estimate.
```
