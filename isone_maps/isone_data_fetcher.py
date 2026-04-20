"""
isone_data_fetcher.py
=====================
Pulls live ISONE data and converts it to the node format used by map_generator.py.

Two data tracks:
  A) Negative-price nodes  — ISONE ISO-Express bulk CSVs (no auth needed)
  B) Interconnection queue — ISONE public queue spreadsheet

Live data is merged with the seed JSON files so maps always have a complete picture.
"""

import io, re, json, logging
from pathlib import Path
from typing import Optional
from datetime import date
import requests
import pandas as pd

log = logging.getLogger("isone_fetcher")

# ── ISONE public data URLs ────────────────────────────────────────────────────
# Bulk LMP CSVs — no credentials required
# Pattern: https://www.iso-ne.com/static-assets/documents/{year}/hourly/da_lmp_{year}.csv
#          (replace 'da' with 'rt' for Real-Time)
ISONE_BULK_DA_LMP_URL = "https://www.iso-ne.com/static-assets/documents/{year}/hourly/da_lmp_{year}.csv"
ISONE_BULK_RT_LMP_URL = "https://www.iso-ne.com/static-assets/documents/{year}/hourly/rt_lmp_{year}.csv"

# Interconnection Queue (XLSX, updated monthly)
ISONE_QUEUE_URL = "https://www.iso-ne.com/static-assets/documents/2024/04/isone_iq.xlsx"

# ISO-Express authenticated API (optional, higher resolution)
ISONE_API_BASE = "https://webservices.iso-ne.com/api/v1.1"


# ── Node geocoding ────────────────────────────────────────────────────────────

def build_geocode_index(seed_json_paths: list[Path]) -> dict:
    """
    Build a {node_name: (lat, lon)} index from the seed JSON files.
    This lets us geolocate new nodes that appear in live data.
    """
    index = {}
    for path in seed_json_paths:
        if not path.exists():
            continue
        with open(path) as f:
            nodes = json.load(f)
        for n in nodes:
            # Try to extract node name from tooltip
            m = re.match(r'^(\S+)', n.get("tooltip", ""))
            if m:
                key = m.group(1).upper()
                index[key] = (n["lat"], n["lon"])
    log.info("Geocode index: %d known nodes", len(index))
    return index


# ── LMP negative-hour counting ────────────────────────────────────────────────

def fetch_negative_hours_from_bulk_csv(year: int, market: str = "DA") -> pd.DataFrame:
    """
    Download an ISONE annual bulk LMP CSV and count negative-price hours per node.

    Returns DataFrame: node_name, neg_hrs, lat (NaN), lon (NaN)
    Columns in ISONE bulk CSV typically:
      Date, Hour, Location ID, Location Name, Location Type, Marginal Loss Component,
      Energy Component, Congestion Component, LMP
    """
    url = (ISONE_BULK_DA_LMP_URL if market.upper() == "DA" else ISONE_BULK_RT_LMP_URL).format(year=year)
    log.info("Fetching %s %s LMP data: %s", market, year, url)
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
    except Exception as e:
        log.error("Bulk CSV fetch failed: %s", e)
        return pd.DataFrame()

    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    lmp_col = next((c for c in df.columns if "lmp" in c and "energy" not in c
                    and "loss" not in c and "congestion" not in c), None)
    name_col = next((c for c in df.columns if "name" in c), None)

    if not lmp_col or not name_col:
        log.error("Could not identify LMP / name columns in %s", list(df.columns))
        return pd.DataFrame()

    df[lmp_col] = pd.to_numeric(df[lmp_col], errors="coerce")
    neg = df[df[lmp_col] < 0]
    counts = neg.groupby(name_col).size().reset_index(name="neg_hrs")
    counts.rename(columns={name_col: "node_name"}, inplace=True)
    counts["lat"] = float("nan")
    counts["lon"] = float("nan")
    log.info("%d nodes with negative hours in %s %d", len(counts), market, year)
    return counts


def merge_neg_hours_into_seed(seed: list[dict], neg_df: pd.DataFrame,
                               year: int, market: str,
                               geocodes: dict, top_n: int = 50) -> list[dict]:
    """
    Merge fresh negative-hour counts into the seed list.
    Replaces existing entries for (year, market) and adds new ones.
    """
    if neg_df.empty:
        return seed

    layer_tag = f"{year} {market}"
    # Remove stale entries for this year/market
    seed = [n for n in seed if layer_tag not in n.get("layer", "")]

    top = neg_df.nlargest(top_n, "neg_hrs")
    vmin, vmax = top["neg_hrs"].min(), top["neg_hrs"].max()

    for _, row in top.iterrows():
        name_key = row["node_name"].upper().split()[0]
        lat, lon = geocodes.get(name_key, (None, None))
        if lat is None:
            log.debug("No geocode for %s — skipping", row["node_name"])
            continue

        from map_generator import _viridis_color, _radius
        neg_hrs = int(row["neg_hrs"])
        color   = _viridis_color(neg_hrs, vmin, vmax)
        radius  = _radius(neg_hrs, vmin, vmax)
        tooltip = f"{row['node_name']} • {neg_hrs} neg hrs ({market}, {year})"

        seed.append({
            "lat": lat, "lon": lon,
            "radius": radius, "color": color,
            "tooltip": tooltip,
            "layer": f"{layer_tag} (top {len(top)})",
        })

    log.info("Merged %d nodes for %s %d", len(top), market, year)
    return seed


# ── Queue data ────────────────────────────────────────────────────────────────

# Technology keyword mapping for ISONE queue 'Fuel Type' column
TECH_KEYWORDS = {
    "Solar":   ["solar", "pv", "photovoltaic"],
    "Wind":    ["wind"],
    "Storage": ["storage", "battery", "bess"],
    "Nuclear": ["nuclear"],
    "Gas":     ["natural gas", "gas", "ng", "combined cycle", "combustion"],
    "Hydro":   ["hydro", "water"],
}

def _classify_tech(fuel_str: str) -> str:
    s = str(fuel_str).lower()
    for tech, keywords in TECH_KEYWORDS.items():
        if any(k in s for k in keywords):
            return tech
    return "Other"


def fetch_and_parse_queue(url: str = ISONE_QUEUE_URL) -> pd.DataFrame:
    """
    Download and parse the ISONE interconnection queue spreadsheet.

    Returns DataFrame: county, state, lat, lon, mw, technology, interconnection_year
    Note: ISONE queue format changes occasionally; column detection is flexible.
    """
    log.info("Downloading ISONE queue: %s", url)
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        xl = pd.ExcelFile(io.BytesIO(r.content))
    except Exception as e:
        log.error("Queue download failed: %s", e)
        return pd.DataFrame()

    # Find the right sheet
    sheet = next((s for s in xl.sheet_names
                  if any(k in s.lower() for k in ["queue", "active", "project"])),
                 xl.sheet_names[0])
    log.info("Using sheet '%s'", sheet)
    raw = xl.parse(sheet)
    raw.columns = [str(c).strip() for c in raw.columns]

    # Flexible column mapping
    col_map = {}
    for c in raw.columns:
        cl = c.lower()
        if "county" in cl:                         col_map["county"] = c
        elif "state" in cl and "county" not in cl: col_map["state"]  = c
        elif any(k in cl for k in ["mw", "capacity", "size"]):
            if "mw" not in col_map:                col_map["mw"]     = c
        elif any(k in cl for k in ["fuel", "tech", "type", "resource"]):
            if "tech" not in col_map:              col_map["tech"]   = c
        elif any(k in cl for k in ["year", "date", "cod", "commercial"]):
            if "year" not in col_map:              col_map["year"]   = c
        elif "lat" in cl:                          col_map["lat"]    = c
        elif any(k in cl for k in ["lon", "lng"]): col_map["lon"]   = c

    required = ["county", "state", "mw", "tech", "year"]
    missing  = [k for k in required if k not in col_map]
    if missing:
        log.warning("Queue: missing columns %s — available: %s", missing, list(raw.columns))
        return pd.DataFrame()

    df = raw.rename(columns=col_map)[list(col_map.keys())].copy()
    df["mw"]   = pd.to_numeric(df["mw"],   errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df.dropna(subset=["mw", "year"], inplace=True)
    df["year"] = df["year"].astype(int)
    df["technology"] = df["tech"].apply(_classify_tech)
    if "lat" not in df.columns: df["lat"] = float("nan")
    if "lon" not in df.columns: df["lon"] = float("nan")

    log.info("Queue parsed: %d projects across years %s–%s",
             len(df), df["year"].min(), df["year"].max())
    return df


def merge_queue_into_seed(seed: list[dict], queue_df: pd.DataFrame,
                           geocodes: dict,
                           future_years: list[int] = None) -> list[dict]:
    """
    Merge parsed queue data into the seed for Map 2 / Map 3.
    Groups by county+year+tech and aggregates MW.
    """
    if queue_df.empty:
        return seed
    if future_years is None:
        future_years = list(range(date.today().year, date.today().year + 4))

    # Remove stale queue entries
    seed = [n for n in seed if "Queue" not in n.get("layer", "")]

    for yr in future_years:
        yr_df = queue_df[queue_df["year"] == yr]
        if yr_df.empty:
            continue

        # All-tech totals
        totals = yr_df.groupby(["county", "state"])["mw"].sum().reset_index()
        for _, row in totals.iterrows():
            label = f"{row['county']}, {row['state']}"
            lat, lon = _geocode_county(label, geocodes)
            if lat is None: continue
            seed.append({
                "lat": lat, "lon": lon,
                "radius": 6.0, "color": "#6d6875",
                "tooltip": f"{label} • {row['mw']:.1f} MW • {yr}",
                "layer": f"Queue {yr} All",
            })

        # By technology
        by_tech = yr_df.groupby(["county", "state", "technology"])["mw"].sum().reset_index()
        for _, row in by_tech.iterrows():
            label = f"{row['county']}, {row['state']}"
            lat, lon = _geocode_county(label, geocodes)
            if lat is None: continue
            from map_generator import TECH_COLORS
            color = TECH_COLORS.get(row["technology"], "#6d6875")
            seed.append({
                "lat": lat, "lon": lon,
                "radius": 6.0, "color": color,
                "tooltip": f"{label} • {row['mw']:.1f} MW • {row['technology']} • {yr}",
                "layer": f"Queue {yr} {row['technology']}",
            })

    log.info("Queue entries merged: seed now has %d nodes", len(seed))
    return seed


def _geocode_county(county_state: str, geocodes: dict) -> tuple:
    """Simple lookup; extend with a geopy call for unknowns."""
    key = county_state.upper()
    return geocodes.get(key, (None, None))


# ── Full refresh ──────────────────────────────────────────────────────────────

def refresh_all_seed_data(seed_dir: Path, years: list[int] = None,
                           api_user: str = None, api_pass: str = None) -> dict:
    """
    Pull fresh LMP + queue data and update all three seed JSON files.
    Returns dict of {seed_name: node_count}.
    """
    if years is None:
        today = date.today()
        years = [today.year - 1, today.year]

    seed_paths = {
        "multiyear":  seed_dir / "seed_multiyear.json",
        "totals":     seed_dir / "seed_totals.json",
        "year_tech":  seed_dir / "seed_year_tech.json",
    }

    geocodes = build_geocode_index(list(seed_paths.values()))

    results = {}
    for name, path in seed_paths.items():
        with open(path) as f:
            seed = json.load(f)

        # Update negative-price layers
        for yr in years:
            for mkt in ("DA", "RT"):
                neg_df = fetch_negative_hours_from_bulk_csv(yr, mkt)
                seed   = merge_neg_hours_into_seed(seed, neg_df, yr, mkt, geocodes)

        # Update queue layers
        if "totals" in name or "tech" in name:
            queue_df = fetch_and_parse_queue()
            seed = merge_queue_into_seed(seed, queue_df, geocodes)

        with open(path, "w") as f:
            json.dump(seed, f, indent=2)

        results[name] = len(seed)
        log.info("Updated %s: %d nodes", name, len(seed))

    return results
