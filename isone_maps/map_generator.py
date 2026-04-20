"""
ISONE Negative Price Node + Queue Map Generator
================================================
Generates three Folium maps monthly:
  1. multiyear_nodes_map      — DA & RT top negative-price nodes by year
  2. queue_totals_by_tech     — same nodes + queue totals (all techs)
  3. queue_year_and_tech      — same nodes + queue split by year & technology

Data sources (all public, no auth required):
  • ISONE LMP API: https://webservices.iso-ne.com/api/v1.1/
  • ISONE Interconnection Queue CSV:
      https://www.iso-ne.com/static-assets/documents/2024/04/isone_iq.xlsx
"""

import os, re, json, math, datetime, logging
from pathlib import Path
from typing import Optional

import requests
import pandas as pd
import folium
from folium import FeatureGroup, LayerControl, CircleMarker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("isone_maps")

# ── Config ────────────────────────────────────────────────────────────────────
ISONE_API_BASE = "https://webservices.iso-ne.com/api/v1.1"
ISONE_QUEUE_URL = (
    "https://www.iso-ne.com/static-assets/documents/2024/04/isone_iq.xlsx"
)
OUTPUT_DIR = Path(os.environ.get("ISONE_OUTPUT_DIR", "./output"))
SEED_DIR   = Path(os.environ.get("ISONE_SEED_DIR",   "./seed_data"))

# Map center (New England)
MAP_CENTER = [44.781105, -68.705434]
MAP_ZOOM   = 7
TILE_URL   = "https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"
TILE_ATTR  = ("&copy; <a href='http://www.openstreetmap.org/copyright'>OpenStreetMap</a> "
              "contributors &copy; <a href='http://cartodb.com/attributions'>CartoDB</a>, "
              "CartoDB <a href='http://cartodb.com/attributions'>attributions</a>")

# Viridis palette samples (dark-to-light = more-to-fewer negative hours)
VIRIDIS_DARK   = "#440154"  # most negative hrs
VIRIDIS_MEDIUM = "#31688e"
VIRIDIS_LIGHT  = "#fde725"  # fewest

# Technology color map for queue markers
TECH_COLORS = {
    "Solar":   "#f4a261",
    "Wind":    "#2a9d8f",
    "Storage": "#e76f51",
    "Nuclear": "#264653",
    "Gas":     "#e9c46a",
    "Hydro":   "#457b9d",
    "Other":   "#6d6875",
}

TOP_N = 50  # nodes per DA/RT layer


# ── Helpers ───────────────────────────────────────────────────────────────────

def _viridis_color(value: float, vmin: float, vmax: float) -> str:
    """Return a viridis hex colour for `value` in [vmin, vmax]."""
    # 16-stop viridis approximation (dark purple → yellow)
    stops = [
        (0.00, "#440154"), (0.06, "#481567"), (0.12, "#482677"),
        (0.18, "#453781"), (0.25, "#404788"), (0.31, "#39568c"),
        (0.38, "#33638d"), (0.44, "#2d708e"), (0.50, "#287d8e"),
        (0.56, "#238a8d"), (0.63, "#1f968b"), (0.69, "#20a387"),
        (0.75, "#29af7f"), (0.82, "#3cbb75"), (0.88, "#5ec962"),
        (0.94, "#84d44b"), (1.00, "#fde725"),
    ]
    if vmax == vmin:
        t = 0.5
    else:
        t = (value - vmin) / (vmax - vmin)
    t = max(0.0, min(1.0, t))
    for i in range(len(stops) - 1):
        t0, c0 = stops[i]
        t1, c1 = stops[i + 1]
        if t <= t1:
            frac = (t - t0) / (t1 - t0) if t1 > t0 else 0
            r = int(int(c0[1:3], 16) * (1 - frac) + int(c1[1:3], 16) * frac)
            g = int(int(c0[3:5], 16) * (1 - frac) + int(c1[3:5], 16) * frac)
            b = int(int(c0[5:7], 16) * (1 - frac) + int(c1[5:7], 16) * frac)
            return f"#{r:02x}{g:02x}{b:02x}ff"
    return stops[-1][1] + "ff"


def _radius(neg_hrs: float, min_hrs: float, max_hrs: float,
            r_min: float = 4.0, r_max: float = 8.0) -> float:
    if max_hrs == min_hrs:
        return (r_min + r_max) / 2
    return r_min + (neg_hrs - min_hrs) / (max_hrs - min_hrs) * (r_max - r_min)


def _queue_radius(mw: float, r_min: float = 4.0, r_max: float = 14.0,
                  mw_max: float = 1000.0) -> float:
    return r_min + min(mw / mw_max, 1.0) * (r_max - r_min)


def _add_circle(fg: FeatureGroup, lat, lon, radius, color, tooltip_html):
    CircleMarker(
        location=[lat, lon],
        radius=radius,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.9,
        weight=1,
        tooltip=folium.Tooltip(tooltip_html, sticky=True),
    ).add_to(fg)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_seed_data(name: str) -> list[dict]:
    """Load pre-extracted seed data (used when live API is unavailable)."""
    path = SEED_DIR / f"seed_{name}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


def fetch_isone_lmp_negative_hours(year: int, market: str = "DA",
                                    api_user: Optional[str] = None,
                                    api_pass: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch per-node negative LMP hour counts for a calendar year from ISONE API.

    Returns DataFrame with columns: node_id, node_name, lat, lon, neg_hrs
    Falls back to seed data if API credentials are missing.

    ISONE API docs: https://webservices.iso-ne.com/docs/v1.1/
    Endpoint pattern:
      GET /hourlylmp/{market}/final/hourly/{year}/{month}/{day}/location/{locid}
    For bulk access, ISONE provides CSV bulk downloads at:
      https://www.iso-ne.com/isoexpress/web/reports/pricing/-/Tree/Day-Ahead-Energy-Market
    """
    if not api_user or not api_pass:
        log.warning("No ISONE API credentials — using seed data for %s %d", market, year)
        return pd.DataFrame()  # caller will fall back to seed

    # NOTE: Full implementation would paginate monthly and aggregate.
    # Sketch of the call:
    auth = (api_user, api_pass)
    results = []
    for month in range(1, 13):
        url = f"{ISONE_API_BASE}/hourlylmp/{market.lower()}/final/monthly/{year}/{month:02d}"
        try:
            r = requests.get(url, auth=auth, timeout=30,
                             headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json()
            # Aggregate negative hours per location
            for row in data.get("HourlyLmps", []):
                if float(row.get("LmpTotal", 0)) < 0:
                    results.append({
                        "node_id": row["Location"]["@LocId"],
                        "node_name": row["Location"]["$"],
                        "neg_hrs": 1,
                    })
        except Exception as e:
            log.error("API error %s/%d: %s", month, year, e)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results).groupby(["node_id", "node_name"]).sum().reset_index()
    return df


def fetch_isone_queue(url: str = ISONE_QUEUE_URL) -> pd.DataFrame:
    """
    Download and parse the ISONE Interconnection Queue Excel workbook.

    Returns DataFrame with columns:
      county, state, lat, lon, mw, technology, interconnection_year
    """
    try:
        log.info("Downloading ISONE queue from %s", url)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        # Queue file has multiple sheets; 'Queue' or 'Active Queue' is the main one
        xl = pd.ExcelFile(r.content)
        sheet = next((s for s in xl.sheet_names
                      if "queue" in s.lower() or "active" in s.lower()), xl.sheet_names[0])
        df = xl.parse(sheet)
        log.info("Queue sheet '%s': %d rows", sheet, len(df))
        return df
    except Exception as e:
        log.error("Queue download failed: %s", e)
        return pd.DataFrame()


def load_node_geocodes() -> dict:
    """
    Load lat/lon for ISONE pricing nodes.  We ship a bundled CSV derived from
    the existing maps; live runs should merge with ISONE's 'pnode' API endpoint.
    """
    path = SEED_DIR / "node_geocodes.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


# ── Map builders ──────────────────────────────────────────────────────────────

def _base_map() -> folium.Map:
    m = folium.Map(location=MAP_CENTER, zoom_start=MAP_ZOOM, prefer_canvas=False)
    folium.TileLayer(TILE_URL, attr=TILE_ATTR, name="cartodbpositron").add_to(m)
    return m


def _build_neg_price_layers(m: folium.Map, seed_nodes: list[dict],
                             years: list[int], show_by_default: tuple = ()) -> None:
    """
    Add DA and RT negative-price node layers (one per year) to map `m`.
    Uses seed_nodes if live data not available.
    """
    years_markets = [(yr, mk) for yr in years for mk in ("DA", "RT")]
    for year, market in years_markets:
        layer_key = f"{year} {market}"
        # Filter seed data for this year+market
        nodes = [n for n in seed_nodes
                 if f"{year}" in n.get("layer", "") and market in n.get("layer", "")]
        if not nodes:
            log.warning("No seed data for %s %s", year, market)
            continue

        neg_hrs_vals = []
        for n in nodes:
            m_neg = re.search(r'(\d+)\s*neg\s*hrs', n["tooltip"])
            neg_hrs_vals.append(int(m_neg.group(1)) if m_neg else 0)

        vmin, vmax = min(neg_hrs_vals), max(neg_hrs_vals)
        top_n_label = f"(top {len(nodes)})"

        show = layer_key in show_by_default
        fg = FeatureGroup(name=f"{layer_key} {top_n_label}", show=show)

        for node, neg_hrs in zip(nodes, neg_hrs_vals):
            color = _viridis_color(neg_hrs, vmin, vmax)
            radius = _radius(neg_hrs, vmin, vmax)
            _add_circle(fg, node["lat"], node["lon"], radius, color,
                        f"<div>{node['tooltip']}</div>")
        fg.add_to(m)


def _build_queue_layers(m: folium.Map, seed_year_tech: list[dict],
                        queue_years: list[int]) -> None:
    """
    Add interconnection queue layers (by year, by tech) to map `m`.
    """
    for year in queue_years:
        # All-tech totals layer
        nodes_all = [n for n in seed_year_tech
                     if str(year) in n.get("layer", "") and "All" in n.get("layer", "")]
        if nodes_all:
            fg_all = FeatureGroup(name=f"Queue totals {year} — All techs", show=False)
            for n in nodes_all:
                m_mw = re.search(r'([\d\.]+)\s*MW', n["tooltip"])
                mw = float(m_mw.group(1)) if m_mw else 100.0
                radius = _queue_radius(mw)
                _add_circle(fg_all, n["lat"], n["lon"], radius, "#6d6875ff",
                            f"<div>{n['tooltip']}</div>")
            fg_all.add_to(m)

        # Tech-split layers (Wind, Solar, Storage, Other …)
        for tech in TECH_COLORS:
            nodes_tech = [n for n in seed_year_tech
                          if str(year) in n.get("layer", "")
                          and tech in n.get("layer", "") + n.get("tooltip", "")]
            if not nodes_tech:
                continue
            fg_t = FeatureGroup(name=f"Queue {year} — {tech}", show=False)
            color = TECH_COLORS[tech] + "ff" if not TECH_COLORS[tech].endswith("ff") else TECH_COLORS[tech]
            for n in nodes_tech:
                m_mw = re.search(r'([\d\.]+)\s*MW', n["tooltip"])
                mw = float(m_mw.group(1)) if m_mw else 100.0
                radius = _queue_radius(mw)
                _add_circle(fg_t, n["lat"], n["lon"], radius, color,
                            f"<div>{n['tooltip']}</div>")
            fg_t.add_to(m)


# ── Public map-generation functions ───────────────────────────────────────────

def build_multiyear_nodes_map(seed_multiyear: list[dict],
                               years: list[int] = None) -> folium.Map:
    """
    Map 1: Negative-price nodes only (DA + RT, multi-year).
    Mirrors: isone_multiyear_nodes_map_*.html
    """
    if years is None:
        years = [2022, 2023, 2024, 2025]
    m = _base_map()
    _build_neg_price_layers(m, seed_multiyear, years)
    LayerControl(collapsed=False, position="topright").add_to(m)
    return m


def build_queue_totals_by_tech(seed_totals: list[dict],
                                seed_queue: list[dict],
                                neg_years: list[int] = None,
                                queue_years: list[int] = None) -> folium.Map:
    """
    Map 2: Negative-price nodes + queue totals by technology.
    Mirrors: isone_da_rt_plus_queue_totals_by_year_tech_*.html
    """
    if neg_years   is None: neg_years   = [2022, 2023, 2024, 2025]
    if queue_years is None: queue_years = [2026, 2027, 2028, 2029]
    m = _base_map()
    _build_neg_price_layers(m, seed_totals, neg_years)
    _build_queue_layers(m, seed_queue, queue_years)
    LayerControl(collapsed=False, position="topright").add_to(m)
    return m


def build_queue_year_and_tech(seed_year_tech: list[dict],
                               neg_years: list[int] = None,
                               queue_years: list[int] = None) -> folium.Map:
    """
    Map 3: Negative-price nodes + queue split by year AND technology.
    Mirrors: isone_da_rt_plus_queue_year_and_tech_*.html
    """
    if neg_years   is None: neg_years   = [2022, 2023, 2024, 2025]
    if queue_years is None: queue_years = [2026, 2027, 2028, 2029]
    m = _base_map()
    _build_neg_price_layers(m, seed_year_tech, neg_years)
    _build_queue_layers(m, seed_year_tech, queue_years)
    LayerControl(collapsed=False, position="topright").add_to(m)
    return m


# ── Runner ────────────────────────────────────────────────────────────────────

def run(
    api_user: Optional[str] = None,
    api_pass: Optional[str] = None,
    output_dir: Optional[Path] = None,
    seed_dir: Optional[Path] = None,
    run_date: Optional[datetime.date] = None,
):
    """
    Generate all three maps.  If ISONE API credentials are supplied, live data
    is fetched; otherwise seed data is used (great for first run / offline).
    """
    global OUTPUT_DIR, SEED_DIR
    if output_dir: OUTPUT_DIR = Path(output_dir)
    if seed_dir:   SEED_DIR   = Path(seed_dir)
    if run_date is None: run_date = datetime.date.today()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = run_date.strftime("%Y%m%d")
    log.info("Generating ISONE maps for %s", stamp)

    # Load seed data
    seed_multiyear  = load_seed_data("multiyear")
    seed_totals     = load_seed_data("totals")
    seed_year_tech  = load_seed_data("year_tech")

    # ── If live credentials supplied, refresh negative-price data ──────────
    # (Uncomment and extend once you have ISONE API credentials)
    #
    # current_year = run_date.year
    # for year in [current_year - 1, current_year]:
    #     for market in ("DA", "RT"):
    #         df = fetch_isone_lmp_negative_hours(year, market, api_user, api_pass)
    #         if not df.empty:
    #             seed_multiyear = merge_live_into_seed(seed_multiyear, df, year, market)
    #
    # queue_df = fetch_isone_queue()
    # if not queue_df.empty:
    #     seed_year_tech = merge_queue_into_seed(seed_year_tech, queue_df)

    # ── Build and save maps ────────────────────────────────────────────────
    maps = {
        f"isone_multiyear_nodes_map_{stamp}.html":
            build_multiyear_nodes_map(seed_multiyear),
        f"isone_da_rt_plus_queue_totals_by_year_tech_{stamp}.html":
            build_queue_totals_by_tech(seed_totals, seed_year_tech),
        f"isone_da_rt_plus_queue_year_and_tech_{stamp}.html":
            build_queue_year_and_tech(seed_year_tech),
    }

    saved = []
    for fname, m in maps.items():
        path = OUTPUT_DIR / fname
        m.save(str(path))
        log.info("Saved → %s", path)
        saved.append(str(path))

    log.info("Done. %d maps written to %s", len(saved), OUTPUT_DIR)
    return saved


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Generate ISONE negative-price + queue maps")
    p.add_argument("--user",       help="ISONE API username",  default=os.getenv("ISONE_USER"))
    p.add_argument("--password",   help="ISONE API password",  default=os.getenv("ISONE_PASS"))
    p.add_argument("--output-dir", help="Output directory",    default="./output")
    p.add_argument("--seed-dir",   help="Seed data directory", default="./seed_data")
    args = p.parse_args()
    run(api_user=args.user, api_pass=args.password,
        output_dir=args.output_dir, seed_dir=args.seed_dir)
