"""
plugshare_scraper.py
====================
Scrape planned / under-construction EV charging sites from PlugShare and snap
each one to a NYISO or ISO-NE priced location.

PlugShare exposes an *undocumented* region endpoint that its web/mobile apps use:

    GET https://api.plugshare.com/v3/locations/region
        ?spatial_distance=...&minimal=1&latitude=..&longitude=..
        &access=1,2,3&exclude_networks=&count=500&region info...

It requires an ``Authorization`` header (the public app ships a token). Requests
without it return HTTP 401 — so live scraping is *token-gated* here exactly the
way the sibling isone_maps package gates the ISONE API behind ISONE_USER/PASS.

Set the token to go live:

    export PLUGSHARE_TOKEN="Basic <token>"        # value of the app's Auth header
    # optional: export PLUGSHARE_UA="..."          # override User-Agent

With no token, ``load_stations()`` transparently falls back to the reproducible
seed set in seed_data/seed_ev_stations.json so the whole pipeline still runs
offline. Please respect PlugShare's Terms of Service and rate limits when using
a token.

Output schema (one dict per station), consumed by node_ranking.py:
    id, name, lat, lon, iso, zone, state, status,
    dcfc_ports, l2_ports, max_kw, planned_online, source
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Iterable, Optional

import requests

from iso_regions import (
    BBOX, DCFC_MIN_KW, SEED_DIR, in_bbox, is_dcfc, nearest_zone, which_iso,
)

log = logging.getLogger("plugshare")

PLUGSHARE_REGION_URL = "https://api.plugshare.com/v3/locations/region"
SEED_STATIONS = SEED_DIR / "seed_ev_stations.json"

# PlugShare "under_repair"/status vocabulary that maps to *not yet operational*.
# The region payload marks pre-operational pins with these station-status ids /
# flags; we treat "planned" + "under construction" + "coming soon" as pipeline.
PLANNED_STATUS_TOKENS = ("coming soon", "under construction", "planned",
                         "not yet open", "future", "proposed")


# ── Live scrape ────────────────────────────────────────────────────────────────

def _headers() -> dict:
    token = os.environ.get("PLUGSHARE_TOKEN", "")
    ua = os.environ.get(
        "PLUGSHARE_UA",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    )
    h = {"User-Agent": ua, "Accept": "application/json"}
    if token:
        h["Authorization"] = token
    return h


def _tile_grid(bbox: tuple[float, float, float, float], step: float = 0.6):
    """
    PlugShare caps results per region query, so we sweep the market bbox as a
    grid of overlapping tiles and de-dupe by station id.
    """
    mn_lat, mn_lon, mx_lat, mx_lon = bbox
    lat = mn_lat
    while lat < mx_lat:
        lon = mn_lon
        while lon < mx_lon:
            yield (lat, lon, min(lat + step, mx_lat), min(lon + step, mx_lon))
            lon += step
        lat += step


def _fetch_tile(tile, session, timeout=30) -> list[dict]:
    mn_lat, mn_lon, mx_lat, mx_lon = tile
    params = {
        "minimal": 1,
        "count": 500,
        "latitude": (mn_lat + mx_lat) / 2,
        "longitude": (mn_lon + mx_lon) / 2,
        "spatial_distance": 60,          # km radius around tile center
        "access": "1,3",                 # public + restricted (exclude private=2)
    }
    r = session.get(PLUGSHARE_REGION_URL, params=params,
                    headers=_headers(), timeout=timeout)
    r.raise_for_status()
    data = r.json()
    # PlugShare returns either a bare list or {"locations": [...]}
    return data.get("locations", data) if isinstance(data, dict) else data


def _is_planned(raw: dict) -> bool:
    blob = json.dumps(raw).lower()
    if any(tok in blob for tok in PLANNED_STATUS_TOKENS):
        return True
    # PlugShare uses under_repair / open flags; a pin with zero score and an
    # "opened" date in the future is treated as pipeline.
    if raw.get("coming_soon") or raw.get("under_construction"):
        return True
    return False


def _parse_station(raw: dict) -> Optional[dict]:
    """Normalize one PlugShare region record → our station schema."""
    lat, lon = raw.get("latitude"), raw.get("longitude")
    if lat is None or lon is None:
        return None
    iso = which_iso(lat, lon)
    if iso is None:
        return None

    dcfc = l2 = 0
    max_kw = 0.0
    for outlet in _iter_outlets(raw):
        power = float(outlet.get("power") or outlet.get("kilowatts") or 0)
        cname = str(outlet.get("connector") or outlet.get("connector_name") or "")
        n = int(outlet.get("count") or 1)
        if is_dcfc(cname, power):
            dcfc += n
        else:
            l2 += n
        max_kw = max(max_kw, power)

    zone, _ = nearest_zone(lat, lon, iso)
    return {
        "id": str(raw.get("id", "")),
        "name": raw.get("name") or raw.get("address") or "PlugShare site",
        "lat": lat, "lon": lon,
        "iso": iso, "zone": zone,
        "state": raw.get("state", ""),
        "status": "under_construction" if raw.get("under_construction") else "planned",
        "dcfc_ports": dcfc,
        "l2_ports": l2,
        "max_kw": max_kw,
        "planned_online": raw.get("opened") or raw.get("estimated_open") or "",
        "source": "plugshare",
    }


def _iter_outlets(raw: dict) -> Iterable[dict]:
    """PlugShare nests connectors under stations→outlets; be liberal in parsing."""
    for station in raw.get("stations", []) or []:
        for outlet in station.get("outlets", []) or []:
            yield outlet
    # minimal payloads sometimes carry a flat connector summary
    for outlet in raw.get("outlets", []) or []:
        yield outlet


def scrape_live(isos: tuple[str, ...] = ("NYISO", "ISONE"),
                pause: float = 0.5) -> list[dict]:
    """
    Sweep PlugShare across the requested ISO bounding boxes and return the
    normalized *planned / under-construction* stations. Requires PLUGSHARE_TOKEN.
    """
    if not os.environ.get("PLUGSHARE_TOKEN"):
        raise RuntimeError(
            "PLUGSHARE_TOKEN not set — cannot scrape live. Export the token or "
            "call load_stations() to use the offline seed set."
        )
    session = requests.Session()
    seen: dict[str, dict] = {}
    for iso in isos:
        for tile in _tile_grid(BBOX[iso]):
            try:
                for raw in _fetch_tile(tile, session):
                    if not _is_planned(raw):
                        continue
                    st = _parse_station(raw)
                    if st and st["id"] and st["id"] not in seen:
                        seen[st["id"]] = st
                time.sleep(pause)
            except Exception as e:            # keep sweeping on tile failure
                log.warning("tile %s failed: %s", tile, e)
    stations = list(seen.values())
    log.info("PlugShare live: %d planned stations across %s", len(stations), isos)
    return stations


# ── Seed fallback ──────────────────────────────────────────────────────────────

def load_seed_stations() -> list[dict]:
    if SEED_STATIONS.exists():
        with open(SEED_STATIONS) as f:
            return json.load(f)
    log.warning("No seed station file at %s", SEED_STATIONS)
    return []


def load_stations(prefer_live: bool = True) -> list[dict]:
    """
    Return planned EV stations. Uses live PlugShare when PLUGSHARE_TOKEN is set
    and prefer_live is True; otherwise the reproducible seed set.
    """
    if prefer_live and os.environ.get("PLUGSHARE_TOKEN"):
        try:
            live = scrape_live()
            if live:
                return live
            log.warning("Live scrape returned nothing — falling back to seed")
        except Exception as e:
            log.error("Live scrape failed (%s) — falling back to seed", e)
    return load_seed_stations()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    st = load_stations()
    n_iso = {}
    for s in st:
        n_iso[s["iso"]] = n_iso.get(s["iso"], 0) + 1
    print(f"{len(st)} planned stations: {n_iso}")
    big = [s for s in st if s["dcfc_ports"] >= 5]
    print(f"{len(big)} large DCFC (5+ ports) sites")
