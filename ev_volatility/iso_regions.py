"""
iso_regions.py
==============
Geographic + market reference data shared across the EV-charger volatility pipeline.

Provides:
  • Bounding boxes for NYISO and ISO-NE (used to filter PlugShare stations to the
    two markets we care about).
  • Load-zone / pricing-node geocodes so every charger site can be snapped to the
    nearest priced location.
  • Charger classification helpers (DC fast vs. Level-2, "large" DCFC threshold).
  • A tiny great-circle nearest-node matcher (no external geo dependency).

Everything here is pure Python + a JSON geocode file, so it runs offline.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Optional

SEED_DIR = Path(__file__).parent / "seed_data"

# ── Market bounding boxes ──────────────────────────────────────────────────────
# (min_lat, min_lon, max_lat, max_lon).  Deliberately generous; the nearest-zone
# match is what ultimately assigns a station to a market, the bbox is a coarse
# pre-filter to throw away obviously out-of-region PlugShare pins.
BBOX = {
    "NYISO": (40.45, -79.85, 45.05, -71.80),   # NY state
    "ISONE": (40.95, -73.75, 47.50, -66.90),   # 6 New England states
}

# A "large" fast-charging site: the user's threshold is >4 DC fast ports.
LARGE_DCFC_MIN_PORTS = 5   # strictly greater than 4

# PlugShare / OCPI connector-power heuristics
DCFC_MIN_KW = 50.0         # >=50 kW is treated as DC fast charging


# ── Load-zone geocodes ─────────────────────────────────────────────────────────
# NYISO has 11 load zones; ISO-NE has 8.  We anchor each to its representative
# metro so charger counts can be aggregated to a priced location even when we
# don't have a specific pnode.  ISO-NE *nodes* (the 135 pnodes shipped with the
# isone_maps package) are layered on top of these zones by the ranking module.

NYISO_ZONES = {
    "WEST":   (42.8864, -78.8784),   # Buffalo
    "GENESE": (43.1610, -77.6109),   # Rochester
    "CENTRL": (43.0481, -76.1474),   # Syracuse
    "NORTH":  (44.6995, -73.4529),   # Plattsburgh
    "MHK VL": (43.1009, -75.2327),   # Utica (Mohawk Valley)
    "CAPITL": (42.6526, -73.7562),   # Albany (Capital)
    "HUD VL": (41.7004, -73.9210),   # Poughkeepsie (Hudson Valley)
    "MILLWD": (41.2045, -73.8290),   # Millwood
    "DUNWOD": (40.9312, -73.8988),   # Dunwoodie (Yonkers)
    "N.Y.C.": (40.7128, -74.0060),   # New York City
    "LONGIL": (40.7891, -73.1350),   # Long Island (Islip)
}

ISONE_ZONES = {
    "ME":   (44.6939, -69.3819),   # Maine
    "NH":   (43.1939, -71.5724),   # New Hampshire
    "VT":   (44.0459, -72.7107),   # Vermont
    "CT":   (41.6032, -73.0877),   # Connecticut
    "RI":   (41.5801, -71.4774),   # Rhode Island
    "SEMA": (41.7601, -70.9494),   # Southeast Mass.
    "WCMA": (42.3601, -72.5898),   # West-Central Mass.
    "NEMA": (42.3601, -71.0589),   # Northeast Mass. / Boston
}


def zone_table(iso: str) -> dict[str, tuple[float, float]]:
    return NYISO_ZONES if iso.upper() == "NYISO" else ISONE_ZONES


# ── ISO-NE pnode geocodes (loaded from the isone_maps package if present) ───────

def load_isone_pnodes() -> dict[str, tuple[float, float]]:
    """
    Pull the 135 ISO-NE pnode lat/lons the sibling isone_maps package already
    ships.  Falls back to an empty dict if that package isn't present.
    """
    candidates = [
        Path(__file__).parent.parent / "isone_maps" / "seed_data" / "node_geocodes.json",
        SEED_DIR / "isone_node_geocodes.json",
    ]
    for path in candidates:
        if path.exists():
            with open(path) as f:
                raw = json.load(f)
            return {k: (v[0], v[1]) for k, v in raw.items()}
    return {}


# ── Geo helpers ────────────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


def in_bbox(lat: float, lon: float, iso: str) -> bool:
    mn_lat, mn_lon, mx_lat, mx_lon = BBOX[iso.upper()]
    return mn_lat <= lat <= mx_lat and mn_lon <= lon <= mx_lon


def which_iso(lat: float, lon: float) -> Optional[str]:
    """Return 'NYISO', 'ISONE', or None for a coordinate."""
    hits = [iso for iso in ("NYISO", "ISONE") if in_bbox(lat, lon, iso)]
    if not hits:
        return None
    if len(hits) == 1:
        return hits[0]
    # Overlap zone (SW New England / lower Hudson) — assign to the closer zone set.
    best_iso, best_d = None, float("inf")
    for iso in hits:
        _, d = nearest_zone(lat, lon, iso)
        if d < best_d:
            best_iso, best_d = iso, d
    return best_iso


def nearest_zone(lat: float, lon: float, iso: str) -> tuple[str, float]:
    """Nearest load zone name + distance (km) within an ISO."""
    best_name, best_d = None, float("inf")
    for name, (zlat, zlon) in zone_table(iso).items():
        d = haversine_km(lat, lon, zlat, zlon)
        if d < best_d:
            best_name, best_d = name, d
    return best_name, best_d


def nearest_pnode(lat: float, lon: float,
                  pnodes: dict[str, tuple[float, float]]) -> tuple[Optional[str], float]:
    """Nearest ISO-NE pnode name + distance (km); ('', inf) if none supplied."""
    best_name, best_d = None, float("inf")
    for name, (plat, plon) in pnodes.items():
        d = haversine_km(lat, lon, plat, plon)
        if d < best_d:
            best_name, best_d = name, d
    return best_name, best_d


# ── Charger classification ─────────────────────────────────────────────────────

# PlugShare connector type ids that are DC fast (CCS/CHAdeMO/Tesla DC).
DCFC_CONNECTOR_IDS = {2, 3, 4, 5, 6, 7, 8, 42}  # superset; matched loosely below
DCFC_KEYWORDS = ("ccs", "chademo", "combo", "supercharger", "dc fast", "dcfc")


def is_dcfc(connector_name: str = "", power_kw: float = 0.0) -> bool:
    s = (connector_name or "").lower()
    if any(k in s for k in DCFC_KEYWORDS):
        return True
    return power_kw >= DCFC_MIN_KW


def classify_station(dcfc_ports: int) -> str:
    """Bucket a station by DC-fast port count for the ranking legend."""
    if dcfc_ports >= LARGE_DCFC_MIN_PORTS:
        return "Large DCFC (5+)"
    if dcfc_ports >= 1:
        return "Small DCFC (1-4)"
    return "L2 only"
