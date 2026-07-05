"""
nrel_stations.py
===============
Pull EV charging stations from the NREL Alternative Fuel Stations API and shape
them for the volatility pipeline.

    https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/

Why NREL over PlugShare: every station carries an ``open_date`` and an explicit
``ev_dc_fast_num`` port count, so we can build a *real* dated event list for the
backtest (last-N-years of large DCFC openings) instead of a representative one,
and we can pull *planned* sites (``status_code == 'P'``) for the forward ranking.

Auth: free NREL key. Stored git-ignored in ev_volatility/.env as NREL_API_KEY
(also read from the OS environment). Get one at https://developer.nrel.gov/signup/

Network note: some sandboxes block developer.nrel.gov at the egress policy. When
the host is unreachable or no key is set, callers fall back to the offline seed
set, and this module can also parse a saved NREL JSON response via ``from_payload``
so the parsing / event-building path is testable without network access.

Normalized station schema (matches plugshare_scraper output):
    id, name, lat, lon, iso, zone, state, status,
    dcfc_ports, l2_ports, max_kw, open_date, planned_online, ev_network, source
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

import requests

from iso_regions import (LARGE_DCFC_MIN_PORTS, SEED_DIR, nearest_zone, which_iso)

log = logging.getLogger("nrel")

NREL_URL = "https://developer.nrel.gov/api/alt-fuel-stations/v1.json"
CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

# States covering NYISO (NY) and ISO-NE (the six New England states).
NYISO_STATES = ["NY"]
ISONE_STATES = ["ME", "NH", "VT", "MA", "CT", "RI"]
ALL_STATES = NYISO_STATES + ISONE_STATES

# NREL status codes: E=Available, P=Planned, T=Temporarily unavailable.
STATUS_MAP = {"E": "operational", "P": "planned", "T": "temporarily_unavailable"}

# Rough DC-fast power by connector for a max_kw estimate when NREL omits it.
_CONNECTOR_KW = {"TESLA": 250, "CHADEMO": 62.5, "J1772COMBO": 150, "NEMA": 7.7}


# ── key loading ─────────────────────────────────────────────────────────────────

def _load_dotenv() -> None:
    """Minimal .env loader (no dependency) — populates os.environ if unset."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def api_key() -> str | None:
    _load_dotenv()
    return os.environ.get("NREL_API_KEY")


# ── parsing ─────────────────────────────────────────────────────────────────────

def _est_max_kw(raw: dict) -> float:
    types = raw.get("ev_connector_types") or []
    kws = [_CONNECTOR_KW.get(str(t).upper(), 0) for t in types]
    if raw.get("ev_dc_fast_num"):
        kws.append(150)          # assume >=150 kW where DC fast ports exist
    return float(max(kws) if kws else 0)


def _parse_open_date(raw: dict) -> str:
    d = raw.get("open_date")
    if not d:
        return ""
    try:
        return datetime.strptime(d[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def normalize(raw: dict) -> dict | None:
    """One NREL fuel_station record → normalized station dict (None if off-grid)."""
    lat, lon = raw.get("latitude"), raw.get("longitude")
    if lat is None or lon is None:
        return None
    iso = which_iso(lat, lon)
    if iso is None:
        return None

    dcfc = int(raw.get("ev_dc_fast_num") or 0)
    l2 = int(raw.get("ev_level2_evse_num") or 0)
    open_date = _parse_open_date(raw)
    status = STATUS_MAP.get(raw.get("status_code", ""), "operational")
    zone, _ = nearest_zone(lat, lon, iso)

    return {
        "id": f"nrel-{raw.get('id')}",
        "name": raw.get("station_name") or "NREL station",
        "lat": lat, "lon": lon,
        "iso": iso, "zone": zone,
        "state": raw.get("state", ""),
        "status": status,
        "dcfc_ports": dcfc,
        "l2_ports": l2,
        "max_kw": _est_max_kw(raw),
        "open_date": open_date,
        "planned_online": open_date if status == "planned" else "",
        "ev_network": raw.get("ev_network", ""),
        "source": "nrel",
    }


def from_payload(payload: dict) -> list[dict]:
    """Parse a raw NREL API JSON payload → list of normalized stations."""
    out = []
    for raw in payload.get("fuel_stations", []):
        st = normalize(raw)
        if st:
            out.append(st)
    return out


# CSV export column → API field name (so we can reuse normalize()).
_CSV_TO_API = {
    "Station Name": "station_name", "State": "state", "Status Code": "status_code",
    "EV Level2 EVSE Num": "ev_level2_evse_num", "EV DC Fast Count": "ev_dc_fast_num",
    "EV Network": "ev_network", "Latitude": "latitude", "Longitude": "longitude",
    "ID": "id", "Open Date": "open_date", "Expected Date": "estimated_open",
    "EV Connector Types": "ev_connector_types",
}


def from_csv(path: str | Path, states: list[str] | None = None) -> list[dict]:
    """
    Parse an official NREL "Alternative Fuel Stations" CSV export → normalized
    stations. This is the download-and-point-at-it path (no live API needed):
    the same export you'd get from https://afdc.energy.gov/stations or the API's
    CSV format. Filters to ELEC + the NYISO/ISO-NE states.
    """
    import pandas as pd
    states = states or ALL_STATES
    df = pd.read_csv(path, low_memory=False)
    df = df[(df["Fuel Type Code"] == "ELEC") & (df["State"].isin(states))]

    out = []
    for _, row in df.iterrows():
        raw = {}
        for csv_col, api_key_ in _CSV_TO_API.items():
            val = row.get(csv_col)
            if pd.isna(val):
                continue
            if api_key_ == "ev_connector_types":
                raw[api_key_] = [c.strip() for c in str(val).split()]
            elif api_key_ in ("ev_level2_evse_num", "ev_dc_fast_num"):
                raw[api_key_] = int(val)
            else:
                raw[api_key_] = val
        st = normalize(raw)
        if st:
            out.append(st)
    log.info("NREL CSV: %d ELEC stations in %s", len(out), states)
    return out


# ── fetching ────────────────────────────────────────────────────────────────────

def fetch(states: list[str] | None = None, use_cache: bool = True,
          timeout: int = 60) -> list[dict]:
    """
    Fetch ELEC stations (all statuses incl. planned) for the given states and
    return normalized stations. Raises if no key; logs+returns [] on network
    failure so callers can fall back to seed.
    """
    key = api_key()
    if not key:
        raise RuntimeError(
            "NREL_API_KEY not set — add it to ev_volatility/.env or the environment."
        )
    states = states or ALL_STATES
    cache = CACHE_DIR / f"nrel_elec_{'_'.join(states)}.json"
    if use_cache and cache.exists():
        payload = json.loads(cache.read_text())
        log.info("NREL: loaded cached payload (%d stations)",
                 len(payload.get("fuel_stations", [])))
        return from_payload(payload)

    params = {
        "api_key": key,
        "fuel_type": "ELEC",
        "state": ",".join(states),
        "status": "all",           # include Planned (P) + Temp-unavailable (T)
        "access": "public",
        "limit": "all",
    }
    try:
        r = requests.get(NREL_URL, params=params, timeout=timeout,
                         headers={"Accept": "application/json"})
        r.raise_for_status()
        payload = r.json()
    except Exception as e:
        log.error("NREL fetch failed (%s) — host may be egress-blocked", e)
        return []
    cache.write_text(json.dumps(payload))
    n = len(payload.get("fuel_stations", []))
    log.info("NREL: fetched %d ELEC stations for %s", n, states)
    return from_payload(payload)


# ── views: backtest events vs. ranking stations ─────────────────────────────────

def historical_openings(stations: list[dict], years: int = 3,
                         min_dcfc: int = LARGE_DCFC_MIN_PORTS,
                         as_of: date | None = None) -> list[dict]:
    """
    Large (> ``min_dcfc``-1 port) DCFC sites that OPENED within the last `years`
    → dated backtest events: {name, iso, zone, dcfc_ports, max_kw, online:'YYYY-MM'}.
    """
    as_of = as_of or date.today()
    cutoff = date(as_of.year - years, as_of.month, 1)
    events = []
    for s in stations:
        if s["dcfc_ports"] < min_dcfc or not s.get("open_date"):
            continue
        try:
            od = datetime.strptime(s["open_date"], "%Y-%m-%d").date()
        except ValueError:
            continue
        if not (cutoff <= od <= as_of):
            continue
        events.append({
            "name": s["name"], "iso": s["iso"], "zone": s["zone"],
            "dcfc_ports": s["dcfc_ports"], "max_kw": s["max_kw"],
            "open_date": s["open_date"], "online": od.strftime("%Y-%m"),
            "ev_network": s.get("ev_network", ""),
        })
    events.sort(key=lambda e: e["online"])
    log.info("NREL: %d large DCFC openings in last %dy", len(events), years)
    return events


def planned_and_recent(stations: list[dict]) -> list[dict]:
    """Stations for the forward ranking: planned + operational with any charging."""
    return [s for s in stations if s["dcfc_ports"] + s["l2_ports"] > 0]


# ── unified loader ──────────────────────────────────────────────────────────────

def load_stations(prefer_live: bool = True) -> list[dict]:
    """
    Station source priority:
      1. NREL CSV export at $NREL_CSV or .cache/nrel_export.csv  (real, offline)
      2. Live NREL API (needs key + reachable host)
      3. Offline seed set (synthetic fallback)
    """
    csv_path = os.environ.get("NREL_CSV")
    if not csv_path:
        default = CACHE_DIR / "nrel_export.csv"
        csv_path = str(default) if default.exists() else None
    if csv_path and Path(csv_path).exists():
        try:
            stations = from_csv(csv_path)
            if stations:
                return stations
        except Exception as e:
            log.error("NREL CSV parse failed (%s) — trying API/seed", e)

    if prefer_live and api_key():
        try:
            live = fetch()
            if live:
                return live
            log.warning("NREL returned nothing / unreachable — using seed set")
        except Exception as e:
            log.error("NREL load failed (%s) — using seed set", e)
    from plugshare_scraper import load_seed_stations
    return load_seed_stations()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    st = load_stations()
    ev = historical_openings(st)
    by_iso = {}
    for s in st:
        by_iso[s["iso"]] = by_iso.get(s["iso"], 0) + 1
    print(f"{len(st)} stations {by_iso}; {len(ev)} large DCFC openings (last 3y)")
