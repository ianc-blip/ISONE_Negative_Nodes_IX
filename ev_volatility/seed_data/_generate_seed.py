"""
_generate_seed.py
=================
Deterministically (fixed RNG seed) build the two offline seed files used when
no live PlugShare token is configured:

  • seed_ev_stations.json  — representative *planned* / *under-construction* EV
    charging sites spread across NYISO and ISO-NE load zones.  Coordinates are
    jittered around zone metros; port counts / networks are plausible but
    SYNTHETIC.  Real runs replace this via plugshare_scraper.scrape_live().

  • seed_charger_events.json — a curated set of large (>4-port) DC-fast sites in
    NYISO used to drive the volatility backtest.  Locations map to real NYISO
    zones and the `online` dates fall inside the window where NYISO publishes
    public LBMP data, so the volatility numbers the backtest computes around them
    are REAL even though the exact site/date pairing is approximate.

Run:  python seed_data/_generate_seed.py
"""

import json
import random
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from iso_regions import NYISO_ZONES, ISONE_ZONES  # noqa: E402

HERE = Path(__file__).parent
RNG = random.Random(20260705)   # fixed seed → reproducible

NETWORKS = ["Tesla Supercharger", "Electrify America", "EVgo",
            "ChargePoint", "Francis Energy", "Red E", "Flo", "Shell Recharge"]
STATUSES = ["planned", "under_construction"]


def _jit(v, amt=0.18):
    return round(v + RNG.uniform(-amt, amt), 6)


def _station(idx, iso, zone, lat, lon):
    # Bias toward larger sites in dense zones so the ranking has structure.
    big = RNG.random() < 0.45
    dcfc = RNG.choice([6, 8, 8, 10, 12, 16, 20]) if big else RNG.choice([0, 2, 2, 4])
    l2 = RNG.choice([0, 0, 2, 4, 6]) if dcfc < 5 else RNG.choice([0, 2, 4])
    kw = RNG.choice([150, 250, 350]) if dcfc >= 5 else RNG.choice([50, 62.5, 150])
    yr = RNG.choice([2025, 2025, 2026, 2026, 2027])
    mo = RNG.randint(1, 12)
    return {
        "id": f"seed-{iso.lower()}-{idx:03d}",
        "name": f"{RNG.choice(NETWORKS)} — {zone} #{idx}",
        "lat": _jit(lat), "lon": _jit(lon),
        "iso": iso, "zone": zone,
        "state": "NY" if iso == "NYISO" else "NE",
        "status": RNG.choice(STATUSES),
        "dcfc_ports": dcfc,
        "l2_ports": l2,
        "max_kw": kw,
        "planned_online": f"{yr}-{mo:02d}",
        "source": "seed",
    }


def build_stations():
    out, idx = [], 0
    # ~3-5 sites per zone
    for iso, zones in (("NYISO", NYISO_ZONES), ("ISONE", ISONE_ZONES)):
        for zone, (lat, lon) in zones.items():
            for _ in range(RNG.randint(3, 5)):
                idx += 1
                out.append(_station(idx, iso, zone, lat, lon))
    # Keep only sites with at least some charging (drop pure-empty)
    out = [s for s in out if s["dcfc_ports"] + s["l2_ports"] > 0]
    return out


# Curated large-DCFC events for the backtest. Zones are real NYISO zones; the
# `online` month is the energization month the event study centers on. These are
# representative corridor build-outs (approximate) — the LMP volatility measured
# around them uses genuine NYISO data.
BACKTEST_EVENTS = [
    {"name": "I-90 Corridor DCFC hub (Buffalo)",    "iso": "NYISO", "zone": "WEST",   "dcfc_ports": 12, "max_kw": 350, "online": "2023-06"},
    {"name": "Thruway plaza DCFC (Syracuse)",       "iso": "NYISO", "zone": "CENTRL", "dcfc_ports": 8,  "max_kw": 250, "online": "2023-09"},
    {"name": "Capital Region megasite (Albany)",    "iso": "NYISO", "zone": "CAPITL", "dcfc_ports": 16, "max_kw": 350, "online": "2023-11"},
    {"name": "Hudson Valley DCFC (Poughkeepsie)",   "iso": "NYISO", "zone": "HUD VL", "dcfc_ports": 8,  "max_kw": 250, "online": "2024-03"},
    {"name": "LI Expressway DCFC (Islip)",          "iso": "NYISO", "zone": "LONGIL", "dcfc_ports": 10, "max_kw": 250, "online": "2024-05"},
    {"name": "NYC curbside fast-charge (Manhattan)","iso": "NYISO", "zone": "N.Y.C.", "dcfc_ports": 20, "max_kw": 250, "online": "2024-07"},
    {"name": "Rochester retail DCFC",               "iso": "NYISO", "zone": "GENESE", "dcfc_ports": 8,  "max_kw": 350, "online": "2023-08"},
    {"name": "Utica corridor DCFC",                 "iso": "NYISO", "zone": "MHK VL", "dcfc_ports": 6,  "max_kw": 150, "online": "2024-02"},
    # ISO-NE large-DCFC openings (representative; clustered to bound downloads).
    {"name": "Boston Seaport Supercharger",         "iso": "ISONE", "zone": "NEMA",   "dcfc_ports": 20, "max_kw": 250, "online": "2024-01"},
    {"name": "Hartford CT retail DCFC",             "iso": "ISONE", "zone": "CT",     "dcfc_ports": 8,  "max_kw": 250, "online": "2023-12"},
    {"name": "Providence RI corridor DCFC",         "iso": "ISONE", "zone": "RI",     "dcfc_ports": 10, "max_kw": 350, "online": "2024-02"},
    {"name": "Worcester MA DCFC hub",               "iso": "ISONE", "zone": "WCMA",   "dcfc_ports": 8,  "max_kw": 150, "online": "2023-11"},
]


def main():
    stations = build_stations()
    with open(HERE / "seed_ev_stations.json", "w") as f:
        json.dump(stations, f, indent=2)
    with open(HERE / "seed_charger_events.json", "w") as f:
        json.dump(BACKTEST_EVENTS, f, indent=2)
    print(f"Wrote {len(stations)} seed stations, {len(BACKTEST_EVENTS)} backtest events")


if __name__ == "__main__":
    main()
