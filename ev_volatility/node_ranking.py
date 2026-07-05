"""
node_ranking.py
==============
Rank NYISO and ISO-NE priced locations by how much *planned* EV fast-charging
infrastructure is pipelined near them, and project a volatility impact using the
backtest calibration.

Granularity:
  • NYISO  — 11 load zones (the level at which NYISO publishes zonal LBMP).
  • ISO-NE — 8 load zones, plus nearest pnode (from the isone_maps package) so
             results line up with the negative-price node maps in that package.

For each location we aggregate:
  n_sites, dcfc_ports, l2_ports, large_sites (>4 DCFC ports), planned_kw
and rank primarily by planned DC-fast ports.

If a backtest report is supplied, we multiply each zone's large-site count by the
mean per-site DiD volatility change to get a projected volatility uplift — the
bridge from "where is the infrastructure" to "where will volatility move".
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path

import folium
from folium import CircleMarker, FeatureGroup, LayerControl

from iso_regions import (LARGE_DCFC_MIN_PORTS, load_isone_pnodes, nearest_pnode,
                         zone_table)

log = logging.getLogger("ranking")


def rank_nodes(stations: list[dict]) -> list[dict]:
    """Aggregate planned stations to (iso, zone) and return a ranked list."""
    pnodes = load_isone_pnodes()
    agg: dict[tuple, dict] = defaultdict(lambda: {
        "n_sites": 0, "dcfc_ports": 0, "l2_ports": 0,
        "large_sites": 0, "planned_kw": 0.0,
    })

    for s in stations:
        iso, zone = s["iso"], s["zone"]
        a = agg[(iso, zone)]
        a["n_sites"] += 1
        a["dcfc_ports"] += s.get("dcfc_ports", 0)
        a["l2_ports"] += s.get("l2_ports", 0)
        a["planned_kw"] += s.get("dcfc_ports", 0) * s.get("max_kw", 0)
        if s.get("dcfc_ports", 0) >= LARGE_DCFC_MIN_PORTS:
            a["large_sites"] += 1

    rows = []
    for (iso, zone), a in agg.items():
        zlat, zlon = zone_table(iso).get(zone, (None, None))
        row = {"iso": iso, "zone": zone, "lat": zlat, "lon": zlon, **a}
        if iso == "ISONE" and pnodes and zlat is not None:
            pn, dist = nearest_pnode(zlat, zlon, pnodes)
            row["nearest_pnode"] = pn
            row["pnode_dist_km"] = round(dist, 1)
        rows.append(row)

    rows.sort(key=lambda r: (r["dcfc_ports"], r["large_sites"]), reverse=True)
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows


def apply_calibration(rows: list[dict], backtest: dict | None) -> list[dict]:
    """
    Attach a projected volatility uplift to each zone using the backtest's mean
    per-large-site DiD on hourly LBMP volatility. Diminishing returns are applied
    (sqrt of large-site count) since sites in the same zone partly overlap.
    """
    if not backtest:
        return rows
    per_site = (backtest.get("aggregate", {})
                        .get("hourly_std", {})
                        .get("mean_did_pct"))
    if per_site is None:
        return rows
    for r in rows:
        n = r["large_sites"]
        r["proj_vol_uplift_pct"] = round(per_site * (n ** 0.5), 1) if n else 0.0
        r["calibration_per_site_pct"] = round(per_site, 1)
    return rows


def format_ranking(rows: list[dict], top: int = 15) -> str:
    lines = ["=" * 84,
             "PLANNED EV FAST-CHARGING BY PRICED LOCATION (NYISO + ISO-NE)",
             "=" * 84,
             f"  {'#':>2s} {'ISO':6s} {'Zone':7s} {'Sites':>5s} {'DCFC':>5s} "
             f"{'Large':>5s} {'MW':>6s} {'ProjVol':>8s}  Nearest pnode"]
    for r in rows[:top]:
        mw = r["planned_kw"] / 1000.0
        proj = r.get("proj_vol_uplift_pct")
        proj_s = f"{proj:+.1f}%" if proj else "   —"
        pn = r.get("nearest_pnode", "")
        lines.append(f"  {r['rank']:2d} {r['iso']:6s} {r['zone']:7s} "
                     f"{r['n_sites']:5d} {r['dcfc_ports']:5d} {r['large_sites']:5d} "
                     f"{mw:6.1f} {proj_s:>8s}  {pn}")
    lines.append("=" * 84)
    return "\n".join(lines)


# ── Map ─────────────────────────────────────────────────────────────────────────

ISO_COLOR = {"NYISO": "#1f77b4", "ISONE": "#d62728"}
MAP_CENTER = [42.6, -73.5]
TILE_URL = "https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png"
TILE_ATTR = ("&copy; OpenStreetMap contributors &copy; CartoDB")


def _radius(dcfc_ports: int, r_min=5, r_max=26, cap=120) -> float:
    return r_min + min(dcfc_ports / cap, 1.0) * (r_max - r_min)


def build_map(stations: list[dict], ranked: list[dict],
              backtest: dict | None = None) -> folium.Map:
    m = folium.Map(location=MAP_CENTER, zoom_start=6, tiles=None)
    folium.TileLayer(TILE_URL, attr=TILE_ATTR, name="CartoDB Positron").add_to(m)

    # Individual planned stations, one layer per ISO
    for iso in ("NYISO", "ISONE"):
        fg = FeatureGroup(name=f"{iso} — planned stations", show=True)
        for s in stations:
            if s["iso"] != iso:
                continue
            big = s.get("dcfc_ports", 0) >= LARGE_DCFC_MIN_PORTS
            color = ISO_COLOR[iso]
            CircleMarker(
                [s["lat"], s["lon"]],
                radius=4 + (3 if big else 0),
                color=color, fill=True, fill_color=color,
                fill_opacity=0.85 if big else 0.4, weight=1,
                tooltip=folium.Tooltip(
                    f"<b>{s['name']}</b><br>{s['dcfc_ports']} DCFC / "
                    f"{s.get('l2_ports',0)} L2 ports • {s.get('max_kw',0):.0f} kW"
                    f"<br>{s['status']} • online {s.get('planned_online','?')}",
                    sticky=True),
            ).add_to(fg)
        fg.add_to(m)

    # Ranked-zone bubbles (sized by DCFC ports, labeled with projected vol uplift)
    fg_rank = FeatureGroup(name="Zone ranking (planned DCFC + projected volatility)",
                           show=True)
    for r in ranked:
        if r["lat"] is None or r["dcfc_ports"] == 0:
            continue
        proj = r.get("proj_vol_uplift_pct")
        proj_s = f"<br><b>Projected vol uplift: {proj:+.1f}%</b>" if proj else ""
        CircleMarker(
            [r["lat"], r["lon"]],
            radius=_radius(r["dcfc_ports"]),
            color="#222", weight=1,
            fill=True, fill_color=ISO_COLOR[r["iso"]], fill_opacity=0.20,
            tooltip=folium.Tooltip(
                f"<b>#{r['rank']} {r['iso']} {r['zone']}</b><br>"
                f"{r['n_sites']} sites • {r['dcfc_ports']} DCFC ports • "
                f"{r['large_sites']} large (5+)"
                f"<br>{r['planned_kw']/1000:.1f} MW planned{proj_s}",
                sticky=True),
        ).add_to(fg_rank)
    fg_rank.add_to(m)

    _legend(m, backtest)
    LayerControl(collapsed=False, position="topright").add_to(m)
    return m


def _legend(m: folium.Map, backtest: dict | None) -> None:
    cal = ""
    if backtest:
        a = backtest.get("aggregate", {}).get("hourly_std", {})
        if a:
            cal = (f"<br><b>Backtest:</b> a large DCFC site is associated with "
                   f"<b>{a['mean_did_pct']:+.1f}%</b> mean DiD change in hourly "
                   f"LBMP volatility (n={a['n']}).")
    html = f"""
    <div style="position:fixed;bottom:20px;left:20px;z-index:9999;
                background:white;padding:10px 12px;border:1px solid #999;
                border-radius:6px;font:12px/1.4 sans-serif;max-width:320px;">
      <b>Planned EV fast-charging → nodal volatility</b><br>
      <span style="color:#1f77b4;">●</span> NYISO&nbsp;&nbsp;
      <span style="color:#d62728;">●</span> ISO-NE<br>
      Bubble size = planned DC-fast ports in that zone.{cal}
    </div>"""
    m.get_root().html.add_child(folium.Element(html))
