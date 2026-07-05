"""
pnode_backtest.py
================
NYISO **pnode-level** charger event study — the sub-zonal test the zonal backtest
couldn't resolve, now feasible because NYISO publishes generator-node coordinates
(mis.nyiso.com/public/htm/generator/generator.htm → seed_data/nyiso_gen_geocodes.json)
that join to the priced generator-node LBMP file (damlbmp_gen).

For each large (>4-port) DCFC opening we:
  • snap the charger to the nearest priced generator node within `MAX_KM`,
  • treat that node's Day-Ahead LBMP as the local price,
  • use the charger's own load ZONE price as the control,
  • run the same pre/post DiD window metrics as the zonal backtest.

DiD = (node post-vs-pre %Δ) − (zone post-vs-pre %Δ). If a charger moved its local
node, the node should diverge from its zone after it came online.

Caveat: the nearest *generator* node is a geographic proxy for the charger's
(load) pnode — public data has no charger→pnode map — but at ~hundreds of nodes
statewide it is far finer than the zone. Coverage is reported honestly.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import lmp_data as L
from iso_regions import SEED_DIR, haversine_km
from volatility_backtest import (GAP_DAYS, METRIC_KEYS, POST_DAYS, PRE_DAYS,
                                 _pct_change, _window_metrics, _aggregate)
import nrel_stations as nrel

log = logging.getLogger("pnode")

GEO_FILES = {
    "NYISO": SEED_DIR / "nyiso_gen_geocodes.json",   # generator-node coords
    "ISONE": SEED_DIR / "isone_pnode_geocodes.json",  # full pnode table
}
OUTPUT_DIR = Path(__file__).parent / "output"
MAX_KM = 10.0          # charger→node snap radius


def load_geocodes(iso: str) -> dict:
    with open(GEO_FILES[iso]) as f:
        return json.load(f)


def _nearest_node(lat, lon, geo) -> tuple[str, float]:
    best, bd = None, float("inf")
    for ptid, g in geo.items():
        d = haversine_km(lat, lon, g["lat"], g["lon"])
        if d < bd:
            best, bd = ptid, d
    return best, bd


def build_events(stations, geo, iso, years=3, as_of: date | None = None) -> list[dict]:
    """Large-DCFC openings for one ISO snapped to a nearby priced node."""
    as_of = as_of or date.today()
    cutoff = date(as_of.year - years, as_of.month, 1)
    events = []
    for s in stations:
        if s["iso"] != iso or s["dcfc_ports"] < 5 or not s.get("open_date"):
            continue
        try:
            od = datetime.strptime(s["open_date"], "%Y-%m-%d").date()
        except ValueError:
            continue
        if not (cutoff <= od <= as_of):
            continue
        ptid, dist = _nearest_node(s["lat"], s["lon"], geo)
        if dist > MAX_KM:
            continue
        events.append({
            "name": s["name"], "iso": iso, "zone": s["zone"], "ptid": ptid,
            "node_km": round(dist, 1), "dcfc_ports": s["dcfc_ports"],
            "online": od, "node_name": geo[ptid]["name"],
        })
    events.sort(key=lambda e: e["online"])
    return events


def _slc(s, lo, hi):
    return s[(s.index.date >= lo) & (s.index.date <= hi)]


def run_event(ev, gen_panel, zone_panel) -> dict | None:
    online = ev["online"]
    pre_lo, pre_hi = online - timedelta(days=PRE_DAYS), online - timedelta(days=GAP_DAYS)
    post_lo, post_hi = online + timedelta(days=GAP_DAYS), online + timedelta(days=POST_DAYS)

    node = gen_panel[gen_panel["ptid"] == ev["ptid"]].sort_values("ts").set_index("ts")["lbmp"]
    zone = L.zone_series(zone_panel, ev["zone"])
    if node.empty or zone.empty:
        return None

    t_pre = _window_metrics(_slc(node, pre_lo, pre_hi), None)
    t_post = _window_metrics(_slc(node, post_lo, post_hi),
                             t_pre.get("spike_threshold") if t_pre else None)
    c_pre = _window_metrics(_slc(zone, pre_lo, pre_hi), None)
    c_post = _window_metrics(_slc(zone, post_lo, post_hi),
                             c_pre.get("spike_threshold") if c_pre else None)
    if not (t_pre and t_post and c_pre and c_post):
        return None

    res = {"event": ev["name"], "iso": ev["iso"], "zone": ev["zone"], "ptid": ev["ptid"],
           "node_name": ev["node_name"], "node_km": ev["node_km"],
           "online": ev["online"].strftime("%Y-%m"), "dcfc_ports": ev["dcfc_ports"],
           "metrics": {}}
    for key in METRIC_KEYS:
        tp, cp = _pct_change(t_pre, t_post, key), _pct_change(c_pre, c_post, key)
        did = (tp - cp) if (tp is not None and cp is not None) else None
        # treated_pct alias lets the shared _aggregate() consume these directly.
        res["metrics"][key] = {"node_pct": tp, "zone_pct": cp,
                               "treated_pct": tp, "did_pct": did}
    return res


def _run_iso(iso, stations, years) -> tuple[list[dict], dict]:
    geo = load_geocodes(iso)
    events = build_events(stations, geo, iso, years=years)
    total = sum(1 for s in stations if s["iso"] == iso and s["dcfc_ports"] >= 5
                and s.get("open_date")
                and s["open_date"][:4] >= str(date.today().year - years))
    cov = {"events_snapped": len(events), "large_openings": total}
    if not events:
        return [], cov

    lows = [e["online"] - timedelta(days=PRE_DAYS) for e in events]
    highs = [min(e["online"] + timedelta(days=POST_DAYS), date.today()) for e in events]
    start, end = min(lows), max(highs)
    node_ids = {e["ptid"] for e in events}
    log.info("%s pnode: %d events, %d nodes, %s→%s",
             iso, len(events), len(node_ids), start, end)

    if iso == "NYISO":
        node_panel = L.nyiso_gen_panel(start, end, node_ids)
    else:
        node_panel = L.isone_node_panel(start, end, node_ids)
    zone_panel = L.zone_panel(iso, start, end)
    per_event = [r for e in events if (r := run_event(e, node_panel, zone_panel))]
    cov["events_run"] = len(per_event)
    return per_event, cov


def run(years=3, isos=("NYISO", "ISONE")) -> dict:
    stations = nrel.load_stations()
    per_event, coverage = [], {}
    for iso in isos:
        ev, cov = _run_iso(iso, stations, years)
        per_event += ev
        coverage[iso] = cov
    if not per_event:
        raise RuntimeError("no events within snap radius")

    by_iso = {}
    for iso in isos:
        ev = [r for r in per_event if r["iso"] == iso]
        if ev:
            by_iso[iso] = {"n": len(ev), "aggregate": _aggregate(ev)}

    return {
        "granularity": "pnode (nearest node vs load zone), DiD",
        "max_snap_km": MAX_KM,
        "coverage": coverage,
        "window": {"pre_days": PRE_DAYS, "post_days": POST_DAYS, "gap_days": GAP_DAYS},
        "aggregate": _aggregate(per_event),
        "by_iso": by_iso,
        "events": per_event,
    }


def format_report(rep) -> str:
    labels = {"hourly_std": "Node volatility (hourly std)",
              "daily_range": "Node daily price range",
              "spike_share": "Node spike freq (>95th)",
              "mean_lbmp": "Node price level"}
    lines = ["=" * 74,
             "PNODE-LEVEL CHARGER EVENT STUDY — nearest node vs its zone (DiD)",
             "=" * 74]
    for iso, c in rep["coverage"].items():
        lines.append(f"{iso} coverage: {c.get('events_run',0)} run / "
                     f"{c['events_snapped']} snapped ≤{rep['max_snap_km']:.0f}km / "
                     f"{c['large_openings']} large openings")
    lines.append(f"Window   : {rep['window']['pre_days']}d pre / {rep['window']['post_days']}d post")

    def headline(agg, indent="  "):
        for k, a in agg.items():
            arrow = "▲" if a["mean_did_pct"] > 0 else "▼"
            lines.append(f"{indent}{arrow} {labels[k]:28s}: {a['mean_did_pct']:+.1f}% mean DiD "
                         f"(median {a['median_did_pct']:+.1f}%, {a['share_increasing']*100:.0f}% up, n={a['n']})")

    lines.append("")
    lines.append("HEADLINE — ALL (node change net of its own zone):")
    headline(rep["aggregate"])
    for iso, b in rep.get("by_iso", {}).items():
        lines.append("")
        lines.append(f"HEADLINE — {iso} ({b['n']} events):")
        headline(b["aggregate"])

    lines.append("")
    lines.append(f"  {'Site':24s} {'ISO':6s} {'Zone':6s} {'km':>3s} "
                 f"{'std':>6s} {'range':>6s} {'price':>6s}")
    for r in sorted(rep["events"], key=lambda x: (x["iso"], x["online"])):
        m = r["metrics"]
        def f(k):
            v = m[k]["did_pct"]
            return f"{v:+.0f}%" if v is not None else " n/a"
        lines.append(f"  {r['event'][:24]:24s} {r['iso']:6s} {r['zone']:6s} "
                     f"{r['node_km']:3.0f} {f('hourly_std'):>6s} {f('daily_range'):>6s} "
                     f"{f('mean_lbmp'):>6s}")
    lines.append("=" * 74)
    return "\n".join(lines)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    OUTPUT_DIR.mkdir(exist_ok=True)
    rep = run()
    (OUTPUT_DIR / "pnode_backtest_report.json").write_text(json.dumps(rep, indent=2))
    txt = format_report(rep)
    (OUTPUT_DIR / "pnode_backtest_report.txt").write_text(txt)
    print("\n" + txt + "\n")


if __name__ == "__main__":
    main()
