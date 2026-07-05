"""
volatility_backtest.py
=====================
Event-study backtest that calibrates how much *local nodal price volatility*
changes after a large (>4-port) DC-fast charging site energizes.

Design — difference-in-differences (DiD) event study
----------------------------------------------------
For each charger event (a zone + an energization month):

  1. Pre window  = [online - 90d, online - 15d]   (the 15d gap avoids ramp/commissioning noise)
     Post window = [online + 15d, online + 90d]

  2. For the *treated* zone and for a *control* (median of all other NYISO zones),
     compute three volatility metrics on hourly Day-Ahead LBMP in each window:
        • hourly_std        — std dev of hourly LBMP ($/MWh)
        • daily_range       — mean of (daily max − daily min)
        • spike_share       — share of hours above the window's 95th pct level*
     (*spike threshold is fixed from the pre window so post is measured on the same bar.)

  3. Treated %Δ  = (post − pre) / pre for each metric.
     Control %Δ  = same for the control series.
     DiD  = Treated %Δ − Control %Δ    → charger-attributable change, net of the
            market-wide seasonal move that hits every zone.

Aggregating DiD across events gives the headline calibration:
    "a large DCFC install is associated with an X% DiD change in hourly LBMP
     volatility at its local zone."

All prices are REAL NYISO Day-Ahead LBMP. The event list is representative
(see seed_data/seed_charger_events.json) and easily swapped for a scraped/known
set of dated energizations.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

import lmp_data as L
from iso_regions import SEED_DIR

log = logging.getLogger("backtest")

EVENTS_FILE = SEED_DIR / "seed_charger_events.json"

PRE_DAYS = 90
POST_DAYS = 90
GAP_DAYS = 15           # commissioning buffer excluded on each side of `online`


# ── metric helpers ─────────────────────────────────────────────────────────────

def _window_metrics(s: pd.Series, spike_threshold: float | None) -> dict:
    """Volatility metrics for one hourly price series over one window."""
    s = s.dropna()
    if len(s) < 48:                       # need at least ~2 days of hours
        return {}
    daily = s.groupby(s.index.date)
    daily_range = (daily.max() - daily.min()).mean()
    thr = spike_threshold if spike_threshold is not None else s.quantile(0.95)
    spike_share = float((s > thr).mean())
    return {
        "hourly_std": float(s.std()),
        "daily_range": float(daily_range),
        "spike_share": spike_share,
        "spike_threshold": float(thr),
        "n_hours": int(len(s)),
        "mean_lbmp": float(s.mean()),
    }


def _pct_change(pre: dict, post: dict, key: str) -> float | None:
    if not pre or not post or key not in pre or key not in post:
        return None
    a, b = pre[key], post[key]
    if a == 0:
        return None
    return (b - a) / abs(a) * 100.0


# ── one event ───────────────────────────────────────────────────────────────────

def _online_date(ev: dict) -> date:
    y, m = ev["online"].split("-")
    return date(int(y), int(m), 15)          # center on mid-month


def run_event(ev: dict, panel: pd.DataFrame) -> dict | None:
    online = _online_date(ev)
    zone = ev["zone"]

    pre_lo = online - timedelta(days=PRE_DAYS)
    pre_hi = online - timedelta(days=GAP_DAYS)
    post_lo = online + timedelta(days=GAP_DAYS)
    post_hi = online + timedelta(days=POST_DAYS)

    treated = L.zone_series(panel, zone)
    if treated.empty:
        log.warning("no price data for zone %s (%s)", zone, ev["name"])
        return None

    # control = median across all OTHER zones at each timestamp
    others = panel[panel["zone"] != zone]
    control = (others.pivot_table(index="ts", columns="zone", values="lbmp")
                     .median(axis=1))

    def slc(s, lo, hi):
        return s[(s.index.date >= lo) & (s.index.date <= hi)]

    t_pre = _window_metrics(slc(treated, pre_lo, pre_hi), None)
    t_post = _window_metrics(slc(treated, post_lo, post_hi),
                             t_pre.get("spike_threshold") if t_pre else None)
    c_pre = _window_metrics(slc(control, pre_lo, pre_hi), None)
    c_post = _window_metrics(slc(control, post_lo, post_hi),
                             c_pre.get("spike_threshold") if c_pre else None)

    if not (t_pre and t_post and c_pre and c_post):
        log.warning("insufficient window data for %s", ev["name"])
        return None

    result = {"event": ev["name"], "iso": ev.get("iso", "NYISO"), "zone": zone,
              "online": ev["online"], "dcfc_ports": ev["dcfc_ports"], "metrics": {}}
    for key in ("hourly_std", "daily_range", "spike_share", "mean_lbmp"):
        t_chg = _pct_change(t_pre, t_post, key)
        c_chg = _pct_change(c_pre, c_post, key)
        did = (t_chg - c_chg) if (t_chg is not None and c_chg is not None) else None
        result["metrics"][key] = {
            "treated_pre": t_pre[key], "treated_post": t_post[key],
            "treated_pct": t_chg, "control_pct": c_chg, "did_pct": did,
        }
    return result


# ── driver ──────────────────────────────────────────────────────────────────────

def load_events() -> list[dict]:
    with open(EVENTS_FILE) as f:
        return json.load(f)


METRIC_KEYS = ("hourly_std", "daily_range", "spike_share", "mean_lbmp")


def _aggregate(per_event: list[dict]) -> dict:
    """DiD aggregation over a list of per-event results."""
    agg = {}
    for key in METRIC_KEYS:
        dids = [r["metrics"][key]["did_pct"] for r in per_event
                if r["metrics"][key]["did_pct"] is not None]
        treated = [r["metrics"][key]["treated_pct"] for r in per_event
                   if r["metrics"][key]["treated_pct"] is not None]
        if dids:
            dids_sorted = sorted(dids)
            mid = len(dids_sorted) // 2
            median = (dids_sorted[mid] if len(dids_sorted) % 2
                      else (dids_sorted[mid - 1] + dids_sorted[mid]) / 2)
            agg[key] = {
                "n": len(dids),
                "mean_did_pct": sum(dids) / len(dids),
                "median_did_pct": median,
                "mean_treated_pct": sum(treated) / len(treated) if treated else None,
                "share_increasing": sum(1 for d in dids if d > 0) / len(dids),
            }
    return agg


def run_backtest(events: list[dict] | None = None) -> dict:
    """
    Run the event study across BOTH ISOs (NYISO + ISO-NE). Each ISO's events are
    measured against that ISO's own zonal price panel and control set.
    """
    events = events or load_events()
    by_iso = {"NYISO": [], "ISONE": []}
    for e in events:
        by_iso.setdefault(e.get("iso", "NYISO"), []).append(e)

    per_event = []
    for iso, iso_events in by_iso.items():
        if not iso_events:
            continue
        lows = [_online_date(e) - timedelta(days=PRE_DAYS) for e in iso_events]
        highs = [_online_date(e) + timedelta(days=POST_DAYS) for e in iso_events]
        # Cap at today — future-dated price files don't exist yet.
        start, end = min(lows), min(max(highs), date.today())
        log.info("Fetching %s panel %s → %s for %d events",
                 iso, start, end, len(iso_events))
        panel = L.zone_panel(iso, start, end)
        if panel.empty:
            log.warning("%s panel empty — skipping %d events", iso, len(iso_events))
            continue
        per_event += [r for e in iso_events if (r := run_event(e, panel))]

    if not per_event:
        raise RuntimeError("No events produced results — no price data fetched")

    by_iso_agg = {}
    for iso in ("NYISO", "ISONE"):
        ev = [r for r in per_event if r["iso"] == iso]
        if ev:
            by_iso_agg[iso] = {"n_events": len(ev), "aggregate": _aggregate(ev)}

    return {
        "n_events": len(per_event),
        "window": {"pre_days": PRE_DAYS, "post_days": POST_DAYS, "gap_days": GAP_DAYS},
        "aggregate": _aggregate(per_event),
        "by_iso": by_iso_agg,
        "events": per_event,
    }


def format_report(report: dict) -> str:
    """Human-readable text summary."""
    lines = []
    lines.append("=" * 72)
    lines.append("EV FAST-CHARGER → NODAL VOLATILITY BACKTEST (NYISO + ISO-NE, DiD)")
    lines.append("=" * 72)
    w = report["window"]
    lines.append(f"Events analysed : {report['n_events']}")
    lines.append(f"Windows         : {w['pre_days']}d pre / {w['post_days']}d post "
                 f"({w['gap_days']}d commissioning gap)")
    labels = {"hourly_std": "Hourly LBMP volatility (std)",
              "daily_range": "Daily price range",
              "spike_share": "Price-spike frequency (>95th pct)",
              "mean_lbmp": "Mean price level ($/MWh)"}

    def _headline(agg: dict, indent: str = "  "):
        for key, a in agg.items():
            arrow = "▲ increases" if a["mean_did_pct"] > 0 else "▼ decreases"
            lines.append(f"{indent}• {labels[key]:34s}: {arrow} "
                         f"{a['mean_did_pct']:+.1f}% mean DiD "
                         f"(median {a['median_did_pct']:+.1f}%, "
                         f"{a['share_increasing']*100:.0f}% up, n={a['n']})")

    lines.append("")
    lines.append("HEADLINE — ALL EVENTS (charger-attributable, market-move removed):")
    _headline(report["aggregate"])

    for iso, block in report.get("by_iso", {}).items():
        lines.append("")
        lines.append(f"HEADLINE — {iso} only ({block['n_events']} events):")
        _headline(block["aggregate"])

    lines.append("")
    lines.append("PER-EVENT (DiD % change vs. rest-of-ISO control):")
    lines.append(f"  {'Site':30s} {'ISO':6s} {'Zone':6s} {'Prt':>3s} "
                 f"{'std':>6s} {'range':>6s} {'spike':>6s} {'price':>6s}")
    for r in report["events"]:
        m = r["metrics"]
        def fmt(k):
            v = m[k]["did_pct"]
            return f"{v:+.0f}%" if v is not None else " n/a"
        lines.append(f"  {r['event'][:30]:30s} {r['iso']:6s} {r['zone']:6s} "
                     f"{r['dcfc_ports']:3d} {fmt('hourly_std'):>6s} "
                     f"{fmt('daily_range'):>6s} {fmt('spike_share'):>6s} "
                     f"{fmt('mean_lbmp'):>6s}")
    lines.append("=" * 72)
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    rep = run_backtest()
    print(format_report(rep))
