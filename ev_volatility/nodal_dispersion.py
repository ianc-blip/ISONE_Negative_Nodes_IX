"""
nodal_dispersion.py
==================
Sub-zonal ("pnode-level") angle on the EV-charger question that does NOT require
mapping each charger to the node that prices it — a mapping public data can't
support (see the coverage note in the README).

Idea
----
EV fast-charging is *localized* load. If it matters at all, it should show up not
in the zone price (too coarse) but in the **locational spread between nodes** —
i.e. how far individual network-node prices sit from each other. The cleanest
proxy is the cross-node dispersion of the **congestion component** of Day-Ahead
LMP, which strips out the system-wide energy price and isolates the purely
locational signal.

Method
------
1. Sample a few days per month of ISO-NE Day-Ahead nodal LMP (all ~1,150 network
   nodes) from `iso-ne.com` — monthly sampling keeps the download tractable.
2. Per sampled day: for each hour compute the cross-node std of the congestion
   component; average over the 24 hours → one dispersion number per day. Average
   the sampled days → a monthly dispersion series. (Total LMP dispersion is also
   recorded for reference.) Only the small per-day scalar is cached.
3. From the NREL data, build the cumulative ISO-NE DC-fast port count by month
   (real, from open_date).
4. Correlate monthly nodal dispersion against cumulative EV DC-fast ports — and,
   as a confound check, against a plain time index (both series trend with time).

This is exploratory and correlational: chargers are a tiny share of ISO-NE load,
and many things move nodal congestion (topology, fuel prices, weather). Read the
correlation as "is there any relationship at all", not causation.
"""

from __future__ import annotations

import io
import json
import logging
from datetime import date
from pathlib import Path

import pandas as pd
import requests

import lmp_data as L
import nrel_stations as nrel

log = logging.getLogger("dispersion")

_DISP_CACHE = L.CACHE_DIR / "isone_dispersion"
_DISP_CACHE.mkdir(exist_ok=True)
OUTPUT_DIR = Path(__file__).parent / "output"

SAMPLE_DAYS = (7, 14, 21, 28)     # days-of-month to sample


# ── per-day dispersion (cached scalar) ──────────────────────────────────────────

def dispersion_day(d: date) -> dict | None:
    """Mean over 24h of the hourly cross-node std of congestion + total LMP."""
    cache = _DISP_CACHE / f"{d:%Y%m%d}.json"
    if cache.exists():
        return json.loads(cache.read_text())

    url = L.ISONE_DAM_DAY_URL.format(ymd=f"{d:%Y%m%d}")
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=90)
            r.raise_for_status()
            break
        except Exception as e:
            if attempt == 2:
                log.warning("dispersion %s fetch failed: %s", d, e)
                return None
    df = pd.read_csv(io.StringIO(r.text), header=None, dtype=str,
                     names=list(range(10)), on_bad_lines="skip")
    d_rows = df[(df[0] == "D") & (df[5] == "NETWORK NODE")].copy()
    if d_rows.empty:
        return None
    he = pd.to_numeric(d_rows[2], errors="coerce")
    cong = pd.to_numeric(d_rows[8], errors="coerce")
    lmp = pd.to_numeric(d_rows[6], errors="coerce")
    g = pd.DataFrame({"he": he, "cong": cong, "lmp": lmp}).dropna()
    rec = {
        "date": d.isoformat(),
        "cong_disp": float(g.groupby("he")["cong"].std().mean()),
        "lmp_disp": float(g.groupby("he")["lmp"].std().mean()),
        "n_nodes": int(g["cong"].count() // g["he"].nunique() if g["he"].nunique() else 0),
    }
    cache.write_text(json.dumps(rec))
    return rec


def monthly_dispersion(start: date, end: date) -> pd.DataFrame:
    """Monthly nodal dispersion series between start and end (sampled days)."""
    rows, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        day_recs = []
        for dom in SAMPLE_DAYS:
            try:
                d = date(y, m, dom)
            except ValueError:
                continue
            if d < start or d > end:
                continue
            rec = dispersion_day(d)
            if rec:
                day_recs.append(rec)
        if day_recs:
            rows.append({
                "month": f"{y}-{m:02d}",
                "cong_disp": sum(r["cong_disp"] for r in day_recs) / len(day_recs),
                "lmp_disp": sum(r["lmp_disp"] for r in day_recs) / len(day_recs),
                "n_days": len(day_recs),
            })
        log.info("dispersion %s-%02d: %d days", y, m, len(day_recs))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return pd.DataFrame(rows)


# ── EV buildout series ──────────────────────────────────────────────────────────

def cumulative_ev_ports(stations: list[dict], iso: str = "ISONE") -> pd.DataFrame:
    """Cumulative DC-fast ports online by month for one ISO (from open_date)."""
    recs = []
    for s in stations:
        if s["iso"] != iso or not s.get("open_date") or s.get("dcfc_ports", 0) <= 0:
            continue
        recs.append({"month": s["open_date"][:7], "ports": s["dcfc_ports"]})
    if not recs:
        return pd.DataFrame(columns=["month", "cum_dcfc_ports"])
    df = pd.DataFrame(recs).groupby("month")["ports"].sum().sort_index().cumsum()
    return df.reset_index().rename(columns={"ports": "cum_dcfc_ports"})


# ── correlation helper (no scipy) ───────────────────────────────────────────────

def _pearson(x: list[float], y: list[float]) -> float | None:
    n = len(x)
    if n < 3:
        return None
    mx, my = sum(x) / n, sum(y) / n
    sx = sum((a - mx) ** 2 for a in x) ** 0.5
    sy = sum((b - my) ** 2 for b in y) ** 0.5
    if sx == 0 or sy == 0:
        return None
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y))
    return cov / (sx * sy)


# ── driver ──────────────────────────────────────────────────────────────────────

def run(start: date = date(2022, 1, 1), end: date | None = None) -> dict:
    end = end or date.today()
    stations = nrel.load_stations()
    disp = monthly_dispersion(start, end)
    if disp.empty:
        raise RuntimeError("no dispersion data fetched")
    ev = cumulative_ev_ports(stations, "ISONE")

    merged = disp.merge(ev, on="month", how="left")
    merged["cum_dcfc_ports"] = merged["cum_dcfc_ports"].ffill().fillna(0)
    merged["t"] = range(len(merged))

    cong = merged["cong_disp"].tolist()
    ports = merged["cum_dcfc_ports"].tolist()
    tidx = merged["t"].tolist()
    report = {
        "n_months": len(merged),
        "period": f"{merged['month'].iloc[0]} → {merged['month'].iloc[-1]}",
        "corr_cong_vs_ports": _pearson(cong, ports),
        "corr_cong_vs_time": _pearson(cong, tidx),
        "corr_ports_vs_time": _pearson(ports, tidx),
        "cong_disp_first": cong[0], "cong_disp_last": cong[-1],
        "ports_first": ports[0], "ports_last": ports[-1],
        "series": merged.to_dict(orient="records"),
    }
    return report


def format_report(rep: dict) -> str:
    L_ = []
    L_.append("=" * 72)
    L_.append("ISO-NE NODAL CONGESTION DISPERSION vs EV FAST-CHARGING BUILDOUT")
    L_.append("=" * 72)
    L_.append(f"Period            : {rep['period']} ({rep['n_months']} months)")
    L_.append(f"Cong. dispersion  : {rep['cong_disp_first']:.2f} → "
              f"{rep['cong_disp_last']:.2f} $/MWh (cross-node std)")
    L_.append(f"DC-fast ports     : {rep['ports_first']:.0f} → {rep['ports_last']:.0f}")
    L_.append("")
    L_.append("CORRELATIONS (Pearson r):")
    def r(v): return f"{v:+.2f}" if v is not None else " n/a"
    L_.append(f"  dispersion vs cumulative DC-fast ports : {r(rep['corr_cong_vs_ports'])}")
    L_.append(f"  dispersion vs time index (confounder)  : {r(rep['corr_cong_vs_time'])}")
    L_.append(f"  ports      vs time index (confounder)  : {r(rep['corr_ports_vs_time'])}")
    L_.append("")
    cvp = rep["corr_cong_vs_ports"]
    cvt = rep["corr_cong_vs_time"]
    if cvp is None:
        note = "Insufficient data."
    elif abs(cvp) < 0.3:
        note = ("No meaningful relationship: nodal congestion dispersion does not "
                "track EV buildout.")
    elif cvt is not None and abs(cvt) >= abs(cvp) - 0.1:
        note = ("Any co-movement is explained by the shared time trend — both "
                "series drift together, not a charger effect.")
    else:
        note = ("Dispersion co-moves with buildout beyond the time trend — worth a "
                "closer, controlled look.")
    L_.append("READ: " + note)
    L_.append("=" * 72)
    return "\n".join(L_)


def build_chart_svg(rep: dict) -> str:
    """Self-contained dual-axis SVG line chart: dispersion vs cumulative ports."""
    s = rep["series"]
    W, H, pad = 860, 380, 55
    xs = [r["month"] for r in s]
    cong = [r["cong_disp"] for r in s]
    ports = [r["cum_dcfc_ports"] for r in s]
    n = len(s)

    def sx(i): return pad + i * (W - 2 * pad) / max(n - 1, 1)
    cmin, cmax = min(cong), max(cong)
    pmin, pmax = min(ports), max(ports)
    def syc(v): return H - pad - (v - cmin) / (cmax - cmin or 1) * (H - 2 * pad)
    def syp(v): return H - pad - (v - pmin) / (pmax - pmin or 1) * (H - 2 * pad)

    cong_pts = " ".join(f"{sx(i):.1f},{syc(v):.1f}" for i, v in enumerate(cong))
    port_pts = " ".join(f"{sx(i):.1f},{syp(v):.1f}" for i, v in enumerate(ports))
    ticks = ""
    for i in range(0, n, max(n // 8, 1)):
        ticks += (f'<text x="{sx(i):.0f}" y="{H-pad+16}" font-size="10" '
                  f'text-anchor="middle" fill="#666">{xs[i]}</text>')
    r = rep["corr_cong_vs_ports"]
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="100%">
  <rect width="{W}" height="{H}" fill="white"/>
  <text x="{pad}" y="26" font-size="15" font-weight="bold" fill="#222">
    ISO-NE nodal congestion dispersion (blue) vs cumulative EV DC-fast ports (orange)</text>
  <text x="{pad}" y="44" font-size="11" fill="#666">
    Pearson r(dispersion, ports) = {r:+.2f} — {rep['period']}</text>
  <polyline points="{cong_pts}" fill="none" stroke="#1f77b4" stroke-width="2"/>
  <polyline points="{port_pts}" fill="none" stroke="#f4a261" stroke-width="2" stroke-dasharray="4 3"/>
  <text x="{pad}" y="{H-pad+34}" font-size="10" fill="#1f77b4">left axis: cross-node congestion std ($/MWh) &#160;&#160;</text>
  <text x="{W/2}" y="{H-pad+34}" font-size="10" fill="#f4a261">right axis: cumulative ISO-NE DC-fast ports</text>
  {ticks}
</svg>"""


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    OUTPUT_DIR.mkdir(exist_ok=True)
    rep = run()
    (OUTPUT_DIR / "nodal_dispersion.json").write_text(json.dumps(rep, indent=2))
    txt = format_report(rep)
    (OUTPUT_DIR / "nodal_dispersion.txt").write_text(txt)
    pd.DataFrame(rep["series"]).to_csv(OUTPUT_DIR / "nodal_dispersion.csv", index=False)
    html = ("<!doctype html><meta charset='utf-8'><title>ISO-NE nodal dispersion vs "
            "EV buildout</title>" + build_chart_svg(rep))
    (OUTPUT_DIR / "nodal_dispersion_chart.html").write_text(html)
    print("\n" + txt + "\n")
    print("wrote output/nodal_dispersion.{json,txt,csv} + _chart.html")


if __name__ == "__main__":
    main()
