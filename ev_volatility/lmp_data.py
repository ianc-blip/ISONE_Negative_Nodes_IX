"""
lmp_data.py
===========
Fetch hourly Locational Marginal / LBMP prices used by the volatility backtest.

NYISO  — 100% public, no auth. Day-Ahead zonal LBMP is published as monthly ZIP
         archives of daily CSVs:
             http://mis.nyiso.com/public/csv/damlbmp/YYYYMM01damlbmp_zone_csv.zip
         Each daily CSV: Time Stamp, Name (zone), PTID, LBMP ($/MWHr), ...
         This is the live source that drives the real backtest numbers.

ISONE  — annual bulk LMP CSVs (also public). Wired up here for completeness; the
         shipped backtest events are NYISO sites, so ISONE fetch is optional.

Monthly downloads are cached under ev_volatility/.cache so re-runs are fast and
kind to NYISO's servers.
"""

from __future__ import annotations

import io
import logging
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

log = logging.getLogger("lmp_data")

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

NYISO_DAM_ZONE_URL = (
    "http://mis.nyiso.com/public/csv/damlbmp/{ym}01damlbmp_zone_csv.zip"
)
# Generator-node Day-Ahead LBMP (hundreds of priced pnodes statewide).
NYISO_DAM_GEN_URL = (
    "http://mis.nyiso.com/public/csv/damlbmp/{ym}01damlbmp_gen_csv.zip"
)

# NYISO "Name" column values that are external proxies / hubs, not load zones.
_NYISO_NON_ZONE = {"H Q", "NPX", "O H", "PJM"}


# ── NYISO ──────────────────────────────────────────────────────────────────────

def _nyiso_month_zip(year: int, month: int, kind: str = "zone") -> bytes | None:
    ym = f"{year}{month:02d}"
    cache = CACHE_DIR / f"nyiso_dam_{kind}_{ym}.zip"
    if cache.exists():
        return cache.read_bytes()
    tmpl = NYISO_DAM_GEN_URL if kind == "gen" else NYISO_DAM_ZONE_URL
    url = tmpl.format(ym=ym)
    try:
        r = requests.get(url, timeout=90)
        r.raise_for_status()
        cache.write_bytes(r.content)
        log.info("NYISO %s: downloaded %d bytes", ym, len(r.content))
        return r.content
    except Exception as e:
        log.error("NYISO %s fetch failed: %s", ym, e)
        return None


def nyiso_zonal_lbmp(year: int, month: int) -> pd.DataFrame:
    """
    Return tidy hourly DAM zonal LBMP for one month:
        columns = [ts (datetime), zone (str), lbmp (float)]
    """
    blob = _nyiso_month_zip(year, month)
    if blob is None:
        return pd.DataFrame(columns=["ts", "zone", "lbmp"])

    frames = []
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as fh:
                df = pd.read_csv(fh)
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["ts", "zone", "lbmp"])

    raw = pd.concat(frames, ignore_index=True)
    raw.columns = [c.strip() for c in raw.columns]
    ts_col = next(c for c in raw.columns if "time" in c.lower())
    name_col = next(c for c in raw.columns if c.lower() == "name")
    lbmp_col = next(c for c in raw.columns if "lbmp" in c.lower())

    out = pd.DataFrame({
        "ts": pd.to_datetime(raw[ts_col], errors="coerce"),
        "zone": raw[name_col].astype(str).str.strip(),
        "lbmp": pd.to_numeric(raw[lbmp_col], errors="coerce"),
    }).dropna(subset=["ts", "lbmp"])
    out = out[~out["zone"].isin(_NYISO_NON_ZONE)]
    return out


def nyiso_zone_panel(start: date, end: date) -> pd.DataFrame:
    """
    Hourly LBMP panel for ALL NYISO zones between start and end (inclusive).
    Returns long DataFrame [ts, zone, lbmp]. Months are fetched once and cached.
    """
    months, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    parts = [nyiso_zonal_lbmp(y, m) for y, m in months]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame(columns=["ts", "zone", "lbmp"])
    panel = pd.concat(parts, ignore_index=True)
    mask = (panel["ts"].dt.date >= start) & (panel["ts"].dt.date <= end)
    return panel[mask].reset_index(drop=True)


def zone_series(panel: pd.DataFrame, zone: str) -> pd.Series:
    """Hourly LBMP series (indexed by ts) for one zone out of a panel."""
    z = panel[panel["zone"] == zone].sort_values("ts")
    return z.set_index("ts")["lbmp"]


# ── NYISO generator-node (pnode) LBMP ───────────────────────────────────────────

def nyiso_gen_lbmp(year: int, month: int, ptids: set[str] | None = None) -> pd.DataFrame:
    """
    Tidy hourly DAM generator-node LBMP for one month:
        columns = [ts, ptid, lbmp]
    Optional `ptids` whitelist keeps memory small (we only need nodes near chargers).
    """
    blob = _nyiso_month_zip(year, month, kind="gen")
    if blob is None:
        return pd.DataFrame(columns=["ts", "ptid", "lbmp"])
    frames = []
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                with zf.open(name) as fh:
                    frames.append(pd.read_csv(fh))
    if not frames:
        return pd.DataFrame(columns=["ts", "ptid", "lbmp"])
    raw = pd.concat(frames, ignore_index=True)
    raw.columns = [c.strip() for c in raw.columns]
    ts_col = next(c for c in raw.columns if "time" in c.lower())
    lbmp_col = next(c for c in raw.columns if "lbmp" in c.lower())
    out = pd.DataFrame({
        "ts": pd.to_datetime(raw[ts_col], errors="coerce"),
        "ptid": raw["PTID"].astype(str),
        "lbmp": pd.to_numeric(raw[lbmp_col], errors="coerce"),
    }).dropna(subset=["ts", "lbmp"])
    if ptids is not None:
        out = out[out["ptid"].isin(ptids)]
    return out


def nyiso_gen_panel(start: date, end: date,
                    ptids: set[str] | None = None) -> pd.DataFrame:
    """Hourly generator-node LBMP panel [ts, ptid, lbmp] over a date range."""
    months, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    parts = [nyiso_gen_lbmp(y, m, ptids) for y, m in months]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame(columns=["ts", "ptid", "lbmp"])
    panel = pd.concat(parts, ignore_index=True)
    mask = (panel["ts"].dt.date >= start) & (panel["ts"].dt.date <= end)
    return panel[mask].reset_index(drop=True)


# ── ISONE (optional) ────────────────────────────────────────────────────────────

# Public daily Day-Ahead hourly LMP report (all locations). 302-redirects to
# www.iso-ne.com/histRpts/...; requests follows it automatically. No auth.
ISONE_DAM_DAY_URL = (
    "https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/"
    "WW_DALMP_ISO_{ymd}.csv"
)

# The 8 ISO-NE load zones (Location IDs 4001-4008) → short zone keys.
ISONE_ZONE_IDS = {
    "4001": "ME", "4002": "NH", "4003": "VT", "4004": "CT",
    "4005": "RI", "4006": "SEMA", "4007": "WCMA", "4008": "NEMA",
}
_ISONE_ZONE_CACHE = CACHE_DIR / "isone_zone"
_ISONE_ZONE_CACHE.mkdir(exist_ok=True)


def isone_zonal_lbmp_day(d: date) -> pd.DataFrame:
    """
    Day-Ahead hourly LMP for the 8 ISO-NE load zones on one day:
        columns = [ts, zone, lbmp]
    Only the small filtered (zone-level) slice is cached, so re-runs are cheap.
    """
    cache = _ISONE_ZONE_CACHE / f"{d:%Y%m%d}.csv"
    if cache.exists():
        c = pd.read_csv(cache, parse_dates=["ts"])
        return c

    url = ISONE_DAM_DAY_URL.format(ymd=f"{d:%Y%m%d}")
    r = None
    for attempt in range(3):                # transient resets are common here
        try:
            r = requests.get(url, timeout=90)   # follows the 302 to histRpts
            r.raise_for_status()
            break
        except Exception as e:
            if attempt == 2:
                log.error("ISONE %s fetch failed after retries: %s", d, e)
                return pd.DataFrame(columns=["ts", "zone", "lbmp"])
    if r is None:
        return pd.DataFrame(columns=["ts", "zone", "lbmp"])

    # Rows: "D",Date,HE,LocID,LocName,LocType,LMP,Energy,Cong,Loss
    raw = pd.read_csv(io.StringIO(r.text), header=None, dtype=str,
                      names=list(range(10)), skip_blank_lines=True,
                      on_bad_lines="skip")
    d_rows = raw[(raw[0] == "D") & (raw[3].isin(ISONE_ZONE_IDS))].copy()
    if d_rows.empty:
        return pd.DataFrame(columns=["ts", "zone", "lbmp"])

    he = pd.to_numeric(d_rows[2], errors="coerce").fillna(1).astype(int)
    base = pd.to_datetime(d_rows[1], format="%m/%d/%Y", errors="coerce")
    out = pd.DataFrame({
        "ts": base + pd.to_timedelta(he - 1, unit="h"),
        "zone": d_rows[3].map(ISONE_ZONE_IDS),
        "lbmp": pd.to_numeric(d_rows[6], errors="coerce"),
    }).dropna(subset=["ts", "lbmp"])
    out.to_csv(cache, index=False)
    return out


def isone_zone_panel(start: date, end: date, max_workers: int = 8) -> pd.DataFrame:
    """
    Hourly DA LMP panel for ALL ISO-NE load zones between start and end
    (inclusive). Daily files are fetched concurrently and cached. Returns long
    DataFrame [ts, zone, lbmp].
    """
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        parts = list(ex.map(isone_zonal_lbmp_day, days))
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame(columns=["ts", "zone", "lbmp"])
    panel = pd.concat(parts, ignore_index=True)
    mask = (panel["ts"].dt.date >= start) & (panel["ts"].dt.date <= end)
    return panel[mask].reset_index(drop=True)


def isone_nodes_day(d: date, node_ids: set[str]) -> pd.DataFrame:
    """
    Day-Ahead hourly LMP for specified ISO-NE network node IDs on one day:
        columns = [ts, ptid, lbmp]
    Caches a small per-day slice for the tracked node set.
    """
    key = f"{abs(hash(frozenset(node_ids))) % 10**8}"
    cache = _ISONE_ZONE_CACHE.parent / "isone_nodes" / f"{d:%Y%m%d}_{key}.csv"
    cache.parent.mkdir(exist_ok=True)
    if cache.exists():
        return pd.read_csv(cache, parse_dates=["ts"])

    url = ISONE_DAM_DAY_URL.format(ymd=f"{d:%Y%m%d}")
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=90)
            r.raise_for_status()
            break
        except Exception as e:
            if attempt == 2:
                log.error("ISONE nodes %s fetch failed: %s", d, e)
                return pd.DataFrame(columns=["ts", "ptid", "lbmp"])
    raw = pd.read_csv(io.StringIO(r.text), header=None, dtype=str,
                      names=list(range(10)), on_bad_lines="skip")
    rows = raw[(raw[0] == "D") & (raw[3].isin(node_ids))].copy()
    if rows.empty:
        out = pd.DataFrame(columns=["ts", "ptid", "lbmp"])
        out.to_csv(cache, index=False)
        return out
    he = pd.to_numeric(rows[2], errors="coerce").fillna(1).astype(int)
    base = pd.to_datetime(rows[1], format="%m/%d/%Y", errors="coerce")
    out = pd.DataFrame({
        "ts": base + pd.to_timedelta(he - 1, unit="h"),
        "ptid": rows[3],
        "lbmp": pd.to_numeric(rows[6], errors="coerce"),
    }).dropna(subset=["ts", "lbmp"])
    out.to_csv(cache, index=False)
    return out


def isone_node_panel(start: date, end: date, node_ids: set[str],
                     max_workers: int = 8) -> pd.DataFrame:
    """Hourly DA LMP panel [ts, ptid, lbmp] for specified ISO-NE nodes."""
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        parts = list(ex.map(lambda d: isone_nodes_day(d, node_ids), days))
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame(columns=["ts", "ptid", "lbmp"])
    panel = pd.concat(parts, ignore_index=True)
    mask = (panel["ts"].dt.date >= start) & (panel["ts"].dt.date <= end)
    return panel[mask].reset_index(drop=True)


def zone_panel(iso: str, start: date, end: date) -> pd.DataFrame:
    """Dispatch to the correct ISO's zonal panel fetcher."""
    if iso.upper() == "NYISO":
        return nyiso_zone_panel(start, end)
    if iso.upper() == "ISONE":
        return isone_zone_panel(start, end)
    raise ValueError(f"unknown iso {iso}")
