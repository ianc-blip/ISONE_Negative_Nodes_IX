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
from datetime import date
from pathlib import Path

import pandas as pd
import requests

log = logging.getLogger("lmp_data")

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

NYISO_DAM_ZONE_URL = (
    "http://mis.nyiso.com/public/csv/damlbmp/{ym}01damlbmp_zone_csv.zip"
)

# NYISO "Name" column values that are external proxies / hubs, not load zones.
_NYISO_NON_ZONE = {"H Q", "NPX", "O H", "PJM"}


# ── NYISO ──────────────────────────────────────────────────────────────────────

def _nyiso_month_zip(year: int, month: int) -> bytes | None:
    ym = f"{year}{month:02d}"
    cache = CACHE_DIR / f"nyiso_dam_zone_{ym}.zip"
    if cache.exists():
        return cache.read_bytes()
    url = NYISO_DAM_ZONE_URL.format(ym=ym)
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


# ── ISONE (optional) ────────────────────────────────────────────────────────────

ISONE_BULK_DA_LMP_URL = (
    "https://www.iso-ne.com/static-assets/documents/{year}/hourly/da_lmp_{year}.csv"
)


def isone_annual_lmp(year: int) -> pd.DataFrame:
    """
    Best-effort fetch of ISO-NE annual bulk DA LMP (public). Returns
    [ts, zone, lmp] or empty on failure. Provided for symmetry; the default
    backtest uses NYISO events.
    """
    url = ISONE_BULK_DA_LMP_URL.format(year=year)
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
    except Exception as e:
        log.error("ISONE %d bulk LMP failed: %s", year, e)
        return pd.DataFrame(columns=["ts", "zone", "lmp"])
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df
