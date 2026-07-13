"""
maine_neb_fetcher.py
====================
Maine distributed-generation compensation data track.

Compiled from the Maine Public Utilities Commission (MPUC) response to
Dichotomy Energy's data request (email exchange 2026-07-12 / 2026-07-13):

  1. Net Energy Billing (NEB) tariff & credit rate schedules
     — Docket 2019-00197, order dated 2025-12-17 (tariff rates CY 2026-2046).
  2. "Analysis of Net Benefits of Net Energy Billing" reports (annual, filed
     each March 31 since 2024) + recent CMP / Versant stranded-cost dockets.
  3. Generation / storage projects — MPUC publishes no machine-readable index;
     monthly utility reports live in docket 2020-00199.

Why this lives next to the ISONE map pipeline
----------------------------------------------
NEB is the primary driver of behind-the-meter and community solar in Maine,
and that distributed solar is what pushes the ISONE nodes tracked by this
platform into negative prices. This module gives the model a machine-readable
handle on Maine's DER-compensation policy record: which docket the tariff
rates come from, where the annual cost-of-NEB (net-benefits) analysis lives,
and the best-available substitute for a generation/storage project index.

The registry itself is seed_data/maine_neb.json. This module loads it, builds
MPUC case-management portal URLs, and provides helpers to populate the tariff
schedule once the numeric rates are pulled from the 2025-12-17 order (the email
response gave the docket pointer, not the rate table).
"""

import json
import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger("maine_neb")

SEED_DIR = Path(__file__).parent / "seed_data"
REGISTRY_PATH = SEED_DIR / "maine_neb.json"

# MPUC public case-management portal. Each docket ("2019-00197" style) resolves
# to a CaseMaster page listing every filing and order in that proceeding.
MPUC_CASE_URL = (
    "https://mpuc-cms.maine.gov/CQM.Public.WebUI/Common/CaseMaster.aspx"
    "?CaseNumber={docket}"
)
MPUC_REPORTS_TO_LEGISLATURE = (
    "https://www.maine.gov/mpuc/regulated-utilities/electricity/reports"
)


# ── Registry loading ──────────────────────────────────────────────────────────

def load_registry(path: Path = REGISTRY_PATH) -> dict:
    """Load the Maine NEB / DER-compensation registry (seed_data/maine_neb.json)."""
    if not path.exists():
        log.warning("Maine NEB registry not found at %s", path)
        return {}
    with open(path) as f:
        return json.load(f)


def docket_url(docket: str) -> str:
    """Build the MPUC case-management portal URL for a docket number."""
    return MPUC_CASE_URL.format(docket=docket)


def all_dockets(registry: Optional[dict] = None) -> list[dict]:
    """
    Flatten every docket referenced in the registry into a lookup list:
      [{"docket", "category", "utility", "url"}, ...]

    Useful for building a citations panel or a link index in the model's
    Maine section.
    """
    reg = registry if registry is not None else load_registry()
    if not reg:
        return []

    out: list[dict] = []

    tariff = reg.get("neb_tariff", {})
    if tariff.get("docket"):
        out.append({
            "docket": tariff["docket"],
            "category": "neb_tariff",
            "utility": None,
            "url": docket_url(tariff["docket"]),
        })

    sc = reg.get("cost_of_neb", {}).get("stranded_cost_dockets", {})
    for utility, dockets in sc.get("utilities", {}).items():
        for d in dockets:
            out.append({
                "docket": d,
                "category": "stranded_cost",
                "utility": utility,
                "url": docket_url(d),
            })

    gen = reg.get("generation_storage_projects", {}).get("best_available_source", {})
    if gen.get("docket"):
        out.append({
            "docket": gen["docket"],
            "category": "generation_storage_monthly",
            "utility": None,
            "url": docket_url(gen["docket"]),
        })

    return out


# ── Tariff schedule ───────────────────────────────────────────────────────────

def get_tariff_schedule(program: str = "tariff_rate_neb",
                        registry: Optional[dict] = None) -> list[dict]:
    """
    Return the NEB tariff rate schedule for a program:
      program="tariff_rate_neb"  → [{"vintage_year", "rate_usd_per_kwh"}, ...]
      program="commercial_neb"   → [] (kWh-credit; not set in advance)
    """
    reg = registry if registry is not None else load_registry()
    for p in reg.get("neb_tariff", {}).get("programs", []):
        if p.get("key") == program:
            return p.get("schedule", [])
    return []


def set_tariff_rates(rates_by_year: dict, program: str = "tariff_rate_neb",
                     path: Path = REGISTRY_PATH) -> int:
    """
    Populate the tariff schedule with numeric rates pulled from the governing
    order (Docket 2019-00197, 2025-12-17). The email response gave the docket
    pointer, not the rate table, so schedule values ship as null until filled.

    Parameters
    ----------
    rates_by_year : {2026: 0.xxxx, 2027: 0.xxxx, ...} in $/kWh.

    Returns the number of vintage years updated. Persists to maine_neb.json.
    """
    reg = load_registry(path)
    updated = 0
    for p in reg.get("neb_tariff", {}).get("programs", []):
        if p.get("key") != program:
            continue
        for row in p.get("schedule", []):
            yr = row.get("vintage_year")
            if yr in rates_by_year:
                row["rate_usd_per_kwh"] = rates_by_year[yr]
                updated += 1
    with open(path, "w") as f:
        json.dump(reg, f, indent=2)
    log.info("Set %d tariff rate(s) for program '%s'", updated, program)
    return updated


# ── kWh-credit valuation ──────────────────────────────────────────────────────

def value_kwh_credit(delivery_rate_usd_per_kwh: float,
                     supply_rate_usd_per_kwh: float) -> float:
    """
    Value one kWh of commercial-NEB (kWh-credit) compensation.

    Per MPUC: the kWh-credit rate is NOT set in advance — the credit is worth
    the delivery + supply rates in effect at the time it is applied. Callers
    should feed the CMP / Versant delivery rate and the prevailing standard-offer
    (or competitive supplier) supply rate for the applicable period.
    """
    return round(float(delivery_rate_usd_per_kwh) + float(supply_rate_usd_per_kwh), 6)


# ── Report index ──────────────────────────────────────────────────────────────

def net_benefits_reports(registry: Optional[dict] = None) -> dict:
    """
    Return the 'Analysis of Net Benefits of Net Energy Billing' report metadata
    (annual, filed each March 31 since 2024) used to calibrate the
    ratepayer-recovery / cost-of-NEB component of the retail-rate model.
    """
    reg = registry if registry is not None else load_registry()
    return reg.get("cost_of_neb", {}).get("net_benefits_reports", {})


def summary() -> str:
    """Human-readable one-screen summary of the Maine NEB record."""
    reg = load_registry()
    if not reg:
        return "Maine NEB registry unavailable."
    t = reg["neb_tariff"]
    lines = [
        "Maine NEB / DER-compensation record (source: MPUC data response, 2026-07-13)",
        f"  NEB tariff        : Docket {t['docket']} — order {t['governing_order_date']} (CY 2026-2046)",
        f"                      portal: {docket_url(t['docket'])}",
        "  Programs          : " + ", ".join(p["name"] for p in t["programs"]),
        f"  Net-benefits rpts : {net_benefits_reports(reg)['title']} — annual, filed Mar 31 since 2024",
        f"                      {MPUC_REPORTS_TO_LEGISLATURE}",
        f"  Stranded-cost     : CMP + Versant dockets 2021-2025 ({len(all_dockets(reg))} dockets indexed total)",
        "  Gen/storage index : not published by MPUC; monthly utility filings in Docket 2020-00199",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(summary())
    print("\nDocket index:")
    for d in all_dockets():
        u = f" [{d['utility']}]" if d["utility"] else ""
        print(f"  {d['docket']}  {d['category']}{u}\n    {d['url']}")
