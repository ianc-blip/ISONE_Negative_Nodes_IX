"""
run_pipeline.py
==============
End-to-end EV-charger volatility pipeline:

  1. BACKTEST  — calibrate how much local nodal LBMP volatility changes after a
                 large (>4-port) DC-fast charger energizes (real NYISO prices,
                 DiD event study). → output/backtest_report.{json,txt}

  2. SCRAPE    — pull planned / under-construction EV sites from PlugShare
                 (token-gated; falls back to the reproducible seed set) and snap
                 each to its NYISO/ISO-NE zone + nearest ISO-NE pnode.

  3. RANK+MAP  — rank zones by planned DC-fast infrastructure, project a
                 volatility uplift per zone from the backtest calibration, and
                 render an interactive Folium map.
                 → output/ev_node_ranking.json, output/ev_planned_chargers_map_*.html

Usage:
    python run_pipeline.py                 # backtest + scrape + rank + map
    python run_pipeline.py --no-backtest   # skip the (network-heavy) backtest
    python run_pipeline.py --skip-live     # ignore PLUGSHARE_TOKEN, use seed
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")

OUTPUT_DIR = Path(__file__).parent / "output"


def main():
    ap = argparse.ArgumentParser(description="EV-charger → nodal volatility pipeline")
    ap.add_argument("--no-backtest", action="store_true",
                    help="Skip the volatility backtest (no NYISO downloads)")
    ap.add_argument("--skip-live", action="store_true",
                    help="Ignore PLUGSHARE_TOKEN and use the offline seed stations")
    args = ap.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)
    stamp = date.today().strftime("%Y%m%d")

    # 0 ── DATA SOURCE ─────────────────────────────────────────────────────────
    # Prefer NREL (real open_date + DCFC counts); fall back to the offline seed.
    import nrel_stations as nrel
    prefer_live = not args.skip_live
    log.info("Loading EV stations (source: %s)…",
             "NREL" if (prefer_live and nrel.api_key()) else "seed")
    stations = nrel.load_stations(prefer_live=prefer_live)
    src = stations[0]["source"] if stations else "none"
    log.info("  %d stations (source=%s)", len(stations), src)

    # Real dated events = large DCFC openings in the last 3 years.
    events = nrel.historical_openings(stations, years=3)
    if not events:
        import json as _json
        with open(nrel.SEED_DIR / "seed_charger_events.json") as f:
            events = _json.load(f)
        log.info("  no NREL openings available — using %d seed events", len(events))

    # 1 ── BACKTEST ────────────────────────────────────────────────────────────
    backtest = None
    if not args.no_backtest:
        import volatility_backtest as bt
        log.info("Step 1/3 — backtest on NYISO prices over %d events…", len(events))
        try:
            backtest = bt.run_backtest(events)
            (OUTPUT_DIR / "backtest_report.json").write_text(json.dumps(backtest, indent=2))
            report_txt = bt.format_report(backtest)
            (OUTPUT_DIR / "backtest_report.txt").write_text(report_txt)
            print("\n" + report_txt + "\n")
        except Exception as e:
            log.error("Backtest failed (%s) — continuing without calibration", e)
    else:
        log.info("Step 1/3 — backtest skipped (--no-backtest)")

    # 2 ── STATIONS FOR RANKING ────────────────────────────────────────────────
    log.info("Step 2/3 — %d stations for node ranking", len(stations))

    # 3 ── RANK + MAP ──────────────────────────────────────────────────────────
    import node_ranking as nr
    log.info("Step 3/3 — ranking nodes and building map…")
    ranked = nr.rank_nodes(stations)
    ranked = nr.apply_calibration(ranked, backtest)

    (OUTPUT_DIR / "ev_node_ranking.json").write_text(json.dumps(ranked, indent=2))
    ranking_txt = nr.format_ranking(ranked)
    (OUTPUT_DIR / "ev_node_ranking.txt").write_text(ranking_txt)
    print("\n" + ranking_txt + "\n")

    m = nr.build_map(stations, ranked, backtest)
    map_path = OUTPUT_DIR / f"ev_planned_chargers_map_{stamp}.html"
    m.save(str(map_path))
    log.info("Map saved → %s", map_path)

    # Top-line summary tying the two halves together
    top = next((r for r in ranked if r["dcfc_ports"] > 0), None)
    if top:
        proj = top.get("proj_vol_uplift_pct")
        proj_s = f" → projected {proj:+.1f}% volatility uplift" if proj else ""
        log.info("Top node: %s %s with %d planned DCFC ports%s",
                 top["iso"], top["zone"], top["dcfc_ports"], proj_s)


if __name__ == "__main__":
    main()
