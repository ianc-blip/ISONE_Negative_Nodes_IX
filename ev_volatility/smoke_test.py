"""
smoke_test.py
============
Offline validation of the NREL → backtest → ranking path.

NREL's host is egress-blocked in some sandboxes, so this drives the pipeline
from a saved NREL-shaped payload (seed_data/nrel_sample_payload.json) instead of
a live call. The LMP prices the backtest pulls are still REAL NYISO data, so this
also confirms the volatility/price math end-to-end.

    python smoke_test.py
"""

import datetime
import json

import nrel_stations as nrel
import node_ranking as nr
import volatility_backtest as bt

AS_OF = datetime.date(2026, 7, 5)


def main():
    payload = json.load(open(nrel.SEED_DIR / "nrel_sample_payload.json"))
    stations = nrel.from_payload(payload)
    assert stations and all("open_date" in s for s in stations), "parse failed"
    print(f"[ok] parsed {len(stations)} NREL stations")

    events = nrel.historical_openings(stations, years=3, as_of=AS_OF)
    assert events, "no events extracted"
    assert all(e["dcfc_ports"] >= 5 for e in events), "large-DCFC filter broken"
    print(f"[ok] {len(events)} large DCFC openings in last 3y")

    report = bt.run_backtest(events)
    assert report["n_events"] > 0, "backtest produced no events"
    for key in ("hourly_std", "daily_range", "spike_share", "mean_lbmp"):
        assert key in report["aggregate"], f"missing metric {key}"
    print(f"[ok] backtest ran on {report['n_events']} events "
          f"(NYISO + ISO-NE, real prices)")
    print(bt.format_report(report))

    ranked = nr.apply_calibration(nr.rank_nodes(stations), report)
    assert ranked and ranked[0]["rank"] == 1
    print("\n" + nr.format_ranking(ranked, top=8))
    print("\n[ok] smoke test passed")


if __name__ == "__main__":
    main()
