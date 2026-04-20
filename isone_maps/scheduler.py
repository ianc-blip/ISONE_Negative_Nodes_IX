"""
scheduler.py
============
Run the ISONE map pipeline on the 1st of every month.

Usage
-----
# Run once (manual / cron):
    python scheduler.py --run-now

# Start the long-running scheduler (blocks until killed):
    python scheduler.py --daemon

# Cron alternative (recommended for servers) — add to crontab:
    0 6 1 * * cd /path/to/isone_maps && python scheduler.py --run-now >> logs/scheduler.log 2>&1

Environment variables (optional):
    ISONE_USER        ISONE API username
    ISONE_PASS        ISONE API password
    ISONE_OUTPUT_DIR  Where to write HTML files    (default: ./output)
    ISONE_SEED_DIR    Where seed JSON files live   (default: ./seed_data)
    ISONE_REFRESH     Set to "1" to pull live data (default: 0 / seed only)
"""

import os, sys, logging, datetime, argparse
from pathlib import Path

import schedule
import time

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent))
from map_generator import run as generate_maps
from isone_data_fetcher import refresh_all_seed_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/scheduler.log", mode="a"),
    ],
)
log = logging.getLogger("scheduler")


def monthly_job():
    today = datetime.date.today()
    log.info("=== Monthly ISONE map run: %s ===", today)

    seed_dir   = Path(os.environ.get("ISONE_SEED_DIR",   "./seed_data"))
    output_dir = Path(os.environ.get("ISONE_OUTPUT_DIR", "./output"))
    api_user   = os.environ.get("ISONE_USER")
    api_pass   = os.environ.get("ISONE_PASS")
    do_refresh = os.environ.get("ISONE_REFRESH", "0") == "1"

    output_dir.mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    # Step 1: optionally refresh seed data from live ISONE sources
    if do_refresh:
        log.info("Refreshing seed data from ISONE APIs…")
        try:
            counts = refresh_all_seed_data(
                seed_dir,
                years=[today.year - 1, today.year],
                api_user=api_user,
                api_pass=api_pass,
            )
            for k, v in counts.items():
                log.info("  %s: %d nodes", k, v)
        except Exception as e:
            log.error("Seed refresh failed: %s — using existing seed data", e)

    # Step 2: generate the three HTML maps
    try:
        saved = generate_maps(
            api_user=api_user,
            api_pass=api_pass,
            output_dir=output_dir,
            seed_dir=seed_dir,
            run_date=today,
        )
        log.info("Maps written: %s", saved)
    except Exception as e:
        log.exception("Map generation failed: %s", e)


def main():
    Path("logs").mkdir(exist_ok=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-now", action="store_true",
                        help="Run the job immediately and exit")
    parser.add_argument("--daemon", action="store_true",
                        help="Run as a long-lived scheduler (blocks)")
    parser.add_argument("--day", type=int, default=1,
                        help="Day of month to run (default: 1)")
    args = parser.parse_args()

    if args.run_now:
        monthly_job()
        return

    if args.daemon:
        # Schedule for the Nth of every month at 06:00 local time
        day_str = f"{args.day:02d}"
        schedule.every().month.at(f"06:00").do(monthly_job)
        log.info("Scheduler started — will run on day %s of each month at 06:00", day_str)
        while True:
            schedule.run_pending()
            time.sleep(60)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
