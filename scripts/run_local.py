#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.finsights_scraping.local_runner import ALL_SOURCES, SOURCE_CHOICES, LocalRunner


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Finsights Local Mode Data Collection - Run scrapers locally and save JSON to data/"
    )
    parser.add_argument(
        "--source",
        "-s",
        default="all",
        choices=[*SOURCE_CHOICES, "all"],
        help="Source to run; all runs the five Financial News RSS publishers",
    )
    parser.add_argument(
        "--date",
        "-d",
        help="Legacy single-day option (YYYY-MM-DD); use --start-date/--end-date for a range",
    )
    parser.add_argument(
        "--start-date",
        help="Inclusive start date (YYYY-MM-DD, Asia/Ho_Chi_Minh)",
    )
    parser.add_argument(
        "--end-date",
        help="Inclusive end date (YYYY-MM-DD, Asia/Ho_Chi_Minh)",
    )
    parser.add_argument(
        "--data-dir",
        default=str(ROOT / "data"),
        help="Output directory path (default: data/ in repository root)",
    )
    args = parser.parse_args()

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    if args.date and (args.start_date or args.end_date):
        parser.error("--date cannot be combined with --start-date or --end-date")
    if args.date:
        start_date = end_date = args.date
    else:
        start_date = args.start_date or args.end_date or today
        end_date = args.end_date or args.start_date or today

    print("=" * 70)
    print("FINSIGHTS DATA COLLECTION - LOCAL MODE")
    print(f"Date Window : {start_date} -> {end_date}")
    print(f"Source(s)   : {args.source}")
    print(f"Output Dir  : {args.data_dir}")
    print("=" * 70)

    runner = LocalRunner(data_dir=args.data_dir)
    results = asyncio.run(runner.run(args.source, start_date, end_date))

    print("\n" + "=" * 70)
    print("RUN SUMMARY")
    print("=" * 70)
    print(json.dumps(results, ensure_ascii=False, indent=2))

    success = all(r.get("status") in {"SUCCESS", "PARTIAL"} for r in results)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())

