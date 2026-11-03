#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.finsights_scraping.local_runner import ALL_SOURCES, LocalRunner


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Finsights Local Mode Data Collection - Run scrapers locally and save JSON to data/"
    )
    parser.add_argument(
        "--source",
        "-s",
        default="all",
        choices=[*ALL_SOURCES, "all"],
        help="Source to run (cafef, baomoi, thanhnien, tuoitre, vnexpress, vietstock, or all)",
    )
    parser.add_argument(
        "--date",
        "-d",
        default="2026-11-02",
        help="Target publication/trade date (YYYY-MM-DD), default: 2026-11-02",
    )
    parser.add_argument(
        "--data-dir",
        default=str(ROOT / "data"),
        help="Output directory path (default: data/ in repository root)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("FINSIGHTS DATA COLLECTION - LOCAL MODE")
    print(f"Target Date : {args.date}")
    print(f"Source(s)   : {args.source}")
    print(f"Output Dir  : {args.data_dir}")
    print("=" * 70)

    runner = LocalRunner(data_dir=args.data_dir)
    results = asyncio.run(runner.run(args.source, args.date))

    print("\n" + "=" * 70)
    print("RUN SUMMARY")
    print("=" * 70)
    print(json.dumps(results, ensure_ascii=False, indent=2))

    success = all(r.get("status") == "SUCCESS" for r in results)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())

