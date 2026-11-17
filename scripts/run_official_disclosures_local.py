#!/usr/bin/env python3
"""Fetch official exchange/regulator disclosures and save normalized JSON locally."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.finsights_scraping.domain.windows import DateWindow
from src.finsights_scraping.sources.disclosures import build_client


def main() -> int:
    parser = argparse.ArgumentParser(description="Local smoke/backfill runner for section 2 official data sources")
    parser.add_argument("--source", choices=["hose", "hnx", "ubcknn", "vsdc", "all"], required=True)
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD, Asia/Ho_Chi_Minh")
    parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD, Asia/Ho_Chi_Minh")
    parser.add_argument("--output-dir", default=str(ROOT / "data" / "disclosures"))
    args = parser.parse_args()

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
    start_date = args.start_date or args.end_date or today
    end_date = args.end_date or args.start_date or today
    window = DateWindow.parse(start_date, end_date)
    sources = ["hose", "hnx", "ubcknn", "vsdc"] if args.source == "all" else [args.source]
    summaries = []
    failed = False

    for source_key in sources:
        config_path = ROOT / "configs" / "sources" / "disclosures" / f"{source_key}.yaml"
        with config_path.open("r", encoding="utf-8") as stream:
            config = yaml.safe_load(stream)
        client = build_client(source_key, config)
        try:
            records = client.collect(window)
            output_dir = Path(args.output_dir) / source_key
            output_dir.mkdir(parents=True, exist_ok=True)
            filename = f"{start_date}.json" if start_date == end_date else f"{start_date}_to_{end_date}.json"
            output_path = output_dir / filename
            payload = {
                "source": source_key,
                "source_code": config["source_code"],
                "start_date": start_date,
                "end_date": end_date,
                "items_count": len(records),
                "fetch_attempts": client.fetch_attempts,
                "items": [record.__dict__ for record in records],
            }
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            summaries.append({
                "source": source_key,
                "status": "SUCCESS",
                "items_count": len(records),
                "output_path": str(output_path),
                "fetch_attempts": len(client.fetch_attempts),
            })
            print(f"{source_key}: {len(records)} records -> {output_path}")
        except Exception as exc:
            failed = True
            summaries.append({"source": source_key, "status": "FAILED", "error": f"{type(exc).__name__}: {exc}"})
            print(f"{source_key}: FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        finally:
            client.close()

    print(json.dumps({"window": f"{start_date}..{end_date}", "results": summaries}, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
