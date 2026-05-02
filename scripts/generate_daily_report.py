from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trend_scan.config import load_settings  # noqa: E402
from trend_scan.date_utils import build_run_context  # noqa: E402
from trend_scan.reporting import build_daily_report  # noqa: E402
from trend_scan.storage import (  # noqa: E402
    daily_report_path,
    normalized_path,
    read_json,
    read_jsonl,
    signals_path,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Run date in YYYY-MM-DD format.")
    args = parser.parse_args()

    settings = load_settings()
    context = build_run_context(args.date, settings)
    normalized_records = read_jsonl(normalized_path(context.run_date_str))
    signal_payload = read_json(signals_path(context.run_date_str), default={"signals": []})
    report = build_daily_report(context.run_date_str, normalized_records, signal_payload)

    path = daily_report_path(context.run_date_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
