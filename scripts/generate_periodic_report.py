from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trend_scan.config import load_settings  # noqa: E402
from trend_scan.date_utils import build_run_context  # noqa: E402
from trend_scan.reporting import build_periodic_report, report_dates  # noqa: E402
from trend_scan.storage import (  # noqa: E402
    monthly_report_path,
    normalized_path,
    read_json,
    read_jsonl,
    signals_path,
    weekly_report_path,
)


def _period_id(period: str, end_date: date) -> str:
    if period == "weekly":
        year, week, _ = end_date.isocalendar()
        return f"{year}-W{week:02d}"
    if period == "monthly":
        return end_date.strftime("%Y-%m")
    raise ValueError(f"Unsupported period: {period}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", choices=["weekly", "monthly"], required=True)
    parser.add_argument("--date", help="End date in YYYY-MM-DD format. Defaults to today's run date.")
    args = parser.parse_args()

    settings = load_settings()
    context = build_run_context(args.date, settings)
    dates = report_dates(context.run_date, args.period)
    records = []
    signal_payloads = []
    for day in dates:
        run_date_str = day.isoformat()
        records.extend(read_jsonl(normalized_path(run_date_str)))
        signal_payload = read_json(signals_path(run_date_str), default=None)
        if signal_payload:
            signal_payloads.append(signal_payload)

    period_id = _period_id(args.period, context.run_date)
    report = build_periodic_report(
        period=args.period,
        period_id=period_id,
        start_date=dates[0],
        end_date=dates[-1],
        normalized_records=records,
        signal_payloads=signal_payloads,
    )

    path = weekly_report_path(period_id) if args.period == "weekly" else monthly_report_path(period_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
