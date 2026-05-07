from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trend_scan.config import load_settings  # noqa: E402
from trend_scan.date_utils import build_run_context  # noqa: E402
from trend_scan.signals import detect_signals  # noqa: E402
from trend_scan.storage import (  # noqa: E402
    normalized_path,
    previous_normalized_path,
    read_jsonl,
    signals_path,
    write_json,
)
from trend_scan.tagging import compile_keyword_map  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Run date in YYYY-MM-DD format.")
    args = parser.parse_args()

    settings = load_settings()
    settings["keyword_map"] = compile_keyword_map(settings["keywords"])
    context = build_run_context(args.date, settings)
    current = read_jsonl(normalized_path(context.run_date_str))
    previous = read_jsonl(previous_normalized_path(context.run_date))
    history = []
    for offset in range(1, 31):
        history_date = context.run_date - timedelta(days=offset)
        history.extend(read_jsonl(normalized_path(history_date.isoformat())))

    signal_config = settings["sources"].get("signals", {})
    payload = detect_signals(
        current,
        previous,
        context.run_date_str,
        tracked_tags=list(settings["keyword_map"].keys()),
        history_records=history,
        top_limit=int(signal_config.get("top_limit", 15)),
        important_limit=int(signal_config.get("important_limit", 40)),
    )
    write_json(signals_path(context.run_date_str), payload)


if __name__ == "__main__":
    main()
