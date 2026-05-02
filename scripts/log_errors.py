from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trend_scan.config import load_settings  # noqa: E402
from trend_scan.date_utils import build_run_context  # noqa: E402
from trend_scan.error_logging import SOURCE_NAMES, build_error_log  # noqa: E402
from trend_scan.storage import error_log_path, error_state_path, raw_path, read_json, write_json  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Run date in YYYY-MM-DD format.")
    args = parser.parse_args()

    settings = load_settings()
    context = build_run_context(args.date, settings)
    raw_payloads = {
        source_name: read_json(raw_path(context.run_date_str, source_name), default={})
        for source_name in SOURCE_NAMES
    }
    previous_state = read_json(error_state_path(), default={"sources": {}})
    error_log, error_state = build_error_log(raw_payloads, previous_state, context.run_date_str)
    write_json(error_log_path(context.run_date_str), error_log)
    write_json(error_state_path(), error_state)


if __name__ == "__main__":
    main()
