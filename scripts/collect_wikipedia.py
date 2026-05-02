from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trend_scan.collectors import wikipedia  # noqa: E402
from trend_scan.config import load_settings  # noqa: E402
from trend_scan.date_utils import build_run_context  # noqa: E402
from trend_scan.storage import raw_path, write_json  # noqa: E402
from trend_scan.tagging import compile_keyword_map  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Run date in YYYY-MM-DD format.")
    args = parser.parse_args()

    settings = load_settings()
    settings["keyword_map"] = compile_keyword_map(settings["keywords"])
    context = build_run_context(args.date, settings)
    payload = wikipedia.collect(context, settings)
    write_json(raw_path(context.run_date_str, "wikipedia"), payload)


if __name__ == "__main__":
    main()
