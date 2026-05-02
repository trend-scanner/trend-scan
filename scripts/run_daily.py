from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Run date in YYYY-MM-DD format.")
    args = parser.parse_args()

    commands = [
        "collect_rss.py",
        "collect_hackernews.py",
        "collect_github.py",
        "collect_wikipedia.py",
        "collect_polymarket.py",
        "normalize.py",
        "log_errors.py",
        "detect_signals.py",
        "generate_daily_report.py",
    ]

    for script_name in commands:
        command = [sys.executable, str(ROOT / "scripts" / script_name)]
        if args.date:
            command.extend(["--date", args.date])
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
