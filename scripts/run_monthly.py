from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()

    command = [sys.executable, str(ROOT / "scripts" / "generate_periodic_report.py"), "--period", "monthly"]
    if args.date:
        command.extend(["--date", args.date])
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
