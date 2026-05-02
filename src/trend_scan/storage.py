from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .config import REPO_ROOT


def raw_dir(run_date_str: str) -> Path:
    return REPO_ROOT / "data" / "raw" / run_date_str


def raw_path(run_date_str: str, source_name: str) -> Path:
    return raw_dir(run_date_str) / f"{source_name}.json"


def normalized_path(run_date_str: str) -> Path:
    return REPO_ROOT / "data" / "normalized" / f"{run_date_str}.jsonl"


def signals_path(run_date_str: str) -> Path:
    return REPO_ROOT / "data" / "signals" / f"{run_date_str}_signals.json"


def daily_report_path(run_date_str: str) -> Path:
    return REPO_ROOT / "reports" / "daily" / f"{run_date_str}.md"


def previous_normalized_path(run_date: date) -> Path:
    previous = run_date - timedelta(days=1)
    return normalized_path(previous.isoformat())


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows
