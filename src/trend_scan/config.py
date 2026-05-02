from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_yaml(relative_path: str) -> dict[str, Any]:
    path = REPO_ROOT / relative_path
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Expected a mapping in {path}")

    return data


def load_settings() -> dict[str, dict[str, Any]]:
    return {
        "sources": load_yaml("config/sources.yml"),
        "watchlists": load_yaml("config/watchlists.yml"),
        "keywords": load_yaml("config/keywords.yml"),
    }
