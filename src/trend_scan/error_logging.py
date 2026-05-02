from __future__ import annotations

from collections import Counter
from typing import Any


SOURCE_NAMES = ("rss", "hackernews", "github", "wikipedia", "polymarket")


def _entry_key(source_name: str, level: str, entry: dict[str, Any]) -> str:
    detail_id = (
        entry.get("source_id")
        or entry.get("query")
        or entry.get("title")
        or entry.get("source")
        or source_name
    )
    return f"{source_name}:{level}:{detail_id}"


def _normalize_entry(
    source_name: str,
    level: str,
    entry: dict[str, Any],
    run_date_str: str,
) -> dict[str, Any]:
    message = entry.get("error") or entry.get("warning") or "Unknown issue"
    detail_id = (
        entry.get("source_id")
        or entry.get("query")
        or entry.get("title")
        or entry.get("source")
        or source_name
    )
    return {
        "key": _entry_key(source_name, level, entry),
        "date": run_date_str,
        "source": source_name,
        "level": level,
        "detail_id": detail_id,
        "message": str(message),
        "raw": entry,
    }


def collect_error_entries(
    raw_payloads: dict[str, dict[str, Any] | None],
    run_date_str: str,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for source_name in SOURCE_NAMES:
        payload = raw_payloads.get(source_name) or {}
        meta = payload.get("meta", {})
        for entry in meta.get("errors", []):
            entries.append(_normalize_entry(source_name, "error", entry, run_date_str))
        for entry in meta.get("warnings", []):
            entries.append(_normalize_entry(source_name, "warning", entry, run_date_str))
    return entries


def update_error_state(
    previous_state: dict[str, Any] | None,
    entries: list[dict[str, Any]],
    run_date_str: str,
) -> dict[str, Any]:
    previous_errors = (previous_state or {}).get("sources", {})
    state_entries = [entry for entry in entries if entry["level"] == "error"]
    active_keys = {entry["key"] for entry in state_entries}
    entries_by_key = {entry["key"]: entry for entry in state_entries}
    sources: dict[str, dict[str, Any]] = {}

    for key in sorted(active_keys):
        previous = previous_errors.get(key, {})
        entry = entries_by_key[key]
        sources[key] = {
            "source": entry["source"],
            "level": entry["level"],
            "detail_id": entry["detail_id"],
            "message": entry["message"],
            "first_seen": previous.get("first_seen", run_date_str),
            "last_seen": run_date_str,
            "consecutive_days": int(previous.get("consecutive_days", 0)) + 1,
        }

    resolved = []
    for key, previous in previous_errors.items():
        if key not in active_keys:
            resolved.append(
                {
                    "key": key,
                    "source": previous.get("source"),
                    "level": previous.get("level"),
                    "detail_id": previous.get("detail_id"),
                    "last_seen": previous.get("last_seen"),
                    "consecutive_days": previous.get("consecutive_days", 0),
                    "resolved_on": run_date_str,
                }
            )

    return {
        "date": run_date_str,
        "sources": sources,
        "resolved": resolved,
        "summary": {
            "active_count": len(sources),
            "resolved_count": len(resolved),
            "active_by_level": dict(Counter(entry["level"] for entry in state_entries)),
            "active_by_source": dict(Counter(entry["source"] for entry in state_entries)),
        },
    }


def build_error_log(
    raw_payloads: dict[str, dict[str, Any] | None],
    previous_state: dict[str, Any] | None,
    run_date_str: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    entries = collect_error_entries(raw_payloads, run_date_str)
    state = update_error_state(previous_state, entries, run_date_str)
    log = {
        "date": run_date_str,
        "entries": entries,
        "summary": {
            "entry_count": len(entries),
            "by_level": dict(Counter(entry["level"] for entry in entries)),
            "by_source": dict(Counter(entry["source"] for entry in entries)),
            "continuous_failures": [
                {
                    "key": key,
                    **value,
                }
                for key, value in state["sources"].items()
                if value["consecutive_days"] >= 2
            ],
        },
    }
    return log, state
