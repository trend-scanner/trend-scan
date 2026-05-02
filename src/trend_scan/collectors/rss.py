from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import feedparser

from ..date_utils import RunContext


def _published_at(entry: dict[str, Any]) -> str | None:
    for key in ("published_parsed", "updated_parsed"):
        value = entry.get(key)
        if value:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", value)

    for key in ("published", "updated"):
        value = entry.get(key)
        if value:
            return str(value)
    return None


def _published_timestamp(entry: dict[str, Any]) -> float | None:
    for key in ("published_parsed", "updated_parsed"):
        value = entry.get(key)
        if value:
            return float(time.mktime(value))
    return None


def _author_list(entry: dict[str, Any]) -> list[str]:
    authors = entry.get("authors") or []
    names = [author.get("name") for author in authors if author.get("name")]
    if names:
        return names

    if entry.get("author"):
        return [str(entry["author"])]
    return []


def collect(context: RunContext, settings: dict) -> dict[str, Any]:
    source_config = settings["sources"].get("rss", {})
    feeds = source_config.get("feeds", [])
    max_entries_per_feed = int(source_config.get("max_entries_per_feed", 40))
    max_item_age_days = int(source_config.get("max_item_age_days", 7))
    cutoff = context.snapshot_at - timedelta(days=max_item_age_days)
    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for feed in feeds:
        parsed = feedparser.parse(feed["url"])
        if parsed.bozo:
            errors.append({"source_id": feed["id"], "error": str(parsed.bozo_exception)})
            continue

        kept = 0
        for entry in parsed.entries:
            published_ts = _published_timestamp(entry)
            if published_ts is not None and published_ts < cutoff.timestamp():
                continue

            entry_tags = [tag.get("term") for tag in entry.get("tags", []) if tag.get("term")]
            items.append(
                {
                    "item_id": entry.get("id") or entry.get("link") or entry.get("title"),
                    "source_id": feed["id"],
                    "source_name": feed["name"],
                    "source_kind": feed.get("kind", "news"),
                    "priority": bool(feed.get("priority")),
                    "region": feed.get("region", "global"),
                    "title": entry.get("title"),
                    "url": entry.get("link"),
                    "published_at": _published_at(entry),
                    "summary": entry.get("summary") or entry.get("description"),
                    "authors": _author_list(entry),
                    "tags": entry_tags,
                }
            )
            kept += 1
            if kept >= max_entries_per_feed:
                break

    return {
        "source": "rss",
        "run_date": context.run_date_str,
        "snapshot_at": context.snapshot_at.isoformat(),
        "items": items,
        "meta": {
            "feed_count": len(feeds),
            "item_count": len(items),
            "errors": errors,
        },
    }
