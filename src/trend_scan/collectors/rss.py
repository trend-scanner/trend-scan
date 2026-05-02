from __future__ import annotations

import time
from datetime import datetime, timedelta
import re
from typing import Any
from urllib.parse import urljoin

import feedparser
import requests
from bs4 import BeautifulSoup

from ..date_utils import RunContext


MONTHS_PATTERN = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
DATE_PATTERN = rf"{MONTHS_PATTERN}\s+\d{{1,2}},\s+\d{{4}}"
ANTHROPIC_CATEGORIES = (
    "Announcements",
    "Economic Research",
    "Engineering",
    "Policy",
    "Product",
    "Research",
    "Safety",
    "Societal Impacts",
)


def _date_to_iso(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%b %d, %Y").strftime("%Y-%m-%dT00:00:00Z")
    except ValueError:
        return None


def _strip_known_category(value: str) -> str:
    text = value.strip()
    for category in ANTHROPIC_CATEGORIES:
        prefix = f"{category} "
        if text.startswith(prefix):
            return text[len(prefix) :].strip()
    return text


def _entry_from_youtube_card(anchor, feed: dict[str, Any]) -> dict[str, Any] | None:
    title = " ".join(anchor.get_text(" ", strip=True).split())
    href = anchor.get("href")
    if not title or not href:
        return None

    if title in {"News & Events", "Creator & Artist Stories", "Culture & Trends", "Inside YouTube", "Made On YouTube"}:
        return None

    url = urljoin(feed["url"], href)
    if not url.startswith("https://blog.youtube/"):
        return None
    if "/topic-hub/" in url or "/search/" in url or url.rstrip("/") == feed["url"].rstrip("/"):
        return None

    container = anchor
    for _ in range(5):
        if container.parent is None:
            break
        container = container.parent

    text = " ".join(container.get_text(" ", strip=True).split())
    return {
        "item_id": url,
        "source_id": feed["id"],
        "source_name": feed["name"],
        "source_kind": feed.get("kind", "news"),
        "priority": bool(feed.get("priority")),
        "region": feed.get("region", "global"),
        "title": title,
        "url": url,
        "published_at": None,
        "summary": text[:800] if text and text != title else None,
        "authors": [],
        "tags": ["youtube"],
    }


def _collect_youtube_blog_html(feed: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    try:
        response = requests.get(
            feed["url"],
            headers={"User-Agent": "trend-scan/0.1"},
            timeout=30,
        )
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return [], str(exc)

    soup = BeautifulSoup(response.text, "html.parser")
    items_by_url: dict[str, dict[str, Any]] = {}
    for anchor in soup.find_all("a", href=True):
        item = _entry_from_youtube_card(anchor, feed)
        if item:
            items_by_url.setdefault(item["url"], item)

    return list(items_by_url.values())[:40], None


def _entry_from_anthropic_card(anchor, feed: dict[str, Any]) -> dict[str, Any] | None:
    href = anchor.get("href")
    text = " ".join(anchor.get_text(" ", strip=True).split())
    if not href or not text:
        return None

    url = urljoin(feed["url"], href)
    if not url.startswith("https://www.anthropic.com/news/"):
        return None

    date_match = re.search(DATE_PATTERN, text)
    published_at = _date_to_iso(date_match.group(0) if date_match else None)

    if date_match and date_match.start() == 0:
        title = _strip_known_category(text[date_match.end() :])
    elif date_match:
        title = text[: date_match.start()]
        for category in ANTHROPIC_CATEGORIES:
            suffix = f" {category}"
            if title.endswith(suffix):
                title = title[: -len(suffix)]
                break
    else:
        title = text

    title = title.strip()
    if not title:
        return None

    return {
        "item_id": url,
        "source_id": feed["id"],
        "source_name": feed["name"],
        "source_kind": feed.get("kind", "news"),
        "priority": bool(feed.get("priority")),
        "region": feed.get("region", "global"),
        "title": title[:180],
        "url": url,
        "published_at": published_at,
        "summary": text[:800] if text != title else None,
        "authors": [],
        "tags": ["anthropic"],
    }


def _collect_anthropic_news_html(feed: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    try:
        response = requests.get(
            feed["url"],
            headers={"User-Agent": "trend-scan/0.1"},
            timeout=30,
        )
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return [], str(exc)

    soup = BeautifulSoup(response.text, "html.parser")
    items_by_url: dict[str, dict[str, Any]] = {}
    for anchor in soup.find_all("a", href=True):
        item = _entry_from_anthropic_card(anchor, feed)
        if item:
            items_by_url.setdefault(item["url"], item)

    return list(items_by_url.values())[:40], None


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
        if feed.get("parser") == "youtube_blog_html":
            html_items, error = _collect_youtube_blog_html(feed)
            items.extend(html_items)
            if error:
                errors.append({"source_id": feed["id"], "error": error})
            continue

        if feed.get("parser") == "anthropic_news_html":
            html_items, error = _collect_anthropic_news_html(feed)
            items.extend(html_items)
            if error:
                errors.append({"source_id": feed["id"], "error": error})
            continue

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
