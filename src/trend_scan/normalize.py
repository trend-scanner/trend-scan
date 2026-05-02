from __future__ import annotations

from typing import Any

from .config import REPO_ROOT
from .storage import raw_path
from .tagging import has_signal_terms, infer_tags, merge_tags


LAYER_BY_SOURCE = {
    "rss": "NEWS",
    "hackernews": "REACTION",
    "github": "BEHAVIOR",
    "wikipedia": "INTEREST",
    "polymarket": "EXPECTATION",
}


def _rss_records(run_date_str: str, raw: dict[str, Any], settings: dict) -> list[dict[str, Any]]:
    keyword_map = settings["keyword_map"]
    keywords_config = settings["keywords"]
    rows: list[dict[str, Any]] = []
    raw_ref = str(raw_path(run_date_str, "rss").relative_to(REPO_ROOT))

    for item in raw.get("items", []):
        tags = merge_tags(
            item.get("tags", []),
            infer_tags(keyword_map, item.get("title"), item.get("summary"), item.get("source_name")),
        )
        rows.append(
            {
                "date": run_date_str,
                "source": "rss",
                "region": item.get("region", "global"),
                "layer": LAYER_BY_SOURCE["rss"],
                "entity_key": str(item.get("item_id") or item.get("url") or item.get("title")),
                "title": item.get("title"),
                "url": item.get("url"),
                "published_at": item.get("published_at"),
                "metrics": {},
                "tags": tags,
                "raw_ref": raw_ref,
                "attributes": {
                    "source_id": item.get("source_id"),
                    "source_name": item.get("source_name"),
                    "source_kind": item.get("source_kind"),
                    "priority": bool(item.get("priority")),
                    "authors": item.get("authors", []),
                    "summary": item.get("summary"),
                    "signal_candidate": has_signal_terms(
                        keywords_config,
                        item.get("title"),
                        item.get("summary"),
                    ),
                },
            }
        )
    return rows


def _hackernews_records(run_date_str: str, raw: dict[str, Any], settings: dict) -> list[dict[str, Any]]:
    keyword_map = settings["keyword_map"]
    rows: list[dict[str, Any]] = []
    raw_ref = str(raw_path(run_date_str, "hackernews").relative_to(REPO_ROOT))

    for item in raw.get("items", []):
        tags = merge_tags(
            item.get("tags", []),
            infer_tags(keyword_map, item.get("title"), item.get("url")),
        )
        rows.append(
            {
                "date": run_date_str,
                "source": "hackernews",
                "region": "global",
                "layer": LAYER_BY_SOURCE["hackernews"],
                "entity_key": str(item.get("item_id")),
                "title": item.get("title"),
                "url": item.get("url"),
                "published_at": item.get("created_at"),
                "metrics": {
                    "score": item.get("points", 0),
                    "comments": item.get("comments", 0),
                },
                "tags": tags,
                "raw_ref": raw_ref,
                "attributes": {
                    "author": item.get("author"),
                    "matched_queries": item.get("matched_queries", []),
                },
            }
        )
    return rows


def _github_records(run_date_str: str, raw: dict[str, Any], settings: dict) -> list[dict[str, Any]]:
    keyword_map = settings["keyword_map"]
    rows: list[dict[str, Any]] = []
    raw_ref = str(raw_path(run_date_str, "github").relative_to(REPO_ROOT))

    for item in raw.get("items", []):
        tags = merge_tags(
            item.get("tags", []),
            infer_tags(keyword_map, item.get("repo_name"), item.get("description"), item.get("language")),
        )
        rows.append(
            {
                "date": run_date_str,
                "source": "github",
                "region": "global",
                "layer": LAYER_BY_SOURCE["github"],
                "entity_key": str(item.get("repo_name")),
                "title": item.get("repo_name"),
                "url": item.get("url"),
                "published_at": item.get("pushed_at") or item.get("updated_at"),
                "metrics": {
                    "stars": item.get("stars", 0),
                    "forks": item.get("forks", 0),
                    "watchers": item.get("watchers", 0),
                    "open_issues": item.get("open_issues", 0),
                },
                "tags": tags,
                "raw_ref": raw_ref,
                "attributes": {
                    "description": item.get("description"),
                    "language": item.get("language"),
                    "topics": item.get("topics", []),
                    "matched_queries": item.get("matched_queries", []),
                    "created_at": item.get("created_at"),
                },
            }
        )
    return rows


def _wikipedia_records(run_date_str: str, raw: dict[str, Any], settings: dict) -> list[dict[str, Any]]:
    keyword_map = settings["keyword_map"]
    rows: list[dict[str, Any]] = []
    raw_ref = str(raw_path(run_date_str, "wikipedia").relative_to(REPO_ROOT))

    for item in raw.get("items", []):
        tags = merge_tags(
            item.get("tags", []),
            infer_tags(keyword_map, item.get("page_title"), item.get("project")),
        )
        rows.append(
            {
                "date": run_date_str,
                "source": "wikipedia",
                "region": item.get("region", "global"),
                "layer": LAYER_BY_SOURCE["wikipedia"],
                "entity_key": f"{item.get('project')}:{item.get('page_title')}",
                "title": item.get("page_title"),
                "url": f"https://{item.get('project')}/wiki/{str(item.get('page_title')).replace(' ', '_')}",
                "published_at": f"{item.get('observed_date')}T00:00:00Z",
                "metrics": {
                    "views": item.get("views", 0),
                },
                "tags": tags,
                "raw_ref": raw_ref,
                "attributes": {
                    "project": item.get("project"),
                    "observed_date": item.get("observed_date"),
                },
            }
        )
    return rows


def _polymarket_records(run_date_str: str, raw: dict[str, Any], settings: dict) -> list[dict[str, Any]]:
    keyword_map = settings["keyword_map"]
    rows: list[dict[str, Any]] = []
    raw_ref = str(raw_path(run_date_str, "polymarket").relative_to(REPO_ROOT))

    for item in raw.get("items", []):
        tags = merge_tags(
            item.get("tags", []),
            infer_tags(keyword_map, item.get("title"), item.get("category")),
        )
        max_price = max(
            (outcome.get("price") or 0.0) for outcome in item.get("outcomes", [])
        ) if item.get("outcomes") else None
        rows.append(
            {
                "date": run_date_str,
                "source": "polymarket",
                "region": "global",
                "layer": LAYER_BY_SOURCE["polymarket"],
                "entity_key": str(item.get("market_id") or item.get("slug") or item.get("title")),
                "title": item.get("title"),
                "url": item.get("url"),
                "published_at": item.get("end_date"),
                "metrics": {
                    "volume": item.get("volume", 0.0),
                    "liquidity": item.get("liquidity", 0.0),
                    "max_price": max_price,
                },
                "tags": tags,
                "raw_ref": raw_ref,
                "attributes": {
                    "category": item.get("category"),
                    "end_date": item.get("end_date"),
                    "outcomes": item.get("outcomes", []),
                },
            }
        )
    return rows


def normalize_day(raw_payloads: dict[str, dict[str, Any]], run_date_str: str, settings: dict) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    source_builders = {
        "rss": _rss_records,
        "hackernews": _hackernews_records,
        "github": _github_records,
        "wikipedia": _wikipedia_records,
        "polymarket": _polymarket_records,
    }

    for source_name, builder in source_builders.items():
        payload = raw_payloads.get(source_name)
        if payload:
            records.extend(builder(run_date_str, payload, settings))

    records.sort(key=lambda row: (row["source"], row["title"] or ""))
    return records
