from __future__ import annotations

from typing import Any

from ..date_utils import RunContext
from ..http import build_session, get_json
from ..tagging import infer_tags


def collect(context: RunContext, settings: dict) -> dict[str, Any]:
    source_config = settings["sources"].get("hackernews", {})
    watchlists = settings["watchlists"]
    keyword_map = settings["keyword_map"]
    session = build_session()

    endpoint = source_config["endpoint"]
    queries = watchlists.get("hackernews_queries", [])
    hits_per_query = int(source_config.get("hits_per_query", 25))
    window_hours = int(source_config.get("window_hours", 24))
    created_after = int(context.snapshot_at.timestamp()) - (window_hours * 3600)

    deduped: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []

    for query in queries:
        params = {
            "query": query,
            "tags": "story",
            "numericFilters": f"created_at_i>{created_after}",
            "hitsPerPage": hits_per_query,
        }

        try:
            payload = get_json(session, endpoint, params=params)
        except Exception as exc:  # noqa: BLE001
            errors.append({"query": query, "error": str(exc)})
            continue

        for hit in payload.get("hits", []):
            object_id = str(hit.get("objectID"))
            title = hit.get("title") or hit.get("story_title")
            url = hit.get("url") or hit.get("story_url") or f"https://news.ycombinator.com/item?id={object_id}"
            tags = infer_tags(keyword_map, title, url, query)

            if object_id not in deduped:
                deduped[object_id] = {
                    "item_id": object_id,
                    "title": title,
                    "url": url,
                    "author": hit.get("author"),
                    "points": hit.get("points") or 0,
                    "comments": hit.get("num_comments") or 0,
                    "created_at": hit.get("created_at"),
                    "matched_queries": [query],
                    "tags": tags,
                }
            else:
                deduped[object_id]["matched_queries"].append(query)
                deduped[object_id]["tags"] = sorted(
                    set(deduped[object_id]["tags"] + tags)
                )

    items = sorted(deduped.values(), key=lambda item: (item["points"], item["comments"]), reverse=True)
    return {
        "source": "hackernews",
        "run_date": context.run_date_str,
        "snapshot_at": context.snapshot_at.isoformat(),
        "items": items,
        "meta": {
            "query_count": len(queries),
            "item_count": len(items),
            "errors": errors,
        },
    }
