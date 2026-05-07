from __future__ import annotations

import os
import time
from typing import Any

import requests

from ..date_utils import RunContext, expand_template
from ..http import build_session, get_json
from ..tagging import infer_tags, merge_tags


def _github_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.getenv("TREND_SCAN_GITHUB_TOKEN") or os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def collect(context: RunContext, settings: dict) -> dict[str, Any]:
    source_config = settings["sources"].get("github", {})
    watchlists = settings["watchlists"]
    keyword_map = settings["keyword_map"]
    session = build_session()

    endpoint = source_config["endpoint"]
    queries = [expand_template(query, context) for query in watchlists.get("github_queries", [])]
    per_query = int(source_config.get("per_query", 15))
    sort = source_config.get("sort", "updated")
    request_interval = float(source_config.get("request_interval_seconds", 0))
    deduped: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []

    for index, query in enumerate(queries):
        if index and request_interval > 0:
            time.sleep(request_interval)

        params = {
            "q": query,
            "sort": sort,
            "order": "desc",
            "per_page": per_query,
        }

        payload = None
        for attempt in range(2):
            try:
                payload = get_json(session, endpoint, params=params, headers=_github_headers())
                break
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in {403, 429} and attempt == 0:
                    reset_header = exc.response.headers.get("X-RateLimit-Reset") if exc.response is not None else None
                    wait_seconds = request_interval or 10
                    if reset_header and reset_header.isdigit():
                        wait_seconds = max(wait_seconds, min(int(reset_header) - int(time.time()) + 1, 75))
                    time.sleep(max(wait_seconds, 1))
                    continue
                errors.append({"query": query, "error": str(exc)})
                break
            except Exception as exc:  # noqa: BLE001
                errors.append({"query": query, "error": str(exc)})
                break

        if payload is None:
            continue

        for repo in payload.get("items", []):
            full_name = repo["full_name"]
            tags = merge_tags(
                repo.get("topics", []),
                infer_tags(keyword_map, full_name, repo.get("description"), query),
            )

            if full_name not in deduped:
                deduped[full_name] = {
                    "repo_name": full_name,
                    "url": repo.get("html_url"),
                    "description": repo.get("description"),
                    "stars": repo.get("stargazers_count") or 0,
                    "forks": repo.get("forks_count") or 0,
                    "watchers": repo.get("watchers_count") or 0,
                    "open_issues": repo.get("open_issues_count") or 0,
                    "language": repo.get("language"),
                    "topics": repo.get("topics") or [],
                    "created_at": repo.get("created_at"),
                    "updated_at": repo.get("updated_at"),
                    "pushed_at": repo.get("pushed_at"),
                    "matched_queries": [query],
                    "owner_login": (repo.get("owner") or {}).get("login"),
                    "tags": tags,
                }
            else:
                deduped[full_name]["matched_queries"].append(query)
                deduped[full_name]["tags"] = merge_tags(deduped[full_name]["tags"], tags)

    items = sorted(deduped.values(), key=lambda item: (item["stars"], item["forks"]), reverse=True)
    return {
        "source": "github",
        "run_date": context.run_date_str,
        "snapshot_at": context.snapshot_at.isoformat(),
        "items": items,
        "meta": {
            "query_count": len(queries),
            "item_count": len(items),
            "errors": errors,
        },
    }
