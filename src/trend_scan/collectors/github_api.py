from __future__ import annotations

import os
from typing import Any

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
    deduped: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []

    for query in queries:
        params = {
            "q": query,
            "sort": sort,
            "order": "desc",
            "per_page": per_query,
        }

        try:
            payload = get_json(session, endpoint, params=params, headers=_github_headers())
        except Exception as exc:  # noqa: BLE001
            errors.append({"query": query, "error": str(exc)})
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
