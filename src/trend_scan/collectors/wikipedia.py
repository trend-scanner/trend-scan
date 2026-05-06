from __future__ import annotations

import time
from typing import Any
from urllib.parse import quote

import requests

from ..date_utils import RunContext, wikipedia_observed_date
from ..http import build_session, get_json
from ..tagging import merge_tags


def _project_name(project: str) -> str:
    return project if project.endswith(".org") else f"{project}.org"


def collect(context: RunContext, settings: dict) -> dict[str, Any]:
    source_config = settings["sources"].get("wikipedia", {})
    watchlists = settings["watchlists"]
    session = build_session()

    endpoint_template = source_config["endpoint_template"]
    observed_date = wikipedia_observed_date(context, int(source_config.get("lag_days", 1)))
    observed_stamp = observed_date.strftime("%Y%m%d")
    request_interval = float(source_config.get("request_interval_seconds", 0.2))

    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for page in watchlists.get("wikipedia_pages", []):
        time.sleep(request_interval)
        url = endpoint_template.format(
            project=_project_name(page["project"]),
            access=source_config.get("access", "all-access"),
            agent=source_config.get("agent", "user"),
            article=quote(page["title"], safe=""),
            granularity=source_config.get("granularity", "daily"),
            start=observed_stamp,
            end=observed_stamp,
        )

        payload = None
        for attempt in range(3):
            try:
                payload = get_json(session, url)
                break
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code == 429 and attempt < 2:
                    retry_after = exc.response.headers.get("Retry-After") if exc.response is not None else None
                    wait_seconds = float(retry_after) if retry_after and retry_after.isdigit() else 2 + attempt
                    time.sleep(max(request_interval, wait_seconds))
                    continue
                errors.append({"title": page["title"], "error": str(exc)})
                payload = None
                break
            except Exception as exc:  # noqa: BLE001
                errors.append({"title": page["title"], "error": str(exc)})
                payload = None
                break

        if payload is None:
            continue

        series = payload.get("items", [])
        if not series:
            continue

        point = series[0]
        items.append(
            {
                "page_title": page["title"].replace("_", " "),
                "project": _project_name(page["project"]),
                "region": page.get("region", "global"),
                "observed_date": observed_date.isoformat(),
                "views": point.get("views") or 0,
                "tags": merge_tags(page.get("tags", [])),
            }
        )

    items.sort(key=lambda item: item["views"], reverse=True)
    return {
        "source": "wikipedia",
        "run_date": context.run_date_str,
        "snapshot_at": context.snapshot_at.isoformat(),
        "items": items,
        "meta": {
            "page_count": len(watchlists.get("wikipedia_pages", [])),
            "item_count": len(items),
            "observed_date": observed_date.isoformat(),
            "errors": errors,
        },
    }
