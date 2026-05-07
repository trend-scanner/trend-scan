from __future__ import annotations

import time
from datetime import date
from typing import Any
from urllib.parse import quote

import requests

from ..date_utils import RunContext, wikipedia_observed_dates
from ..http import build_session, get_json
from ..tagging import merge_tags


def _project_name(project: str) -> str:
    return project if project.endswith(".org") else f"{project}.org"


def _pageviews_url(
    endpoint_template: str,
    source_config: dict[str, Any],
    page: dict[str, Any],
    article_title: str,
    observed_date: date,
) -> str:
    observed_stamp = observed_date.strftime("%Y%m%d")
    return endpoint_template.format(
        project=_project_name(page["project"]),
        access=source_config.get("access", "all-access"),
        agent=source_config.get("agent", "user"),
        article=quote(article_title.replace(" ", "_"), safe=""),
        granularity=source_config.get("granularity", "daily"),
        start=observed_stamp,
        end=observed_stamp,
    )


def _resolve_article_title(
    session: requests.Session,
    project: str,
    title: str,
) -> tuple[str, str | None]:
    endpoint = f"https://{_project_name(project)}/w/api.php"
    try:
        payload = get_json(
            session,
            endpoint,
            params={
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "redirects": "1",
                "titles": title,
            },
        )
    except Exception as exc:  # noqa: BLE001
        return title, str(exc)

    pages = payload.get("query", {}).get("pages", [])
    if not pages:
        return title, "MediaWiki title lookup returned no pages"

    page = pages[0]
    if page.get("missing"):
        return title, f"MediaWiki title not found: {title}"

    resolved_title = str(page.get("title") or title)
    return resolved_title, None


def _fetch_pageviews(
    session: requests.Session,
    url: str,
    request_interval: float,
) -> tuple[dict[str, Any] | None, str | None, int | None]:
    for attempt in range(3):
        try:
            return get_json(session, url), None, None
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 429 and attempt < 2:
                retry_after = exc.response.headers.get("Retry-After") if exc.response is not None else None
                wait_seconds = float(retry_after) if retry_after and retry_after.isdigit() else 2 + attempt
                time.sleep(max(request_interval, wait_seconds))
                continue
            return None, str(exc), status_code
        except Exception as exc:  # noqa: BLE001
            return None, str(exc), None
    return None, "Wikipedia request failed after retries", None


def _resolve_observed_date(
    session: requests.Session,
    source_config: dict[str, Any],
    endpoint_template: str,
    probe_page: dict[str, Any],
    probe_title: str,
    candidate_dates: list[date],
    request_interval: float,
) -> tuple[date, list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    for candidate_date in candidate_dates:
        time.sleep(request_interval)
        url = _pageviews_url(endpoint_template, source_config, probe_page, probe_title, candidate_date)
        payload, error, status_code = _fetch_pageviews(session, url, request_interval)
        if payload and payload.get("items"):
            return candidate_date, warnings
        if status_code == 404:
            warnings.append(
                {
                    "title": probe_page["title"],
                    "observed_date": candidate_date.isoformat(),
                    "warning": error or "No Wikipedia pageviews for this date yet",
                }
            )
            continue
        if error:
            warnings.append(
                {
                    "title": probe_page["title"],
                    "observed_date": candidate_date.isoformat(),
                    "warning": error,
                }
            )

    return candidate_dates[-1], warnings


def collect(context: RunContext, settings: dict) -> dict[str, Any]:
    source_config = settings["sources"].get("wikipedia", {})
    watchlists = settings["watchlists"]
    session = build_session()

    endpoint_template = source_config["endpoint_template"]
    candidate_dates = wikipedia_observed_dates(
        context,
        int(source_config.get("min_lag_days", source_config.get("lag_days", 1))),
        int(source_config.get("max_lag_days", 7)),
    )
    request_interval = float(source_config.get("request_interval_seconds", 0.2))

    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    pages = watchlists.get("wikipedia_pages", [])

    observed_date = candidate_dates[0]
    if pages:
        probe_page = pages[0]
        observed_date, date_warnings = _resolve_observed_date(
            session,
            source_config,
            endpoint_template,
            probe_page,
            probe_page["title"],
            candidate_dates,
            request_interval,
        )
        warnings.extend(date_warnings)

    for page in pages:
        time.sleep(request_interval)
        article_title = page["title"]
        url = _pageviews_url(endpoint_template, source_config, page, article_title, observed_date)
        payload, error, status_code = _fetch_pageviews(session, url, request_interval)

        if error and status_code == 404:
            article_title, warning = _resolve_article_title(session, page["project"], page["title"])
            if warning:
                warnings.append({"title": page["title"], "warning": warning})
            if article_title != page["title"]:
                retry_url = _pageviews_url(endpoint_template, source_config, page, article_title, observed_date)
                payload, error, status_code = _fetch_pageviews(session, retry_url, request_interval)

        if error:
            errors.append({"title": page["title"], "resolved_title": article_title, "error": error})

        if payload is None:
            continue

        series = payload.get("items", [])
        if not series:
            continue

        point = series[0]
        items.append(
            {
                "page_title": article_title.replace("_", " "),
                "original_title": page["title"].replace("_", " "),
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
            "observed_lag_days": (context.run_date - observed_date).days,
            "errors": errors,
            "warnings": warnings,
        },
    }
