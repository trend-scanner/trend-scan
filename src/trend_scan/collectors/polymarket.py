from __future__ import annotations

import json
from typing import Any

from ..date_utils import RunContext
from ..http import build_session, get_json
from ..tagging import infer_tags, merge_tags


def _as_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _extract_outcomes(market: dict[str, Any]) -> list[dict[str, Any]]:
    names = _parse_json_list(market.get("outcomes"))
    prices = _parse_json_list(market.get("outcomePrices"))
    outcomes: list[dict[str, Any]] = []

    for index, name in enumerate(names):
        price = prices[index] if index < len(prices) else None
        outcomes.append({"name": str(name), "price": _as_float(price)})
    return outcomes


def _tag_names(raw_tags: Any) -> list[str]:
    if not isinstance(raw_tags, list):
        return []
    tag_names = []
    for tag in raw_tags:
        if isinstance(tag, dict):
            name = tag.get("name") or tag.get("slug")
            if name:
                tag_names.append(str(name))
        elif tag:
            tag_names.append(str(tag))
    return tag_names


def _should_keep(market: dict[str, Any], category_filters: list[str]) -> bool:
    if not category_filters:
        return True

    haystack_parts = [
        str(market.get("question") or ""),
        str(market.get("title") or ""),
        str(market.get("category") or ""),
        " ".join(_tag_names(market.get("tags", []))),
    ]
    haystack = " ".join(haystack_parts).lower()
    return any(category.lower() in haystack for category in category_filters)


def collect(context: RunContext, settings: dict) -> dict[str, Any]:
    source_config = settings["sources"].get("polymarket", {})
    watchlists = settings["watchlists"]
    keyword_map = settings["keyword_map"]
    session = build_session()

    endpoint = source_config["gamma_endpoint"]
    limit = int(source_config.get("limit", 200))
    filter_config = watchlists.get("polymarket_filters", {})
    category_filters = filter_config.get("categories", [])
    min_volume = float(filter_config.get("min_volume", 0))

    errors: list[dict[str, str]] = []
    items: list[dict[str, Any]] = []

    try:
        payload = get_json(
            session,
            endpoint,
            params={"limit": limit, "active": "true", "closed": "false"},
        )
    except Exception as exc:  # noqa: BLE001
        errors.append({"source": "polymarket", "error": str(exc)})
        payload = []

    markets = payload if isinstance(payload, list) else payload.get("data", [])

    for market in markets:
        if not _should_keep(market, category_filters):
            continue

        volume = _as_float(market.get("volume")) or _as_float(market.get("volumeNum")) or 0.0
        if volume < min_volume:
            continue

        title = market.get("question") or market.get("title")
        slug = market.get("slug")
        tags = merge_tags(
            _tag_names(market.get("tags", [])),
            infer_tags(keyword_map, title, market.get("category")),
        )
        outcomes = _extract_outcomes(market)

        items.append(
            {
                "market_id": market.get("id") or market.get("conditionId") or slug,
                "title": title,
                "slug": slug,
                "url": market.get("url") or (f"https://polymarket.com/event/{slug}" if slug else None),
                "category": market.get("category"),
                "volume": volume,
                "liquidity": _as_float(market.get("liquidity")) or 0.0,
                "end_date": market.get("endDate") or market.get("end_date"),
                "active": market.get("active"),
                "closed": market.get("closed"),
                "outcomes": outcomes,
                "tags": tags,
            }
        )

    items.sort(key=lambda item: (item["volume"], item["liquidity"]), reverse=True)
    return {
        "source": "polymarket",
        "run_date": context.run_date_str,
        "snapshot_at": context.snapshot_at.isoformat(),
        "items": items,
        "meta": {
            "market_count": len(items),
            "errors": errors,
        },
    }
