from __future__ import annotations

import json
import re
from typing import Any

from ..date_utils import RunContext
from ..http import build_session, get_json
from ..tagging import infer_tags, merge_tags


STRATEGY_TAGS = {
    "ai",
    "automation",
    "crypto",
    "economy",
    "energy",
    "geopolitics",
    "inflation",
    "llm",
    "platform-risk",
    "regulation",
    "search",
    "semiconductor",
    "social-media",
    "video",
    "youtube",
}

FALLBACK_CATEGORIES = {
    "business",
    "crypto",
    "economics",
    "economy",
    "finance",
    "markets",
    "politics",
    "science",
    "technology",
    "world",
}


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


def _market_haystack(market: dict[str, Any]) -> str:
    haystack_parts = [
        str(market.get("question") or ""),
        str(market.get("title") or ""),
        str(market.get("category") or ""),
        " ".join(_tag_names(market.get("tags", []))),
    ]
    return " ".join(haystack_parts).lower()


def _matched_keywords(haystack: str, keywords: list[str]) -> list[str]:
    matched = []
    for keyword in keywords:
        term = keyword.strip().lower()
        if not term:
            continue
        if term.isascii():
            escaped = re.escape(term).replace(r"\ ", r"\s+")
            pattern = rf"(?<![a-z0-9]){escaped}(?![a-z0-9])"
            if re.search(pattern, haystack):
                matched.append(keyword)
        elif term in haystack:
            matched.append(keyword)
    return matched


def _market_volume(market: dict[str, Any]) -> float:
    for key in ("volume", "volumeNum", "volume24hr", "volume24hrClob", "volume1wk"):
        value = _as_float(market.get(key))
        if value is not None:
            return value
    return 0.0


def _market_liquidity(market: dict[str, Any]) -> float:
    for key in ("liquidity", "liquidityNum", "liquidityClob"):
        value = _as_float(market.get(key))
        if value is not None:
            return value
    return 0.0


def _category_matches(market: dict[str, Any]) -> bool:
    category = str(market.get("category") or "").strip().lower()
    return category in FALLBACK_CATEGORIES


def _should_keep(
    market: dict[str, Any],
    include_keywords: list[str],
    exclude_keywords: list[str],
    keyword_map: dict[str, list[str]],
    min_volume: float,
    fallback_min_volume: float,
) -> tuple[bool, dict[str, Any]]:
    haystack = _market_haystack(market)
    matched_include = _matched_keywords(haystack, include_keywords)
    matched_exclude = _matched_keywords(haystack, exclude_keywords)
    title = market.get("question") or market.get("title")
    tags = merge_tags(
        _tag_names(market.get("tags", [])),
        infer_tags(keyword_map, title, market.get("category")),
    )
    strategy_tags = sorted(set(tags) & STRATEGY_TAGS)
    volume = _market_volume(market)
    liquidity = _market_liquidity(market)
    high_volume_fallback = volume >= fallback_min_volume and _category_matches(market)

    score = 0.0
    reasons: list[str] = []
    if matched_include:
        score += 3.5 + min(len(matched_include) * 0.35, 1.5)
        reasons.append("watchlist_keyword")
    if strategy_tags:
        score += 1.5 + min(len(strategy_tags) * 0.25, 1.25)
        reasons.append("strategy_tag")
    if high_volume_fallback:
        score += 1.25
        reasons.append("high_volume_category")
    if volume >= min_volume:
        score += min(volume / 100000.0, 2.0)
    if liquidity:
        score += min(liquidity / 50000.0, 1.0)
    if matched_exclude:
        score -= 8.0
        reasons.append("excluded_noise")

    keep = (
        not matched_exclude
        and volume >= min_volume
        and bool(matched_include or strategy_tags or high_volume_fallback)
    )
    relevance = {
        "score": round(score, 3),
        "reasons": reasons,
        "matched_keywords": matched_include,
        "excluded_keywords": matched_exclude,
        "strategy_tags": strategy_tags,
        "high_volume_fallback": high_volume_fallback,
    }
    return keep, relevance


def collect(context: RunContext, settings: dict) -> dict[str, Any]:
    source_config = settings["sources"].get("polymarket", {})
    watchlists = settings["watchlists"]
    keyword_map = settings["keyword_map"]
    session = build_session()

    endpoint = source_config["gamma_endpoint"]
    limit = int(source_config.get("limit", 200))
    page_size = int(source_config.get("page_size", min(limit, 100)))
    pages = int(source_config.get("pages", max(1, (limit + page_size - 1) // page_size)))
    order = source_config.get("order", "volume")
    filter_config = watchlists.get("polymarket_filters", {})
    include_keywords = filter_config.get("include_keywords") or filter_config.get("categories", [])
    exclude_keywords = filter_config.get("exclude_keywords", [])
    min_volume = float(filter_config.get("min_volume", 0))
    fallback_min_volume = float(filter_config.get("fallback_min_volume", 50000))
    target_count = int(filter_config.get("target_count", 60))

    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    items: list[dict[str, Any]] = []

    markets_by_key: dict[str, dict[str, Any]] = {}
    for page_index in range(pages):
        try:
            payload = get_json(
                session,
                endpoint,
                params={
                    "limit": page_size,
                    "offset": page_index * page_size,
                    "active": "true",
                    "closed": "false",
                    "order": order,
                    "ascending": "false",
                },
            )
        except Exception as exc:  # noqa: BLE001
            errors.append({"source": "polymarket", "error": str(exc), "page": str(page_index)})
            continue

        page_markets = payload if isinstance(payload, list) else payload.get("data", [])
        if not page_markets:
            break
        for market in page_markets:
            slug = market.get("slug")
            key = str(market.get("id") or market.get("conditionId") or slug or market.get("question") or market.get("title"))
            markets_by_key.setdefault(key, market)

    markets = list(markets_by_key.values())

    for market in markets:
        keep, relevance = _should_keep(
            market,
            include_keywords,
            exclude_keywords,
            keyword_map,
            min_volume,
            fallback_min_volume,
        )
        if not keep:
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
                "volume": _market_volume(market),
                "liquidity": _market_liquidity(market),
                "end_date": market.get("endDate") or market.get("end_date"),
                "active": market.get("active"),
                "closed": market.get("closed"),
                "outcomes": outcomes,
                "tags": tags,
                "relevance": relevance,
            }
        )

    items.sort(
        key=lambda item: (
            item.get("relevance", {}).get("score", 0),
            item["volume"],
            item["liquidity"],
        ),
        reverse=True,
    )
    items = items[:target_count]

    if markets and not items:
        warnings.append(
            {
                "source": "polymarket",
                "warning": "No markets matched the strategy watchlist after noise filters.",
            }
        )

    return {
        "source": "polymarket",
        "run_date": context.run_date_str,
        "snapshot_at": context.snapshot_at.isoformat(),
        "items": items,
        "meta": {
            "market_count": len(items),
            "fetched_market_count": len(markets),
            "target_count": target_count,
            "errors": errors,
            "warnings": warnings,
        },
    }
