from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any


def _importance(score: float) -> str:
    if score >= 3.0:
        return "high"
    if score >= 2.0:
        return "medium"
    return "low"


def _delta(current: dict[str, Any], previous: dict[str, Any] | None, key: str) -> float | None:
    if previous is None:
        return None
    current_value = current.get("metrics", {}).get(key)
    previous_value = previous.get("metrics", {}).get(key)
    if current_value is None or previous_value is None:
        return None
    try:
        return float(current_value) - float(previous_value)
    except (TypeError, ValueError):
        return None


def _signal(
    record: dict[str, Any],
    reason: str,
    score: float,
    summary: str,
    deltas: dict[str, float | None] | None = None,
) -> dict[str, Any]:
    return {
        "title": record.get("title"),
        "layer": record.get("layer"),
        "source": record.get("source"),
        "reason": reason,
        "importance": _importance(score),
        "summary": summary,
        "url": record.get("url"),
        "tags": record.get("tags", []),
        "entity_key": record.get("entity_key"),
        "metrics": record.get("metrics", {}),
        "deltas": deltas or {},
    }


def _price_swing(record: dict[str, Any], previous: dict[str, Any] | None) -> float | None:
    if previous is None:
        return None

    current_map = {
        outcome.get("name"): outcome.get("price")
        for outcome in record.get("attributes", {}).get("outcomes", [])
    }
    previous_map = {
        outcome.get("name"): outcome.get("price")
        for outcome in previous.get("attributes", {}).get("outcomes", [])
    }

    swings = []
    for outcome_name, current_value in current_map.items():
        previous_value = previous_map.get(outcome_name)
        if current_value is None or previous_value is None:
            continue
        swings.append(abs(float(current_value) - float(previous_value)))

    return max(swings) if swings else None


def _record_signals(record: dict[str, Any], previous: dict[str, Any] | None) -> list[dict[str, Any]]:
    source = record.get("source")
    metrics = record.get("metrics", {})

    if source == "rss":
        if (
            record.get("attributes", {}).get("priority")
            and record.get("attributes", {}).get("signal_candidate")
            and record.get("tags")
        ):
            return [
                _signal(
                    record,
                    "official_update",
                    2.0,
                    "Priority RSS source published a likely product, policy, or platform update.",
                )
            ]
        return []

    if source == "hackernews":
        score_delta = _delta(record, previous, "score") or 0.0
        comments_delta = _delta(record, previous, "comments") or 0.0
        score = float(metrics.get("score", 0))
        comments = float(metrics.get("comments", 0))
        bootstrap = previous is None and (score >= 100 or comments >= 75)
        moved = previous is not None and (
            score >= 75 or comments >= 40 or score_delta >= 30 or comments_delta >= 20
        )
        if bootstrap or moved:
            return [
                _signal(
                    record,
                    "hn_engagement_spike",
                    2.0 + max(score_delta / 50.0, comments_delta / 40.0, score / 200.0),
                    "Hacker News engagement is strong enough to treat this as a reaction signal.",
                    {"score": score_delta, "comments": comments_delta},
                )
            ]
        return []

    if source == "github":
        stars_delta = _delta(record, previous, "stars") or 0.0
        forks_delta = _delta(record, previous, "forks") or 0.0
        stars = float(metrics.get("stars", 0))
        forks = float(metrics.get("forks", 0))
        bootstrap = previous is None and (stars >= 5000 or forks >= 500)
        moved = previous is not None and (stars_delta >= 25 or forks_delta >= 10)
        if bootstrap or moved:
            return [
                _signal(
                    record,
                    "github_repo_surge",
                    2.0 + max(stars_delta / 50.0, forks_delta / 20.0, stars / 1000.0),
                    "Repository activity suggests real developer adoption or a fast-moving launch.",
                    {"stars": stars_delta, "forks": forks_delta},
                )
            ]
        return []

    if source == "wikipedia":
        views_delta = _delta(record, previous, "views") or 0.0
        views = float(metrics.get("views", 0))
        previous_views = float(previous.get("metrics", {}).get("views", 0)) if previous else 0.0
        pct = (views_delta / previous_views) if previous_views else 0.0
        bootstrap = previous is None and views >= 10000
        moved = previous is not None and (views_delta >= 1000 or pct >= 0.5)
        if bootstrap or moved:
            return [
                _signal(
                    record,
                    "wikipedia_interest_spike",
                    2.0 + max(views / 50000.0, views_delta / 5000.0, pct),
                    "Wikipedia pageviews point to a broad interest jump worth tracking.",
                    {"views": views_delta, "views_pct": round(pct, 3)},
                )
            ]
        return []

    if source == "polymarket":
        volume_delta = _delta(record, previous, "volume") or 0.0
        swing = _price_swing(record, previous) or 0.0
        volume = float(metrics.get("volume", 0))
        bootstrap = previous is None and volume >= 50000
        moved = previous is not None and (swing >= 0.1 or volume_delta >= 10000)
        if bootstrap or moved:
            return [
                _signal(
                    record,
                    "polymarket_probability_shift",
                    2.0 + max(swing * 5.0, volume / 100000.0, volume_delta / 50000.0),
                    "Prediction market pricing or volume moved enough to treat this as an expectation signal.",
                    {"volume": volume_delta, "max_price_swing": round(swing, 3)},
                )
            ]
        return []

    return []


def _cross_source_signals(records: list[dict[str, Any]], tracked_tags: set[str]) -> list[dict[str, Any]]:
    tag_sources: dict[str, set[str]] = defaultdict(set)
    tag_layers: dict[str, set[str]] = defaultdict(set)
    tag_examples: dict[str, list[str]] = defaultdict(list)

    for record in records:
        for tag in record.get("tags", []):
            if tag not in tracked_tags:
                continue
            tag_sources[tag].add(record.get("source", ""))
            tag_layers[tag].add(record.get("layer", ""))
            if len(tag_examples[tag]) < 3:
                tag_examples[tag].append(record.get("title"))

    signals: list[dict[str, Any]] = []
    for tag, sources in tag_sources.items():
        if len(sources) < 2 or len(tag_layers[tag]) < 2:
            continue
        signals.append(
            {
                "title": f"{tag} appearing across sources",
                "layer": "MULTI_LAYER",
                "source": "combined",
                "reason": "cross_source_theme",
                "importance": "medium" if len(sources) == 2 else "high",
                "summary": f"{tag} showed up across {len(sources)} sources and {len(tag_layers[tag])} layers.",
                "url": None,
                "tags": [tag],
                "entity_key": f"combined:{tag}",
                "metrics": {
                    "source_count": len(sources),
                    "layer_count": len(tag_layers[tag]),
                },
                "examples": tag_examples[tag],
            }
        )

    return signals


def detect_signals(
    current_records: list[dict[str, Any]],
    previous_records: list[dict[str, Any]],
    run_date_str: str,
    tracked_tags: list[str] | None = None,
) -> dict[str, Any]:
    previous_index = {
        (record.get("source"), record.get("entity_key")): record
        for record in previous_records
    }
    tracked_tag_set = set(tracked_tags or [])

    signals: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()

    for record in current_records:
        previous = previous_index.get((record.get("source"), record.get("entity_key")))
        record_signals = _record_signals(record, previous)
        signals.extend(record_signals)
        source_counts.update(signal["source"] for signal in record_signals)

    signals.extend(_cross_source_signals(current_records, tracked_tag_set))

    signals.sort(
        key=lambda signal: (
            {"high": 3, "medium": 2, "low": 1}.get(signal["importance"], 0),
            len(signal.get("tags", [])),
            signal.get("title") or "",
        ),
        reverse=True,
    )

    return {
        "date": run_date_str,
        "signals": signals,
        "meta": {
            "current_record_count": len(current_records),
            "previous_record_count": len(previous_records),
            "signal_count": len(signals),
            "signals_by_source": dict(source_counts),
        },
    }
