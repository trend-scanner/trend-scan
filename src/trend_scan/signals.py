from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from typing import Any


def _importance(score: float) -> str:
    if score >= 4.0:
        return "high"
    if score >= 2.7:
        return "medium"
    return "low"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _metric(record: dict[str, Any] | None, key: str) -> float | None:
    if record is None:
        return None
    value = record.get("metrics", {}).get(key)
    if value is None:
        return None
    return _safe_float(value)


def _delta(current: dict[str, Any], previous: dict[str, Any] | None, key: str) -> float | None:
    current_value = _metric(current, key)
    previous_value = _metric(previous, key)
    if current_value is None or previous_value is None:
        return None
    return current_value - previous_value


def _history_index(records: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        key = (str(record.get("source")), str(record.get("entity_key")))
        index[key].append(record)
    for rows in index.values():
        rows.sort(key=lambda row: row.get("date") or "")
    return index


def _novelty(record: dict[str, Any], history: list[dict[str, Any]], run_date_str: str) -> dict[str, Any]:
    run_date = date.fromisoformat(run_date_str)
    seen_dates = sorted(
        {
            parsed.isoformat()
            for parsed in (_parse_date(row.get("date")) for row in history)
            if parsed and parsed <= run_date
        }
    )
    first_seen_date = seen_dates[0] if seen_dates else run_date_str
    previous_seen_date = seen_dates[-1] if seen_dates else None
    first_seen = date.fromisoformat(first_seen_date)
    previous_seen = date.fromisoformat(previous_seen_date) if previous_seen_date else None
    gap_days = (run_date - previous_seen).days if previous_seen else None
    return {
        "first_seen_date": first_seen_date,
        "days_since_first_seen": (run_date - first_seen).days,
        "seen_count_30d": len(seen_dates) + 1,
        "previous_seen_date": previous_seen_date,
        "is_first_seen": not seen_dates,
        "is_reappearing": gap_days is not None and gap_days >= 7,
    }


def _novelty_boost(novelty: dict[str, Any]) -> float:
    if novelty.get("is_first_seen"):
        return 0.8
    if novelty.get("is_reappearing"):
        return 0.5
    seen_count = int(novelty.get("seen_count_30d", 1))
    if seen_count >= 10:
        return -0.5
    return 0.0


def _created_age_days(record: dict[str, Any], run_date_str: str) -> int | None:
    created_at = record.get("attributes", {}).get("created_at")
    created_date = _parse_date(created_at)
    if created_date is None:
        return None
    return (date.fromisoformat(run_date_str) - created_date).days


def _tag_region_counts(records: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"global": 0, "jp": 0})
    for record in records:
        region = "jp" if record.get("region") == "jp" else "global"
        for tag in record.get("tags", []):
            counts[tag][region] += 1
    return counts


def _region_gap(record: dict[str, Any], region_counts: dict[str, dict[str, int]]) -> dict[str, Any]:
    if record.get("region") == "jp":
        return {"status": "not_global_first"}
    for tag in record.get("tags", []):
        counts = region_counts.get(tag, {})
        global_count = counts.get("global", 0)
        jp_count = counts.get("jp", 0)
        if global_count >= 2 and jp_count == 0:
            return {
                "status": "global_strong_jp_weak",
                "tag": tag,
                "global_count": global_count,
                "jp_count": jp_count,
            }
    return {"status": "balanced_or_unclear"}


def _position_hint(source: str, reason: str, region_gap: dict[str, Any]) -> str:
    if region_gap.get("status") == "global_strong_jp_weak":
        return "Global-first candidate: validate Japanese demand before the topic becomes crowded."
    if source == "github":
        return "Developer-adoption candidate: test with a small demo, explainer, or tool wrapper."
    if source == "hackernews":
        return "Early-reaction candidate: wait for confirmation from GitHub, RSS, or search interest."
    if source == "rss":
        return "Official-change candidate: check whether this shifts platform, API, content, or compliance work."
    if source == "wikipedia":
        return "Broad-interest candidate: validate with search demand and lightweight content."
    if source == "polymarket":
        return "Expectation-shift candidate: confirm with primary news and liquidity before acting."
    if reason == "cross_source_theme":
        return "Multi-source candidate: prioritize for private strategy review."
    return "Watch candidate: validate persistence over the next 7 days."


def _causal_notes(source: str, reason: str) -> tuple[list[str], list[str]]:
    alternatives = {
        "github": ["Existing large repository may be receiving routine updates.", "Stars can lag real usage or reflect one-off attention."],
        "hackernews": ["HN attention can be short-lived or controversy-driven.", "A single popular post may not represent durable demand."],
        "rss": ["Official announcements can be PR-heavy.", "Immediate user impact may be smaller than the announcement suggests."],
        "wikipedia": ["Pageviews can spike from news cycles rather than durable interest.", "A redirect or naming change can affect apparent volume."],
        "polymarket": ["Thin liquidity can exaggerate moves.", "Political or event-driven markets can be noisy."],
        "combined": ["Sources may be reacting to the same single event.", "Tag overlap does not prove a causal trend."],
    }
    validations = {
        "github": ["Compare 7-day star growth.", "Check issue activity and recent contributors.", "Look for Japanese content gaps."],
        "hackernews": ["Check whether discussion persists after 24 hours.", "Look for matching GitHub or RSS movement."],
        "rss": ["Read the primary announcement.", "Check API/pricing/policy impact."],
        "wikipedia": ["Compare against prior week pageviews.", "Check search demand before creating content."],
        "polymarket": ["Check volume and liquidity.", "Confirm with primary reporting or official data."],
        "combined": ["Validate source independence.", "Recheck after 7 days for persistence."],
    }
    return alternatives.get(source, alternatives["combined"]), validations.get(source, validations["combined"])


def _signal(
    record: dict[str, Any],
    reason: str,
    score: float,
    summary: str,
    *,
    signal_type: str,
    novelty: dict[str, Any],
    region_gap: dict[str, Any],
    deltas: dict[str, float | None] | None = None,
    examples: list[str] | None = None,
) -> dict[str, Any]:
    source = str(record.get("source"))
    alternatives, validations = _causal_notes(source, reason)
    return {
        "title": record.get("title"),
        "layer": record.get("layer"),
        "source": source,
        "reason": reason,
        "signal_type": signal_type,
        "importance": _importance(score),
        "score": round(score, 3),
        "summary": summary,
        "url": record.get("url"),
        "tags": record.get("tags", []),
        "entity_key": record.get("entity_key"),
        "metrics": record.get("metrics", {}),
        "deltas": deltas or {},
        "novelty": novelty,
        "region_gap": region_gap,
        "position_hint": _position_hint(source, reason, region_gap),
        "causal_status": "correlation_only",
        "alternative_explanations": alternatives,
        "suggested_validation": validations,
        "examples": examples or [],
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


def _reference(history: list[dict[str, Any]], previous: dict[str, Any] | None) -> dict[str, Any] | None:
    return previous or (history[-1] if history else None)


def _record_signals(
    record: dict[str, Any],
    previous: dict[str, Any] | None,
    history: list[dict[str, Any]],
    run_date_str: str,
    region_counts: dict[str, dict[str, int]],
) -> list[dict[str, Any]]:
    source = record.get("source")
    metrics = record.get("metrics", {})
    novelty = _novelty(record, history, run_date_str)
    region_gap = _region_gap(record, region_counts)
    novelty_score = _novelty_boost(novelty)
    gap_score = 0.5 if region_gap.get("status") == "global_strong_jp_weak" else 0.0
    reference = _reference(history, previous)

    if source == "rss":
        if (
            record.get("attributes", {}).get("priority")
            and record.get("attributes", {}).get("signal_candidate")
            and record.get("tags")
        ):
            score = 2.2 + novelty_score + gap_score
            return [
                _signal(
                    record,
                    "official_update",
                    score,
                    "Priority RSS source published a likely product, policy, or platform update.",
                    signal_type="official_change",
                    novelty=novelty,
                    region_gap=region_gap,
                )
            ]
        return []

    if source == "hackernews":
        score_delta = _delta(record, reference, "score") or 0.0
        comments_delta = _delta(record, reference, "comments") or 0.0
        score_value = float(metrics.get("score", 0))
        comments = float(metrics.get("comments", 0))
        new_hot = novelty.get("is_first_seen") and (score_value >= 75 or comments >= 40)
        moved = score_delta >= 30 or comments_delta >= 20
        if new_hot or moved:
            signal_score = (
                2.0
                + max(score_delta / 45.0, comments_delta / 35.0, score_value / 220.0)
                + novelty_score
                + gap_score
            )
            return [
                _signal(
                    record,
                    "hn_engagement_spike",
                    signal_score,
                    "Hacker News engagement moved enough to treat this as an early reaction signal.",
                    signal_type="attention_acceleration" if moved else "new_attention",
                    novelty=novelty,
                    region_gap=region_gap,
                    deltas={"score": score_delta, "comments": comments_delta},
                )
            ]
        return []

    if source == "github":
        stars = float(metrics.get("stars", 0))
        forks = float(metrics.get("forks", 0))
        stars_delta = _delta(record, reference, "stars") or 0.0
        forks_delta = _delta(record, reference, "forks") or 0.0
        first_history = history[0] if history else None
        stars_30d_delta = _delta(record, first_history, "stars") or 0.0
        forks_30d_delta = _delta(record, first_history, "forks") or 0.0
        first_stars = _metric(first_history, "stars") or 0.0
        stars_30d_pct = stars_30d_delta / max(first_stars, 1.0)
        created_age_days = _created_age_days(record, run_date_str)
        recent_repo = created_age_days is not None and created_age_days <= 180
        huge_static_repo = stars >= 5000 and stars_delta < 15 and stars_30d_delta < 50 and not recent_repo
        new_hot = novelty.get("is_first_seen") and (recent_repo and stars >= 75 or stars < 5000 and stars >= 250)
        accelerated = stars_delta >= 15 or forks_delta >= 5 or stars_30d_delta >= 40 or forks_30d_delta >= 10
        fast_relative_growth = stars_30d_delta >= 25 and stars_30d_pct >= 0.05

        if huge_static_repo or not (new_hot or accelerated or fast_relative_growth):
            return []

        large_repo_penalty = 0.6 if stars >= 20000 and stars_delta < 50 else 0.0
        signal_score = (
            2.2
            + max(stars_delta / 35.0, forks_delta / 12.0, stars_30d_delta / 120.0, min(stars_30d_pct, 2.0))
            + novelty_score
            + (0.4 if recent_repo else 0.0)
            + gap_score
            - large_repo_penalty
        )
        return [
            _signal(
                record,
                "github_repo_acceleration" if accelerated or fast_relative_growth else "github_new_repo",
                signal_score,
                "Repository movement favors recent acceleration, novelty, or fast relative growth over total stars.",
                signal_type="developer_adoption_acceleration" if accelerated else "new_developer_tool",
                novelty=novelty,
                region_gap=region_gap,
                deltas={
                    "stars_1d": stars_delta,
                    "forks_1d": forks_delta,
                    "stars_30d": stars_30d_delta,
                    "forks_30d": forks_30d_delta,
                    "stars_30d_pct": round(stars_30d_pct, 3),
                },
            )
        ]

    if source == "wikipedia":
        views_delta = _delta(record, reference, "views") or 0.0
        views = float(metrics.get("views", 0))
        previous_views = _metric(reference, "views") or 0.0
        pct = (views_delta / previous_views) if previous_views else 0.0
        new_interest = novelty.get("is_first_seen") and views >= 5000
        moved = views_delta >= 1000 or pct >= 0.5
        if new_interest or moved:
            return [
                _signal(
                    record,
                    "wikipedia_interest_spike",
                    2.0 + max(views / 50000.0, views_delta / 5000.0, pct) + novelty_score + gap_score,
                    "Wikipedia pageviews point to broad interest movement worth validating.",
                    signal_type="broad_interest_acceleration" if moved else "new_broad_interest",
                    novelty=novelty,
                    region_gap=region_gap,
                    deltas={"views": views_delta, "views_pct": round(pct, 3)},
                )
            ]
        return []

    if source == "polymarket":
        volume_delta = _delta(record, reference, "volume") or 0.0
        swing = _price_swing(record, reference) or 0.0
        volume = float(metrics.get("volume", 0))
        new_market = novelty.get("is_first_seen") and volume >= 10000
        moved = swing >= 0.07 or volume_delta >= 5000
        if new_market or moved:
            volume_delta_component = min(max(volume_delta, 0.0) / 50000.0, 2.0)
            volume_size_component = min(volume / 1000000.0, 1.5)
            return [
                _signal(
                    record,
                    "polymarket_probability_shift",
                    2.1 + max(swing * 6.0, volume_delta_component, volume_size_component) + novelty_score + gap_score,
                    "Prediction market pricing or volume moved in a strategy-relevant watchlist theme.",
                    signal_type="expectation_shift" if moved else "new_expectation_market",
                    novelty=novelty,
                    region_gap=region_gap,
                    deltas={"volume": volume_delta, "max_price_swing": round(swing, 3)},
                )
            ]
        return []

    return []


def _cross_source_signals(
    moving_records: list[dict[str, Any]],
    all_records: list[dict[str, Any]],
    tracked_tags: set[str],
    run_date_str: str,
) -> list[dict[str, Any]]:
    tag_sources: dict[str, set[str]] = defaultdict(set)
    tag_layers: dict[str, set[str]] = defaultdict(set)
    tag_regions: dict[str, set[str]] = defaultdict(set)
    tag_examples: dict[str, list[str]] = defaultdict(list)
    region_counts = _tag_region_counts(all_records)

    for record in moving_records:
        for tag in record.get("tags", []):
            if tag not in tracked_tags:
                continue
            tag_sources[tag].add(record.get("source", ""))
            tag_layers[tag].add(record.get("layer", ""))
            tag_regions[tag].add(record.get("region", "global"))
            if len(tag_examples[tag]) < 3:
                tag_examples[tag].append(record.get("title"))

    signals: list[dict[str, Any]] = []
    for tag, sources in tag_sources.items():
        if len(sources) < 2 or len(tag_layers[tag]) < 2:
            continue
        gap = {"status": "balanced_or_unclear"}
        counts = region_counts.get(tag, {})
        if counts.get("global", 0) >= 2 and counts.get("jp", 0) == 0:
            gap = {
                "status": "global_strong_jp_weak",
                "tag": tag,
                "global_count": counts.get("global", 0),
                "jp_count": counts.get("jp", 0),
            }
        score = 2.6 + (len(sources) * 0.35) + (len(tag_layers[tag]) * 0.25)
        if gap["status"] == "global_strong_jp_weak":
            score += 0.7
        synthetic_record = {
            "title": f"{tag} moving across sources",
            "layer": "MULTI_LAYER",
            "source": "combined",
            "url": None,
            "tags": [tag],
            "entity_key": f"combined:{tag}",
            "metrics": {
                "source_count": len(sources),
                "layer_count": len(tag_layers[tag]),
                "region_count": len(tag_regions[tag]),
            },
        }
        signals.append(
            _signal(
                synthetic_record,
                "cross_source_theme",
                score,
                f"{tag} moved across {len(sources)} sources and {len(tag_layers[tag])} layers.",
                signal_type="multi_source_acceleration",
                novelty={
                    "first_seen_date": run_date_str,
                    "days_since_first_seen": 0,
                    "seen_count_30d": 1,
                    "previous_seen_date": None,
                    "is_first_seen": True,
                    "is_reappearing": False,
                },
                region_gap=gap,
                examples=tag_examples[tag],
            )
        )

    return signals


def _sort_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        signals,
        key=lambda signal: (
            float(signal.get("score", 0)),
            {"high": 3, "medium": 2, "low": 1}.get(signal.get("importance"), 0),
            signal.get("title") or "",
        ),
        reverse=True,
    )


def _select_top_signals(signals: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    sorted_signals = _sort_signals(signals)
    selected: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    for signal in sorted_signals:
        if len(selected) >= limit:
            break
        source = signal.get("source", "unknown")
        reason = signal.get("reason", "unknown")
        if source_counts[source] >= 5:
            continue
        if reason_counts[reason] >= 4:
            continue
        selected.append(signal)
        source_counts[source] += 1
        reason_counts[reason] += 1

    if len(selected) < limit:
        selected_keys = {(signal.get("source"), signal.get("entity_key"), signal.get("reason")) for signal in selected}
        for signal in sorted_signals:
            key = (signal.get("source"), signal.get("entity_key"), signal.get("reason"))
            if key in selected_keys:
                continue
            selected.append(signal)
            if len(selected) >= limit:
                break
    return selected


def detect_signals(
    current_records: list[dict[str, Any]],
    previous_records: list[dict[str, Any]],
    run_date_str: str,
    tracked_tags: list[str] | None = None,
    history_records: list[dict[str, Any]] | None = None,
    top_limit: int = 15,
    important_limit: int = 40,
) -> dict[str, Any]:
    previous_index = {
        (record.get("source"), record.get("entity_key")): record
        for record in previous_records
    }
    history = _history_index(history_records or previous_records)
    tracked_tag_set = set(tracked_tags or [])
    region_counts = _tag_region_counts(current_records)

    signals: list[dict[str, Any]] = []
    moving_records: list[dict[str, Any]] = []
    source_counts: Counter[str] = Counter()

    for record in current_records:
        key = (str(record.get("source")), str(record.get("entity_key")))
        previous = previous_index.get((record.get("source"), record.get("entity_key")))
        record_signals = _record_signals(
            record,
            previous,
            history.get(key, []),
            run_date_str,
            region_counts,
        )
        if record_signals:
            moving_records.append(record)
        signals.extend(record_signals)
        source_counts.update(signal["source"] for signal in record_signals)

    cross_source = _cross_source_signals(moving_records, current_records, tracked_tag_set, run_date_str)
    signals.extend(cross_source)
    source_counts.update(signal["source"] for signal in cross_source)

    sorted_signals = _sort_signals(signals)
    important_signals = sorted_signals[:important_limit]
    top_signals = _select_top_signals(sorted_signals, top_limit)

    return {
        "date": run_date_str,
        "signals": sorted_signals,
        "important_signals": important_signals,
        "top_signals": top_signals,
        "meta": {
            "current_record_count": len(current_records),
            "previous_record_count": len(previous_records),
            "history_record_count": len(history_records or previous_records),
            "signal_count": len(sorted_signals),
            "important_signal_count": len(important_signals),
            "top_signal_count": len(top_signals),
            "signals_by_source": dict(source_counts),
        },
    }
