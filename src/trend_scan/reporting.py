from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from typing import Any


def _top(records: list[dict[str, Any]], source: str, metric: str | None, limit: int = 5) -> list[dict[str, Any]]:
    candidates = [record for record in records if record.get("source") == source]
    if metric is None:
        return sorted(
            candidates,
            key=lambda record: record.get("published_at") or "",
            reverse=True,
        )[:limit]
    return sorted(
        candidates,
        key=lambda record: record.get("metrics", {}).get(metric, 0),
        reverse=True,
    )[:limit]


def build_daily_report(
    run_date_str: str,
    normalized_records: list[dict[str, Any]],
    signal_payload: dict[str, Any],
) -> str:
    counts_by_source = Counter(record.get("source") for record in normalized_records)
    counts_by_layer = Counter(record.get("layer") for record in normalized_records)
    signals = signal_payload.get("signals", [])

    lines = [
        f"# Daily Trend Scan Report - {run_date_str}",
        "",
        "## Snapshot",
        "",
        f"- Normalized records: {len(normalized_records)}",
        f"- Signals detected: {len(signals)}",
        "",
        "### By source",
        "",
    ]

    for source, count in sorted(counts_by_source.items()):
        lines.append(f"- {source}: {count}")

    lines.extend(["", "### By layer", ""])
    for layer, count in sorted(counts_by_layer.items()):
        lines.append(f"- {layer}: {count}")

    lines.extend(["", "## Key signals", ""])
    if not signals:
        lines.append("- No strong signals detected yet.")
    else:
        for signal in signals[:10]:
            summary = signal.get("summary") or ""
            tags = ", ".join(signal.get("tags", []))
            lines.append(
                f"- [{signal.get('importance')}] {signal.get('title')} ({signal.get('source')}): {summary} Tags: {tags}"
            )

    sections = [
        ("RSS updates", _top(normalized_records, "rss", None)),
        ("Hacker News leaders", _top(normalized_records, "hackernews", "score")),
        ("GitHub leaders", _top(normalized_records, "github", "stars")),
        ("Wikipedia leaders", _top(normalized_records, "wikipedia", "views")),
        ("Polymarket leaders", _top(normalized_records, "polymarket", "volume")),
    ]

    for title, records in sections:
        lines.extend(["", f"## {title}", ""])
        if not records:
            lines.append("- No records collected.")
            continue

        for record in records:
            metrics = record.get("metrics", {})
            interesting_metric = ""
            if record.get("source") == "hackernews":
                interesting_metric = f"score={metrics.get('score', 0)}, comments={metrics.get('comments', 0)}"
            elif record.get("source") == "github":
                interesting_metric = f"stars={metrics.get('stars', 0)}, forks={metrics.get('forks', 0)}"
            elif record.get("source") == "wikipedia":
                interesting_metric = f"views={metrics.get('views', 0)}"
            elif record.get("source") == "polymarket":
                interesting_metric = f"volume={metrics.get('volume', 0)}, liquidity={metrics.get('liquidity', 0)}"

            lines.append(
                f"- {record.get('title')} | {interesting_metric} | {record.get('url') or 'n/a'}"
            )

    lines.append("")
    return "\n".join(lines)


def _date_range(end_date: date, days: int) -> list[date]:
    start = end_date - timedelta(days=days - 1)
    return [start + timedelta(days=offset) for offset in range(days)]


def report_dates(end_date: date, period: str) -> list[date]:
    if period == "weekly":
        return _date_range(end_date, 7)
    if period == "monthly":
        return _date_range(end_date, 30)
    raise ValueError(f"Unsupported period: {period}")


def _tag_counter(records: list[dict[str, Any]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for record in records:
        counter.update(record.get("tags", []))
    return counter


def _tag_source_layers(records: list[dict[str, Any]]) -> dict[str, dict[str, set[str]]]:
    result: dict[str, dict[str, set[str]]] = defaultdict(lambda: {"sources": set(), "layers": set(), "regions": set()})
    for record in records:
        for tag in record.get("tags", []):
            result[tag]["sources"].add(record.get("source", "unknown"))
            result[tag]["layers"].add(record.get("layer", "unknown"))
            result[tag]["regions"].add(record.get("region", "unknown"))
    return result


def _metric_sum(records: list[dict[str, Any]], source: str, metric: str) -> list[tuple[str, float]]:
    values: Counter[str] = Counter()
    for record in records:
        if record.get("source") != source:
            continue
        value = record.get("metrics", {}).get(metric)
        if value is None:
            continue
        try:
            values[str(record.get("title"))] += float(value)
        except (TypeError, ValueError):
            continue
    return values.most_common(10)


def _format_tags(counter: Counter[str], limit: int = 12) -> list[str]:
    if not counter:
        return ["- No tags found."]
    return [f"- {tag}: {count}" for tag, count in counter.most_common(limit)]


def _format_cross_source(records: list[dict[str, Any]], limit: int = 12) -> list[str]:
    tag_context = _tag_source_layers(records)
    rows = []
    for tag, context in tag_context.items():
        if len(context["sources"]) < 2 or len(context["layers"]) < 2:
            continue
        rows.append((tag, len(context["sources"]), len(context["layers"]), len(context["regions"])))
    rows.sort(key=lambda row: (row[1], row[2], row[3], row[0]), reverse=True)
    if not rows:
        return ["- No cross-source themes found."]
    return [
        f"- {tag}: sources={source_count}, layers={layer_count}, regions={region_count}"
        for tag, source_count, layer_count, region_count in rows[:limit]
    ]


def _format_global_jp_gap(records: list[dict[str, Any]], limit: int = 10) -> list[str]:
    global_counts: Counter[str] = Counter()
    jp_counts: Counter[str] = Counter()
    for record in records:
        tags = record.get("tags", [])
        if record.get("region") == "jp":
            jp_counts.update(tags)
        else:
            global_counts.update(tags)

    rows = []
    for tag, global_count in global_counts.items():
        jp_count = jp_counts.get(tag, 0)
        if global_count >= 2 and jp_count == 0:
            rows.append((tag, global_count, jp_count))
    rows.sort(key=lambda row: (row[1], row[0]), reverse=True)
    if not rows:
        return ["- No clear global-first gaps found."]
    return [f"- {tag}: global={global_count}, jp={jp_count}" for tag, global_count, jp_count in rows[:limit]]


def _format_signal_summary(signal_payloads: list[dict[str, Any]], limit: int = 12) -> list[str]:
    signals = []
    for payload in signal_payloads:
        signals.extend(payload.get("signals", []))
    if not signals:
        return ["- No signals found."]

    reason_counts = Counter(signal.get("reason", "unknown") for signal in signals)
    source_counts = Counter(signal.get("source", "unknown") for signal in signals)
    lines = ["### By reason", ""]
    lines.extend(f"- {reason}: {count}" for reason, count in reason_counts.most_common(limit))
    lines.extend(["", "### By source", ""])
    lines.extend(f"- {source}: {count}" for source, count in source_counts.most_common(limit))
    return lines


def _format_action_candidates(records: list[dict[str, Any]], signal_payloads: list[dict[str, Any]]) -> list[str]:
    tag_counts = _tag_counter(records)
    signal_tags: Counter[str] = Counter()
    for payload in signal_payloads:
        for signal in payload.get("signals", []):
            signal_tags.update(signal.get("tags", []))

    action_tags = []
    for tag, count in tag_counts.most_common(40):
        score = count + (signal_tags.get(tag, 0) * 3)
        action_tags.append((tag, score, count, signal_tags.get(tag, 0)))
    action_tags.sort(key=lambda row: (row[1], row[2], row[0]), reverse=True)

    if not action_tags:
        return ["- Not enough data yet."]

    lines = []
    for tag, score, record_count, signal_count in action_tags[:8]:
        lines.append(
            f"- Watch `{tag}`: score={score}, records={record_count}, signals={signal_count}. "
            "Check whether this deserves a post, tool idea, or deeper private review."
        )
    return lines


def build_periodic_report(
    *,
    period: str,
    period_id: str,
    start_date: date,
    end_date: date,
    normalized_records: list[dict[str, Any]],
    signal_payloads: list[dict[str, Any]],
) -> str:
    title = "Weekly" if period == "weekly" else "Monthly"
    counts_by_source = Counter(record.get("source") for record in normalized_records)
    counts_by_layer = Counter(record.get("layer") for record in normalized_records)
    tag_counts = _tag_counter(normalized_records)

    lines = [
        f"# {title} Trend Scan Report - {period_id}",
        "",
        "## Snapshot",
        "",
        f"- Period: {start_date.isoformat()} to {end_date.isoformat()}",
        f"- Window: {'7 days' if period == 'weekly' else '30 days'}",
        f"- Normalized records: {len(normalized_records)}",
        f"- Signal files: {len(signal_payloads)}",
        f"- Signals: {sum(len(payload.get('signals', [])) for payload in signal_payloads)}",
        "",
        "### By source",
        "",
    ]

    lines.extend(f"- {source}: {count}" for source, count in sorted(counts_by_source.items()))
    lines.extend(["", "### By layer", ""])
    lines.extend(f"- {layer}: {count}" for layer, count in sorted(counts_by_layer.items()))

    lines.extend(["", "## Strong Themes", ""])
    lines.extend(_format_tags(tag_counts))

    lines.extend(["", "## Cross-Source Themes", ""])
    lines.extend(_format_cross_source(normalized_records))

    lines.extend(["", "## Global-First JP Gaps", ""])
    lines.extend(_format_global_jp_gap(normalized_records))

    lines.extend(["", "## Signal Summary", ""])
    lines.extend(_format_signal_summary(signal_payloads))

    lines.extend(["", "## Behavioral Leaders", ""])
    github_rows = _metric_sum(normalized_records, "github", "stars")
    if github_rows:
        lines.extend(f"- {title}: stars={int(value)}" for title, value in github_rows)
    else:
        lines.append("- No GitHub leaders found.")

    lines.extend(["", "## Attention Leaders", ""])
    hn_rows = _metric_sum(normalized_records, "hackernews", "comments")
    if hn_rows:
        lines.extend(f"- {title}: comments={int(value)}" for title, value in hn_rows)
    else:
        lines.append("- No Hacker News leaders found.")

    lines.extend(["", "## Expectation Leaders", ""])
    market_rows = _metric_sum(normalized_records, "polymarket", "volume")
    if market_rows:
        lines.extend(f"- {title}: volume={int(value)}" for title, value in market_rows)
    else:
        lines.append("- No Polymarket leaders found.")

    lines.extend(["", "## Action Candidates", ""])
    lines.extend(_format_action_candidates(normalized_records, signal_payloads))

    lines.append("")
    return "\n".join(lines)
