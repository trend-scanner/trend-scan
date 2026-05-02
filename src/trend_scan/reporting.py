from __future__ import annotations

from collections import Counter
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
