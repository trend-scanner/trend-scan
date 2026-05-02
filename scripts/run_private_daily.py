from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trend_scan.collectors import rss  # noqa: E402
from trend_scan.config import REPO_ROOT, load_settings, load_yaml  # noqa: E402
from trend_scan.date_utils import build_run_context  # noqa: E402
from trend_scan.normalize import LAYER_BY_SOURCE  # noqa: E402
from trend_scan.storage import read_jsonl, write_json, write_jsonl  # noqa: E402
from trend_scan.tagging import compile_keyword_map, infer_tags, merge_tags  # noqa: E402


PRIVATE_CONFIG_PATH = "config/private_sources.yml"


def _private_raw_path(run_date_str: str, source_name: str) -> Path:
    return REPO_ROOT / "data" / "private" / "raw" / run_date_str / f"{source_name}.json"


def _private_normalized_path(run_date_str: str) -> Path:
    return REPO_ROOT / "data" / "private" / "normalized" / f"{run_date_str}.jsonl"


def _private_report_path(run_date_str: str) -> Path:
    return REPO_ROOT / "data" / "private" / "reports" / f"{run_date_str}.md"


def _normalize_private_rss(
    raw: dict[str, Any],
    run_date_str: str,
    keyword_map: dict[str, list[str]],
) -> list[dict[str, Any]]:
    raw_ref = str(_private_raw_path(run_date_str, "rss").relative_to(REPO_ROOT))
    rows: list[dict[str, Any]] = []

    for item in raw.get("items", []):
        tags = merge_tags(
            item.get("tags", []),
            infer_tags(keyword_map, item.get("title"), item.get("summary"), item.get("source_name")),
        )
        rows.append(
            {
                "date": run_date_str,
                "source": "rss",
                "region": item.get("region", "global"),
                "layer": LAYER_BY_SOURCE["rss"],
                "entity_key": str(item.get("item_id") or item.get("url") or item.get("title")),
                "title": item.get("title"),
                "url": item.get("url"),
                "published_at": item.get("published_at"),
                "metrics": {},
                "tags": tags,
                "raw_ref": raw_ref,
                "attributes": {
                    "source_id": item.get("source_id"),
                    "source_name": item.get("source_name"),
                    "source_kind": item.get("source_kind"),
                    "priority": bool(item.get("priority")),
                    "authors": item.get("authors", []),
                    "summary": item.get("summary"),
                    "private": True,
                },
            }
        )

    rows.sort(key=lambda row: (row["published_at"] or "", row["title"] or ""), reverse=True)
    return rows


def _build_private_report(run_date_str: str, rows: list[dict[str, Any]], raw: dict[str, Any]) -> str:
    lines = [
        f"# Private Daily Sources - {run_date_str}",
        "",
        f"- RSS records: {len(rows)}",
        f"- Feed count: {raw.get('meta', {}).get('feed_count', 0)}",
        f"- Errors: {len(raw.get('meta', {}).get('errors', []))}",
        "",
        "## Latest items",
        "",
    ]

    for row in rows[:20]:
        source_name = row.get("attributes", {}).get("source_name")
        lines.append(f"- {row.get('title')} | {source_name} | {row.get('url')}")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Run date in YYYY-MM-DD format.")
    args = parser.parse_args()

    private_sources = load_yaml(PRIVATE_CONFIG_PATH)
    feeds = private_sources.get("rss", {}).get("feeds", [])
    if not feeds:
        raise SystemExit(
            f"No private feeds configured. Create {PRIVATE_CONFIG_PATH} from "
            "config/private_sources.example.yml first."
        )

    public_settings = load_settings()
    private_settings = {
        "sources": {
            "metadata": private_sources.get("metadata", {}),
            "rss": private_sources.get("rss", {}),
        },
        "watchlists": {},
        "keywords": public_settings.get("keywords", {}),
    }
    keyword_map = compile_keyword_map(private_settings["keywords"])
    private_settings["keyword_map"] = keyword_map

    context = build_run_context(args.date, private_settings)
    raw_payload = rss.collect(context, private_settings)
    rows = _normalize_private_rss(raw_payload, context.run_date_str, keyword_map)

    write_json(_private_raw_path(context.run_date_str, "rss"), raw_payload)
    write_jsonl(_private_normalized_path(context.run_date_str), rows)

    report_path = _private_report_path(context.run_date_str)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        _build_private_report(context.run_date_str, rows, raw_payload),
        encoding="utf-8",
    )

    previous_rows = read_jsonl(_private_normalized_path(context.run_date_str))
    print(
        f"private_sources date={context.run_date_str} "
        f"rss_items={len(rows)} saved_records={len(previous_rows)} "
        f"errors={len(raw_payload.get('meta', {}).get('errors', []))}"
    )


if __name__ == "__main__":
    main()
