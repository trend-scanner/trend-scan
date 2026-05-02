from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class RunContext:
    run_date: date
    timezone: str
    snapshot_at: datetime
    explicit_date: bool = False

    @property
    def run_date_str(self) -> str:
        return self.run_date.isoformat()


def build_run_context(date_arg: str | None, settings: dict | None = None) -> RunContext:
    settings = settings or {}
    metadata = settings.get("sources", {}).get("metadata", {})
    timezone_name = os.getenv("TREND_SCAN_TIMEZONE") or metadata.get("timezone") or "Asia/Tokyo"
    timezone = ZoneInfo(timezone_name)

    if date_arg:
        run_date = date.fromisoformat(date_arg)
        snapshot_at = datetime.combine(run_date + timedelta(days=1), time.min, timezone)
        return RunContext(
            run_date=run_date,
            timezone=timezone_name,
            snapshot_at=snapshot_at,
            explicit_date=True,
        )

    snapshot_at = datetime.now(timezone)
    return RunContext(
        run_date=snapshot_at.date(),
        timezone=timezone_name,
        snapshot_at=snapshot_at,
        explicit_date=False,
    )


def expand_template(value: str, context: RunContext) -> str:
    replacements = {
        "{run_date}": context.run_date.isoformat(),
        "{recent_7d}": (context.run_date - timedelta(days=7)).isoformat(),
        "{recent_30d}": (context.run_date - timedelta(days=30)).isoformat(),
        "{recent_90d}": (context.run_date - timedelta(days=90)).isoformat(),
    }

    expanded = value
    for token, replacement in replacements.items():
        expanded = expanded.replace(token, replacement)
    return expanded


def previous_date(run_date: date) -> date:
    return run_date - timedelta(days=1)


def wikipedia_observed_date(context: RunContext) -> date:
    if context.explicit_date:
        return context.run_date
    return context.run_date - timedelta(days=1)
