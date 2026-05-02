from __future__ import annotations

from typing import Any

import requests


DEFAULT_TIMEOUT = 30


def build_session(user_agent: str = "trend-scan/0.1") -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
        }
    )
    return session


def get_json(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Any:
    response = session.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()
