from __future__ import annotations

import re
from typing import Iterable


def compile_keyword_map(keywords_config: dict) -> dict[str, list[str]]:
    tags = keywords_config.get("tags", {})
    return {
        key: [term.strip().lower() for term in values if term and term.strip()]
        for key, values in tags.items()
    }


def _term_matches(term: str, haystack: str) -> bool:
    if term.isascii() and re.fullmatch(r"[a-z0-9][a-z0-9+.#_-]*", term):
        pattern = rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])"
        return re.search(pattern, haystack) is not None
    return term in haystack


def infer_tags(keyword_map: dict[str, list[str]], *values: str | None) -> list[str]:
    haystack = " ".join(value or "" for value in values).lower()
    tags: list[str] = []
    for tag, terms in keyword_map.items():
        if any(_term_matches(term, haystack) for term in terms):
            tags.append(tag)
    return sorted(set(tags))


def merge_tags(*groups: Iterable[str]) -> list[str]:
    tags = set()
    for group in groups:
        for tag in group:
            normalized = re.sub(r"\s+", "-", str(tag).strip().lower())
            if normalized:
                tags.add(normalized)
    return sorted(tags)


def has_signal_terms(keywords_config: dict, *values: str | None) -> bool:
    haystack = " ".join(value or "" for value in values).lower()
    for term in keywords_config.get("signal_terms", []):
        if _term_matches(term.lower(), haystack):
            return True
    return False
