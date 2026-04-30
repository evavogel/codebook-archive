"""Keyword-based candidate filter.

A candidate is kept when its searchable text matches at least one
'codebook' term AND at least one 'topic' term.
"""

from __future__ import annotations

from .config import Keywords


def match_terms(text: str, terms: list[str]) -> list[str]:
    if not text:
        return []
    haystack = text.lower()
    return [t for t in terms if t in haystack]


def is_candidate(
    *,
    text: str,
    keywords: Keywords,
) -> tuple[bool, list[str], list[str]]:
    """Return (kept, matched_codebook_terms, matched_topic_terms)."""
    cb = match_terms(text, keywords.codebook_all)
    tp = match_terms(text, keywords.topic_all)
    return (bool(cb) and bool(tp), cb, tp)
