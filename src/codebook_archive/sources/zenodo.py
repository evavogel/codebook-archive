"""Zenodo source worker.

Searches Zenodo records whose metadata mentions a codebook term,
then keeps those that also mention a political-communication topic.

Zenodo API docs: https://developers.zenodo.org/
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from ..config import Keywords, Settings
from ..db import upsert_candidate
from ..filtering import is_candidate
from ..http_client import get_json, make_client

log = logging.getLogger(__name__)

API = "https://zenodo.org/api/records"
PAGE_SIZE = 50


def _auth_headers(settings: Settings) -> dict[str, str]:
    if settings.zenodo_token:
        return {"Authorization": f"Bearer {settings.zenodo_token}"}
    return {}


def _search(
    client, term: str, settings: Settings, max_pages: int
) -> Iterator[dict[str, Any]]:
    base_params: dict[str, Any] = {
        "q": term,
        "size": PAGE_SIZE,
        "sort": "mostrecent",
    }
    headers = _auth_headers(settings)
    page = 1
    while page <= max_pages:
        params = {**base_params, "page": page}
        data = get_json(client, API, params=params, headers=headers)
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        for hit in hits:
            yield hit
        if len(hits) < PAGE_SIZE:
            break
        page += 1


def _record_text(record: dict[str, Any]) -> str:
    md = record.get("metadata", {}) or {}
    parts = [
        md.get("title") or "",
        md.get("description") or "",
        " ".join(k.get("keyword", "") if isinstance(k, dict) else str(k) for k in md.get("keywords", []) or []),
        " ".join(s.get("subject", "") if isinstance(s, dict) else str(s) for s in md.get("subjects", []) or []),
    ]
    files_text = " ".join(f.get("key", "") for f in record.get("files", []) or [])
    parts.append(files_text)
    return " ".join(p for p in parts if p)


def run(
    conn,
    keywords: Keywords,
    settings: Settings,
    *,
    max_pages_per_term: int = 4,
) -> dict[str, int]:
    """Discover Zenodo candidates. Returns counts dict."""
    new = 0
    updated = 0
    skipped = 0
    with make_client() as client:
        for term in keywords.codebook_all:
            log.info("Zenodo: searching records for %r (so far: new=%d updated=%d skipped=%d)",
                     term, new, updated, skipped)
            term_new = term_updated = term_skipped = 0
            for hit in _search(client, term, settings, max_pages_per_term):
                searchable = _record_text(hit)
                kept, cb, tp = is_candidate(text=searchable, keywords=keywords)
                if not kept:
                    skipped += 1
                    term_skipped += 1
                    continue
                md = hit.get("metadata", {}) or {}
                authors = [c.get("name") for c in md.get("creators", []) or [] if c.get("name")]
                pub_date = md.get("publication_date") or ""
                year = int(pub_date[:4]) if pub_date[:4].isdigit() else None
                file_urls = [
                    f.get("links", {}).get("self")
                    for f in hit.get("files", []) or []
                    if f.get("links", {}).get("self")
                ]
                license_id = None
                lic = md.get("license")
                if isinstance(lic, dict):
                    license_id = lic.get("id")
                elif isinstance(lic, str):
                    license_id = lic
                is_new = upsert_candidate(
                    conn,
                    source="zenodo",
                    source_id=str(hit.get("id")),
                    source_url=hit.get("links", {}).get("html")
                    or f"https://zenodo.org/records/{hit.get('id')}",
                    title=md.get("title"),
                    description=md.get("description"),
                    authors=authors or None,
                    year=year,
                    doi=md.get("doi") or hit.get("doi"),
                    license=license_id,
                    file_urls=file_urls or None,
                    matched_codebook_terms=cb,
                    matched_topic_terms=tp,
                    raw_metadata={"resource_type": md.get("resource_type")},
                )
                if is_new:
                    new += 1
                    term_new += 1
                else:
                    updated += 1
                    term_updated += 1
            conn.commit()
            log.info("Zenodo: term %r done — new=%d updated=%d skipped=%d",
                     term, term_new, term_updated, term_skipped)
    return {"new": new, "updated": updated, "skipped": skipped}
