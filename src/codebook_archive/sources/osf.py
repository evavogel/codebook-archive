"""OSF source worker.

Searches OSF for files whose name matches a codebook term, then keeps
those whose containing project mentions a political-communication topic.

OSF API docs: https://developer.osf.io/
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from ..config import Keywords, Settings
from ..db import upsert_candidate
from ..filtering import is_candidate
from ..http_client import HTTPClientError, get_json, make_client

log = logging.getLogger(__name__)

API = "https://api.osf.io/v2"
PAGE_SIZE = 50


def _auth_headers(settings: Settings) -> dict[str, str]:
    if settings.osf_token:
        return {"Authorization": f"Bearer {settings.osf_token}"}
    return {}


def _search_files(
    client, term: str, headers: dict[str, str], max_pages: int
) -> Iterator[dict[str, Any]]:
    url = f"{API}/search/files/"
    params = {"q": term, "page[size]": PAGE_SIZE}
    pages = 0
    while url and pages < max_pages:
        data = get_json(client, url, params=params, headers=headers)
        for hit in data.get("data", []):
            yield hit
        url = data.get("links", {}).get("next")
        params = None  # next URL already encodes them
        pages += 1


def _node_metadata(client, node_id: str, headers: dict[str, str]) -> dict[str, Any] | None:
    try:
        data = get_json(client, f"{API}/nodes/{node_id}/", headers=headers)
        return data.get("data", {})
    except HTTPClientError:
        return None  # private / deleted / not found — skip silently
    except Exception as e:
        log.warning("OSF node fetch failed for %s: %s", node_id, e)
        return None


def _node_text(node: dict[str, Any]) -> str:
    a = node.get("attributes", {}) or {}
    parts = [
        a.get("title") or "",
        a.get("description") or "",
        " ".join(a.get("tags", []) or []),
        a.get("category") or "",
    ]
    return " ".join(p for p in parts if p)


def run(
    conn,
    keywords: Keywords,
    settings: Settings,
    *,
    max_pages_per_term: int = 4,
) -> dict[str, int]:
    """Discover OSF candidates. Returns counts dict."""
    headers = _auth_headers(settings)
    seen_nodes: dict[str, dict[str, Any]] = {}
    new = 0
    updated = 0
    skipped = 0

    with make_client() as client:
        for term in keywords.codebook_all:
            log.info("OSF: searching files for %r (so far: new=%d updated=%d skipped=%d)",
                     term, new, updated, skipped)
            term_new = term_updated = term_skipped = 0
            for hit in _search_files(client, term, headers, max_pages_per_term):
                attrs = hit.get("attributes", {}) or {}
                file_name = attrs.get("name") or ""
                node_id = (
                    hit.get("relationships", {})
                    .get("node", {})
                    .get("data", {})
                    .get("id")
                )
                if not node_id:
                    skipped += 1
                    term_skipped += 1
                    continue
                node = seen_nodes.get(node_id)
                if node is None:
                    node = _node_metadata(client, node_id, headers)
                    if node is None:
                        skipped += 1
                        term_skipped += 1
                        continue
                    seen_nodes[node_id] = node
                searchable = " ".join([file_name, _node_text(node)])
                kept, cb, tp = is_candidate(text=searchable, keywords=keywords)
                if not kept:
                    skipped += 1
                    term_skipped += 1
                    continue
                a = node.get("attributes", {}) or {}
                links = node.get("links", {}) or {}
                file_links = hit.get("links", {}) or {}
                file_url = file_links.get("download") or file_links.get("self")
                contributors = a.get("contributors", []) or []
                year = None
                date_created = a.get("date_created") or ""
                if date_created[:4].isdigit():
                    year = int(date_created[:4])
                is_new = upsert_candidate(
                    conn,
                    source="osf",
                    source_id=node_id,
                    source_url=links.get("html") or f"https://osf.io/{node_id}/",
                    title=a.get("title"),
                    description=a.get("description"),
                    authors=contributors if contributors else None,
                    year=year,
                    doi=None,
                    license=(a.get("node_license") or {}).get("id")
                    if isinstance(a.get("node_license"), dict)
                    else None,
                    file_urls=[file_url] if file_url else None,
                    matched_codebook_terms=cb,
                    matched_topic_terms=tp,
                    raw_metadata={"file_name": file_name, "node": a},
                )
                if is_new:
                    new += 1
                    term_new += 1
                else:
                    updated += 1
                    term_updated += 1
            conn.commit()
            log.info("OSF: term %r done — new=%d updated=%d skipped=%d",
                     term, term_new, term_updated, term_skipped)
    return {"new": new, "updated": updated, "skipped": skipped}
