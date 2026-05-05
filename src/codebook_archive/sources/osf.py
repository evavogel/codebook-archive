"""OSF source worker.

Two complementary discovery strategies, both of which feed the same
candidates table:

  1. file-search:  find files whose content/name matches a codebook term,
                   then keep those whose containing project also matches a
                   political-communication topic term.
  2. node-search:  find projects whose title contains a political-comm topic
                   term, then keep those that have at least one codebook-like
                   file in their osfstorage tree.

Strategy 1 catches projects with weak titles but well-indexed files;
strategy 2 catches projects with strong topical titles whose codebook file
is not surfaced by the file-search index. Most projects are caught by both.

OSF API docs: https://developer.osf.io/
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from ..config import Keywords, Settings
from ..db import upsert_candidate
from ..filtering import is_candidate, match_terms
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


def _list_node_files(
    client, node_id: str, headers: dict[str, str], max_depth: int = 2
) -> list[str]:
    """Return file names within a node's osfstorage tree (folders walked up to max_depth)."""
    names: list[str] = []
    queue: list[tuple[str, int]] = [
        (f"{API}/nodes/{node_id}/files/osfstorage/", 0)
    ]
    visited: set[str] = set()
    while queue:
        url, depth = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            data = get_json(client, url, headers=headers)
        except HTTPClientError:
            continue
        except Exception as e:
            log.warning("OSF files listing failed for %s: %s", url, e)
            continue
        for entry in data.get("data", []):
            attrs = entry.get("attributes", {}) or {}
            kind = attrs.get("kind")
            name = attrs.get("name") or ""
            if kind == "file":
                names.append(name)
            elif kind == "folder" and depth < max_depth:
                rel = (
                    entry.get("relationships", {})
                    .get("files", {})
                    .get("links", {})
                    .get("related", {})
                    .get("href")
                )
                if rel:
                    queue.append((rel, depth + 1))
        nxt = data.get("links", {}).get("next")
        if nxt and len(visited) < 30:  # safety cap on per-node pagination
            queue.append((nxt, depth))
    return names


def run_node_search(
    conn,
    keywords: Keywords,
    settings: Settings,
    *,
    max_pages_per_term: int = 2,
) -> dict[str, int]:
    """Topic-driven discovery: find projects whose title contains a political-comm
    topic term, then check their files for codebook-shaped names."""
    headers = _auth_headers(settings)
    seen_nodes: set[str] = set()
    new = updated = skipped = 0
    with make_client() as client:
        for topic_term in keywords.topic_all:
            log.info(
                "OSF node-search: title icontains %r (so far: new=%d updated=%d skipped=%d)",
                topic_term, new, updated, skipped,
            )
            term_new = term_updated = term_skipped = 0
            page = 1
            while page <= max_pages_per_term:
                try:
                    data = get_json(
                        client,
                        f"{API}/nodes/",
                        params={
                            "filter[title][icontains]": topic_term,
                            "page[size]": 50,
                            "page": page,
                        },
                        headers=headers,
                    )
                except HTTPClientError as e:
                    log.warning("OSF node filter failed for %r p%d: %s", topic_term, page, e)
                    break
                nodes = data.get("data", []) or []
                if not nodes:
                    break
                for node in nodes:
                    node_id = node.get("id")
                    if not node_id or node_id in seen_nodes:
                        continue
                    seen_nodes.add(node_id)
                    a = node.get("attributes", {}) or {}
                    if not a.get("public"):
                        term_skipped += 1
                        continue
                    file_names = _list_node_files(client, node_id, headers)
                    cb_matches = sorted({
                        m
                        for fname in file_names
                        for m in match_terms(fname, keywords.codebook_all)
                    })
                    if not cb_matches:
                        term_skipped += 1
                        continue
                    title_text = " ".join([
                        a.get("title") or "",
                        a.get("description") or "",
                        " ".join(a.get("tags") or []),
                    ])
                    tp_matches = match_terms(title_text, keywords.topic_all) or [topic_term]
                    contributors = a.get("contributors", []) or []
                    date_created = a.get("date_created") or ""
                    year = int(date_created[:4]) if date_created[:4].isdigit() else None
                    links = node.get("links", {}) or {}
                    license_id = (
                        (a.get("node_license") or {}).get("id")
                        if isinstance(a.get("node_license"), dict)
                        else None
                    )
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
                        license=license_id,
                        file_urls=None,
                        matched_codebook_terms=cb_matches,
                        matched_topic_terms=tp_matches,
                        raw_metadata={"strategy": "node_search", "files_seen": file_names},
                    )
                    if is_new:
                        new += 1
                        term_new += 1
                    else:
                        updated += 1
                        term_updated += 1
                if len(nodes) < 50:
                    break
                page += 1
            conn.commit()
            log.info(
                "OSF node-search: %r done — new=%d updated=%d skipped=%d",
                topic_term, term_new, term_updated, term_skipped,
            )
    return {"new": new, "updated": updated, "skipped": skipped}


def run_file_search(
    conn,
    keywords: Keywords,
    settings: Settings,
    *,
    max_pages_per_term: int = 4,
) -> dict[str, int]:
    """Discover OSF candidates via the file-search strategy. Returns counts dict."""
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


def run(
    conn,
    keywords: Keywords,
    settings: Settings,
    *,
    max_pages_per_term: int = 4,
) -> dict[str, int]:
    """Run both OSF strategies (file-search + topic-driven node-search) and
    sum their counts. node-search uses fewer pages by default because each
    node entails a files-listing request."""
    log.info("OSF: starting file-search strategy")
    counts_file = run_file_search(conn, keywords, settings, max_pages_per_term=max_pages_per_term)
    log.info("OSF: file-search done %s", counts_file)
    log.info("OSF: starting node-search strategy")
    counts_node = run_node_search(conn, keywords, settings, max_pages_per_term=max(2, max_pages_per_term // 2))
    log.info("OSF: node-search done %s", counts_node)
    return {
        "new": counts_file["new"] + counts_node["new"],
        "updated": counts_file["updated"] + counts_node["updated"],
        "skipped": counts_file["skipped"] + counts_node["skipped"],
    }
