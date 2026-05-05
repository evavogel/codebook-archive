"""Enrich stored candidates with data not captured during discovery.

Currently handles:
  - OSF: fetch contributor names from /v2/nodes/{id}/citation/
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time

from .config import DB_PATH, Settings
from .http_client import HTTPClientError, make_client, get_json

log = logging.getLogger(__name__)

OSF_BASE = "https://api.osf.io/v2"


def _fetch_osf_authors(client, node_id: str, auth_header: str) -> list[str] | None:
    """Return sorted bibliographic author names from the OSF citation endpoint.

    Returns None on any error so the caller can skip gracefully.
    """
    url = f"{OSF_BASE}/nodes/{node_id}/citation/"
    try:
        data = get_json(client, url, headers={"Authorization": auth_header})
    except (HTTPClientError, RuntimeError) as e:
        log.debug("citation fetch failed for %s: %s", node_id, e)
        return None
    authors_raw = data.get("data", {}).get("attributes", {}).get("author", [])
    names: list[str] = []
    for a in authors_raw:
        family = a.get("family", "")
        given = a.get("given", "")
        if family and given:
            names.append(f"{family}, {given}")
        elif family:
            names.append(family)
        elif given:
            names.append(given)
    return names if names else None


def enrich_authors(path=DB_PATH) -> dict[str, int]:
    """Fetch and store missing author data for OSF candidates."""
    settings = Settings.from_env()
    if not settings.osf_token:
        raise RuntimeError("OSF_TOKEN not set in .env")
    auth_header = f"Bearer {settings.osf_token}"

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT id, source_id FROM candidates WHERE source = 'osf' AND authors IS NULL"
    ).fetchall()

    log.info("Enriching authors for %d OSF candidates", len(rows))

    updated = 0
    skipped = 0
    client = make_client()

    for i, row in enumerate(rows, 1):
        names = _fetch_osf_authors(client, row["source_id"], auth_header)
        if names:
            conn.execute(
                "UPDATE candidates SET authors = ? WHERE id = ?",
                (json.dumps(names, ensure_ascii=False), row["id"]),
            )
            updated += 1
        else:
            skipped += 1

        if i % 10 == 0:
            conn.commit()
            log.info("  %d / %d done (%d updated, %d skipped)", i, len(rows), updated, skipped)

        time.sleep(0.3)

    conn.commit()
    conn.close()
    log.info("Author enrichment complete: %d updated, %d skipped", updated, skipped)
    return {"updated": updated, "skipped": skipped}
