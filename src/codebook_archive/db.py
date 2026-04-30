"""SQLite store for discovered candidates.

A candidate is a single discovered item from a source — identified by
(source, source_id). Records are upserted on rediscovery so we keep a
stable last_seen_at and don't lose the human review status.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS candidates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    source_url      TEXT NOT NULL,
    title           TEXT,
    description     TEXT,
    authors         TEXT,
    year            INTEGER,
    doi             TEXT,
    license         TEXT,
    file_urls       TEXT,
    matched_codebook_terms TEXT,
    matched_topic_terms    TEXT,
    raw_metadata    TEXT,
    discovered_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    review_status   TEXT NOT NULL DEFAULT 'pending',
    review_notes    TEXT,
    UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_candidates_status   ON candidates(review_status);
CREATE INDEX IF NOT EXISTS idx_candidates_source   ON candidates(source);
CREATE INDEX IF NOT EXISTS idx_candidates_last_seen ON candidates(last_seen_at);
"""


@contextmanager
def connect(path: Path = DB_PATH) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        yield conn
        conn.commit()
    finally:
        conn.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _serialize(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def upsert_candidate(
    conn: sqlite3.Connection,
    *,
    source: str,
    source_id: str,
    source_url: str,
    title: str | None,
    description: str | None,
    authors: list[str] | None,
    year: int | None,
    doi: str | None,
    license: str | None,
    file_urls: list[str] | None,
    matched_codebook_terms: list[str],
    matched_topic_terms: list[str],
    raw_metadata: dict[str, Any] | None,
) -> bool:
    """Insert or update a candidate. Returns True if the row was new."""
    now = _now()
    cur = conn.execute(
        "SELECT id FROM candidates WHERE source = ? AND source_id = ?",
        (source, source_id),
    )
    existing = cur.fetchone()
    if existing is None:
        conn.execute(
            """
            INSERT INTO candidates (
                source, source_id, source_url, title, description,
                authors, year, doi, license, file_urls,
                matched_codebook_terms, matched_topic_terms, raw_metadata,
                discovered_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                source_id,
                source_url,
                title,
                description,
                _serialize(authors),
                year,
                doi,
                license,
                _serialize(file_urls),
                _serialize(matched_codebook_terms),
                _serialize(matched_topic_terms),
                _serialize(raw_metadata),
                now,
                now,
            ),
        )
        return True
    conn.execute(
        """
        UPDATE candidates
           SET source_url = ?, title = ?, description = ?,
               authors = ?, year = ?, doi = ?, license = ?, file_urls = ?,
               matched_codebook_terms = ?, matched_topic_terms = ?,
               raw_metadata = ?, last_seen_at = ?
         WHERE id = ?
        """,
        (
            source_url,
            title,
            description,
            _serialize(authors),
            year,
            doi,
            license,
            _serialize(file_urls),
            _serialize(matched_codebook_terms),
            _serialize(matched_topic_terms),
            _serialize(raw_metadata),
            now,
            existing["id"],
        ),
    )
    return False
