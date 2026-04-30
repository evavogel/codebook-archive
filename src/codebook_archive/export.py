"""Export candidates to CSV for human review."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from .config import OUT_DIR
from .db import connect

CSV_FIELDS = [
    "id",
    "source",
    "source_id",
    "source_url",
    "title",
    "year",
    "authors",
    "license",
    "doi",
    "matched_codebook_terms",
    "matched_topic_terms",
    "review_status",
    "description",
    "file_urls",
    "discovered_at",
    "last_seen_at",
]


def _flatten(value):
    if value is None:
        return ""
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return value
    if isinstance(parsed, list):
        return "; ".join(str(x) for x in parsed)
    return value


def export_candidates(out_path: Path | None = None, status: str = "all") -> Path:
    out_path = out_path or (OUT_DIR / "candidates.csv")
    with connect() as conn:
        if status == "all":
            rows = conn.execute(
                f"SELECT {', '.join(CSV_FIELDS)} FROM candidates ORDER BY discovered_at DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {', '.join(CSV_FIELDS)} FROM candidates "
                "WHERE review_status = ? ORDER BY discovered_at DESC",
                (status,),
            ).fetchall()

    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_FIELDS)
        for row in rows:
            w.writerow(
                [
                    _flatten(row[k]) if k in {"authors", "matched_codebook_terms",
                                              "matched_topic_terms", "file_urls"}
                    else (row[k] if row[k] is not None else "")
                    for k in CSV_FIELDS
                ]
            )
    return out_path
