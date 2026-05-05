"""Generate MkDocs markdown pages from the curated candidates database.

Run via:  python -m codebook_archive build-site

Writes:
  site/docs/index.md          — browsable table of all accepted entries
  site/docs/codebooks/<id>.md — one page per accepted codebook
  site/docs/tags.md           — MkDocs tags index stub
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

from .config import DB_PATH, REPO_ROOT

log = logging.getLogger(__name__)

DOCS_DIR = REPO_ROOT / "site" / "docs"
CODEBOOKS_DIR = DOCS_DIR / "codebooks"

SOURCE_LABELS = {
    "osf": "OSF",
    "zenodo": "Zenodo",
}

CONCEPT_TAG_MAP = {
    "populism": "Populism",
    "populist": "Populism",
    "negative campaigning": "Negative Campaigning",
    "partisan": "Partisanship",
    "partisanship": "Partisanship",
    "polarization": "Polarization",
    "polarisation": "Polarization",
    "incivility": "Incivility",
    "hate speech": "Hate Speech",
    "framing": "Framing",
    "disinformation": "Disinformation",
    "misinformation": "Disinformation",
    "influencer": "Political Influencers",
    "social media": "Social Media",
    "election": "Elections",
    "campaign": "Campaigns",
    "political content": "Political Content",
    "extremism": "Extremism",
}


def _slug(text: str, max_len: int = 50) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")[:max_len]


def _concepts_to_tags(concepts_json: str | None) -> list[str]:
    if not concepts_json:
        return []
    try:
        concepts = json.loads(concepts_json)
    except (json.JSONDecodeError, TypeError):
        return []
    tags: set[str] = set()
    for c in concepts:
        lower = c.lower()
        for keyword, tag in CONCEPT_TAG_MAP.items():
            if keyword in lower:
                tags.add(tag)
    return sorted(tags)


def _flatten_json(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return ", ".join(str(x) for x in parsed)
        return str(parsed)
    except (json.JSONDecodeError, TypeError):
        return value or ""


def _availability_badge(file_available: int | None) -> str:
    if file_available == 1:
        return "**Files available**"
    if file_available == 0:
        return "Files unavailable"
    return "Availability not checked"


def _source_badge(source: str) -> str:
    return SOURCE_LABELS.get(source, source.upper())


def _build_codebook_page(row: sqlite3.Row) -> str:
    title = row["title"] or "Untitled"
    tags = _concepts_to_tags(row["classifier_concepts"])
    tags_yaml = "\n".join(f"  - {t}" for t in tags) if tags else ""

    authors_str = _flatten_json(row["authors"]) or "Unknown"
    year = row["year"] or "n.d."
    source_label = _source_badge(row["source"])
    source_url = row["source_url"] or ""
    doi = row["doi"] or ""
    license_str = row["license"] or "Not specified"
    description = (row["description"] or "").strip()[:800]
    concepts_str = _flatten_json(row["classifier_concepts"])
    confidence = f'{float(row["classifier_confidence"] or 0):.0%}'
    reasoning = row["classifier_reasoning"] or ""
    cb_terms = _flatten_json(row["matched_codebook_terms"])
    topic_terms = _flatten_json(row["matched_topic_terms"])
    avail = _availability_badge(row["file_available"])

    frontmatter = f"""---
title: "{title.replace('"', "'")}"
tags:
{tags_yaml if tags_yaml else "  - Uncategorised"}
---
"""

    doi_line = f"\n**DOI:** [{doi}](https://doi.org/{doi})" if doi else ""
    desc_block = f"\n## Description\n\n{description}\n" if description else ""
    concepts_block = f"\n**Concepts:** {concepts_str}" if concepts_str else ""

    body = f"""# {title}

| Field | Value |
|---|---|
| Source | [{source_label}]({source_url}) |
| Year | {year} |
| Authors | {authors_str} |
| License | {license_str} |
| Availability | {avail} |
{f"| DOI | [{doi}](https://doi.org/{doi}) |" if doi else ""}

{desc_block}

## Classification

> {reasoning}

**Classifier confidence:** {confidence}
{concepts_block}

??? note "Keyword matches"
    **Codebook terms matched:** {cb_terms}

    **Topic terms matched:** {topic_terms}

[View on {source_label} :octicons-link-external-16:]({source_url}){{ .md-button .md-button--primary }}
"""
    return frontmatter + body


def _build_index(rows: list[sqlite3.Row]) -> str:
    lines = [
        "---",
        "title: Codebook Archive",
        "---",
        "",
        "# Political Communication Codebook Archive",
        "",
        "A curated collection of codebooks and annotation schemes used in "
        "political communication research. All entries have been verified by "
        "automated classification and are available from open-access repositories.",
        "",
        f"**{len(rows)} codebooks** indexed from OSF and Zenodo.",
        "",
        "Use the search bar above or browse by tag in the navigation.",
        "",
        "---",
        "",
        "| Title | Source | Year | Concepts |",
        "|---|---|---|---|",
    ]
    for row in rows:
        title = (row["title"] or "Untitled").replace("|", "—")
        slug = _slug(title)
        page_path = f"codebooks/{row['id']}-{slug}"
        source_label = _source_badge(row["source"])
        year = row["year"] or "n.d."
        tags = _concepts_to_tags(row["classifier_concepts"])
        tags_str = ", ".join(tags[:4]) if tags else "—"
        lines.append(f"| [{title[:70]}]({page_path}.md) | {source_label} | {year} | {tags_str} |")

    return "\n".join(lines) + "\n"


def build(accepted_only: bool = True) -> dict[str, int]:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    CODEBOOKS_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    where = "WHERE review_status = 'accepted'" if accepted_only else ""
    rows = conn.execute(
        f"SELECT * FROM candidates {where} ORDER BY year DESC, title"
    ).fetchall()
    conn.close()

    log.info("Building site for %d codebooks", len(rows))

    written = 0
    for row in rows:
        title = row["title"] or "untitled"
        fname = f"{row['id']}-{_slug(title)}.md"
        page = _build_codebook_page(row)
        (CODEBOOKS_DIR / fname).write_text(page, encoding="utf-8")
        written += 1

    # Index page
    (DOCS_DIR / "index.md").write_text(_build_index(rows), encoding="utf-8")

    # Tags stub required by the tags plugin
    (DOCS_DIR / "tags.md").write_text(
        "---\ntitle: Browse by Tag\n---\n\n# Browse by Tag\n\n[TAGS]\n",
        encoding="utf-8",
    )

    # CNAME for GitHub Pages custom domain
    cname_path = REPO_ROOT / "site" / "docs" / "CNAME"
    # The CNAME must be in site_dir (build output), handled via docs extra
    (REPO_ROOT / "site" / "build").mkdir(parents=True, exist_ok=True)

    log.info("Wrote %d codebook pages + index", written)
    return {"codebooks": written}
