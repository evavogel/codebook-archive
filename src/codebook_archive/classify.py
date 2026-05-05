"""LLM-based codebook classifier using Claude Haiku with prompt caching.

For each pending candidate we send the project title, description, matched
terms, and file names to the model.  It returns structured JSON that drives
the auto-review status.

Auto-status logic:
  accepted  — model says it IS a codebook AND IS political communication,
               confidence >= 0.75
  rejected  — model says NOT a codebook OR NOT political communication,
               confidence >= 0.75
  maybe     — low confidence, or model is uncertain on one dimension
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

import httpx
from anthropic import Anthropic

from .config import Settings

log = logging.getLogger(__name__)

CLASSIFIER_VERSION = "haiku-4-5-v1"

SYSTEM_PROMPT = """\
You are a research assistant helping to curate a database of codebooks used \
in political communication research.

A codebook (also: coding scheme, coding manual, annotation guidelines, \
Codebuch, Kodierschema, Kodierhandbuch) is a structured document that \
defines the categories, variables, and decision rules researchers use when \
manually or computationally coding media content.

You will receive metadata about an open-science project or repository file. \
Classify it on two dimensions:

1. IS_CODEBOOK: Does this project contain at least one codebook / coding \
scheme / annotation guideline as described above? A codebook is NOT a raw \
dataset, a statistical script, a survey questionnaire alone, a paper PDF, \
or a slide deck.

2. IS_POLITICAL_COMMUNICATION: Does the codebook cover concepts relevant to \
political communication research? Relevant concepts include (but are not \
limited to): political content classification, political topics, election \
campaigns, political advertising, populism / populist rhetoric, partisanship, \
negative campaigning, political incivility, political polarization, political \
framing, political social media, political influencers, disinformation / \
misinformation, hate speech with political dimensions, political agenda setting.

NOT relevant: codebooks for nursing care quality, music education, special \
education, pharmacy, autism intervention, literary spatial annotation, \
ancient history, purely technical NLP benchmarks unrelated to political \
content, or other non-political domains.

Return ONLY valid JSON with this exact schema (no markdown, no prose):
{
  "is_codebook": true/false,
  "is_political_communication": true/false,
  "confidence": 0.0-1.0,
  "concepts": ["list", "of", "relevant", "political-comm", "concepts"],
  "reasoning": "one sentence explaining the decision"
}

Confidence guide:
- 0.9+ : title or file name makes the classification unambiguous
- 0.7-0.9 : strong signal from description or matched terms, minor uncertainty
- 0.5-0.7 : mixed signals or very sparse metadata; genuinely unclear
- <0.5 : almost no usable signal
"""


def _migrate_db(conn: sqlite3.Connection) -> None:
    """Add classifier columns if they do not yet exist."""
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(candidates)").fetchall()
    }
    cols = {
        "classifier_version": "TEXT",
        "classifier_confidence": "REAL",
        "classifier_concepts": "TEXT",
        "classifier_status": "TEXT",
        "classifier_reasoning": "TEXT",
        "file_available": "INTEGER",  # 1=yes, 0=no, NULL=unchecked
    }
    for col, typ in cols.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE candidates ADD COLUMN {col} {typ}")
    conn.commit()


def _build_user_message(row: sqlite3.Row) -> str:
    parts: list[str] = []
    if row["title"]:
        parts.append(f"Title: {row['title']}")
    if row["description"]:
        desc = (row["description"] or "")[:500]
        parts.append(f"Description: {desc}")
    if row["matched_codebook_terms"]:
        parts.append(f"Matched codebook terms: {row['matched_codebook_terms']}")
    if row["matched_topic_terms"]:
        parts.append(f"Matched topic terms: {row['matched_topic_terms']}")
    raw = row["raw_metadata"]
    if raw:
        try:
            meta = json.loads(raw)
            files = meta.get("files_seen") or []
            if files:
                parts.append(f"File names in project: {', '.join(str(f) for f in files[:20])}")
            fname = meta.get("file_name")
            if fname:
                parts.append(f"Matched file name: {fname}")
        except (json.JSONDecodeError, TypeError):
            pass
    parts.append(f"Source URL: {row['source_url']}")
    return "\n".join(parts)


def _auto_status(result: dict[str, Any]) -> str:
    conf = float(result.get("confidence", 0))
    is_cb = bool(result.get("is_codebook"))
    is_pc = bool(result.get("is_political_communication"))
    if conf >= 0.75:
        if is_cb and is_pc:
            return "accepted"
        return "rejected"
    return "maybe"


def _check_file_availability(file_urls_json: str | None) -> int | None:
    """Return 1 if at least one URL is reachable, 0 if all fail, None if no URLs."""
    if not file_urls_json:
        return None
    try:
        urls = json.loads(file_urls_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(urls, list) or not urls:
        return None
    for url in urls[:2]:  # check first two at most
        if not isinstance(url, str) or not url.startswith("http"):
            continue
        try:
            r = httpx.head(url, timeout=8.0, follow_redirects=True)
            if r.status_code < 400:
                return 1
        except Exception:
            pass
    return 0


def run_classifier(
    conn: sqlite3.Connection,
    settings: Settings,
    *,
    model: str = "claude-haiku-4-5",
    check_files: bool = True,
    dry_run: bool = False,
) -> dict[str, int]:
    """Classify all pending candidates. Returns counts."""
    _migrate_db(conn)
    client = Anthropic(api_key=settings.anthropic_api_key)

    rows = conn.execute(
        "SELECT * FROM candidates WHERE classifier_version IS NULL ORDER BY id"
    ).fetchall()

    log.info("Classifying %d unclassified candidates with %s", len(rows), model)

    accepted = rejected = maybe = errors = 0

    for i, row in enumerate(rows):
        user_msg = _build_user_message(row)

        if dry_run:
            log.info("[dry-run] would classify id=%d: %s", row["id"], (row["title"] or "")[:60])
            continue

        try:
            resp = client.messages.create(
                model=model,
                max_tokens=256,
                system=[
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_msg}],
            )
            raw_text = resp.content[0].text.strip()
            # Strip any accidental markdown fences
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]
            result = json.loads(raw_text)
        except Exception as e:
            log.warning("Classifier error for id=%d: %s", row["id"], e)
            errors += 1
            continue

        status = _auto_status(result)
        file_avail = None
        if check_files:
            file_avail = _check_file_availability(row["file_urls"])

        conn.execute(
            """
            UPDATE candidates
               SET classifier_version    = ?,
                   classifier_confidence = ?,
                   classifier_concepts   = ?,
                   classifier_status     = ?,
                   classifier_reasoning  = ?,
                   review_status         = ?,
                   file_available        = ?
             WHERE id = ?
            """,
            (
                CLASSIFIER_VERSION,
                result.get("confidence"),
                json.dumps(result.get("concepts", []), ensure_ascii=False),
                status,
                result.get("reasoning"),
                status,
                file_avail,
                row["id"],
            ),
        )
        conn.commit()

        if status == "accepted":
            accepted += 1
        elif status == "rejected":
            rejected += 1
        else:
            maybe += 1

        if (i + 1) % 10 == 0:
            log.info(
                "Progress: %d/%d — accepted=%d rejected=%d maybe=%d errors=%d",
                i + 1, len(rows), accepted, rejected, maybe, errors,
            )
        # Small pause to stay well within rate limits
        time.sleep(0.3)

    log.info(
        "Classifier done: accepted=%d rejected=%d maybe=%d errors=%d",
        accepted, rejected, maybe, errors,
    )
    return {"accepted": accepted, "rejected": rejected, "maybe": maybe, "errors": errors}
