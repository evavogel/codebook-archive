# Project Status

**Last updated:** 2026-04-30
**Current focus:** First candidates CSV produced. Awaiting Eva's review of `out/candidates.csv` to tune keywords and decide next sources.
**Where we left off:** Discovery pipeline ran end-to-end against OSF + Zenodo with `max_pages=2`. **42 candidates** captured (18 OSF, 24 Zenodo) after keyword filtering — all in `pending` review status. Roughly a third look like clear political-communication codebooks; the rest is keyword-collision noise (pedagogy, healthcare, NLP for Latin, etc.) that an LLM classifier will need to remove.
**Next steps:**
- Eva reviews `out/candidates.csv`. We tune `config/keywords.toml` based on the false positives observed.
- Re-run discovery with tuned keywords and `max_pages=4`.
- Add Figshare, OpenAlex, and SocArXiv source workers.
- Add the LLM classifier step (Anthropic Haiku 4.5) once keyword precision is acceptable.
- Build the Streamlit review UI.
- Build the MkDocs static site published to `codebooks.emvogel.com`.
- Set up the monthly GitHub Actions cron (kept disabled until manual runs are stable).

## Architecture (current)

```
discover (per-source workers, keyword filter)
  → SQLite candidates table (review_status='pending')
  → CSV export for human review
  → [later] LLM classifier
  → [later] PDF/text extraction for OA-licensed records
  → [later] Hugging Face dataset publish
  → [later] MkDocs site at codebooks.emvogel.com
```

## Decisions

- **Languages:** English + German codebooks. Site UI is English-only.
- **Curation:** human-in-the-loop. Eva reviews a candidates queue. Public site shows only `reviewed='accepted'` entries.
- **Storage:** mirror PDFs only when license is permissive (CC-BY, CC0, public domain, OSF default-public). Always cache extracted text. Closed-access SI: link + metadata only.
- **Hosting:** GitHub Pages from `evavogel/codebook-archive`, custom domain `codebooks.emvogel.com`.
- **Dataset:** published to Hugging Face Datasets (metadata + extracted text, no binaries).
- **License:** code MIT; metadata + extracted text dataset CC-BY 4.0; site content CC-BY 4.0.
- **Cron:** monthly, 1st of the month, 03:00 UTC (not yet enabled).
