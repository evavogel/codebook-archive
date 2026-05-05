# Project Status

**Last updated:** 2026-04-30
**Current focus:** First candidates CSV produced. Awaiting Eva's review of `out/candidates.csv` to tune keywords and decide next sources.
**Where we left off:** Discovery pipeline ran end-to-end against OSF (file-search + node-search) and Zenodo with `max_pages=2`. **193 candidates** captured (169 OSF, 24 Zenodo) after keyword filtering — all in `pending` review status. The OSF node-search strategy was added after Eva pointed out a missed project (`q3k2d`) whose codebook file was not surfaced by OSF's file-search index. Node-search caught it and 150 others.
**Next steps:**
- Eva reviews `out/candidates.csv`. We tune `config/keywords.toml` based on the false positives observed.
- Drop or tighten broad terms that produced lots of noise (`social media`, `political` alone) — keep specific high-yield terms (`populism`, `polarization`, `political topics`, `negative campaigning`).
- Add file-extension awareness: `.docx`/`.pdf` codebook files weight higher than miscellaneous file matches.
- Add the LLM classifier step (Anthropic Haiku 4.5) to push precision above 90%.
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
