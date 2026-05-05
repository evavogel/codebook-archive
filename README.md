# codebook-archive

A curated, searchable archive of political-communication codebooks discovered from open sources (OSF, Zenodo, Figshare, OpenAlex, SocArXiv, GitHub).

Maintained by Eva Vogel (UZH). Site: https://emvogel.com/codebook-archive/

## What this is

We code social-media and political-communication content for variables such as: political vs. non-political, type of politics, topics discussed, negative campaigning, partisanship, populist rhetoric, framing. Reusing codebooks from prior published work is preferable to reinventing them. This project collects and curates such codebooks so other researchers can find and cite them.

## Pipeline

1. Discovery — per-source workers query open repositories with English + German keywords.
2. Filter — keyword match against political-communication topic terms.
3. Review — human-in-the-loop accept/reject of candidates.
4. Classification — LLM-assisted concept tagging.
5. Extract — text extraction (and PDF mirroring where the license allows).
6. Publish — static site + Hugging Face dataset; refreshed monthly.

## Quickstart

```bash
python -m pip install -e .
cp .env.example .env   # fill in tokens
python -m codebook_archive discover --source osf -v
python -m codebook_archive discover --source zenodo -v
python -m codebook_archive stats
python -m codebook_archive export
```

## License

Code: MIT. Dataset (metadata + extracted text): CC-BY 4.0.
