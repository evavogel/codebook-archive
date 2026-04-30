"""Load configuration from .env and config/keywords.toml."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
TEXT_DIR = DATA_DIR / "text"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = DATA_DIR / "candidates.sqlite"
OUT_DIR = REPO_ROOT / "out"

for d in (DATA_DIR, RAW_DIR, TEXT_DIR, CACHE_DIR, OUT_DIR):
    d.mkdir(parents=True, exist_ok=True)

load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    anthropic_api_key: str | None
    github_token: str | None
    osf_token: str | None
    zenodo_token: str | None
    huggingface_token: str | None
    polite_pool_email: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY") or None,
            github_token=os.getenv("GITHUB_TOKEN") or None,
            osf_token=os.getenv("OSF_TOKEN") or None,
            zenodo_token=os.getenv("ZENODO_TOKEN") or None,
            huggingface_token=os.getenv("HUGGINGFACE_TOKEN") or None,
            polite_pool_email=os.getenv("POLITE_POOL_EMAIL", "eva_vogel@web.de"),
        )


@dataclass(frozen=True)
class Keywords:
    codebook_en: list[str]
    codebook_de: list[str]
    topic_en: list[str]
    topic_de: list[str]

    @property
    def codebook_all(self) -> list[str]:
        return self.codebook_en + self.codebook_de

    @property
    def topic_all(self) -> list[str]:
        return self.topic_en + self.topic_de

    @classmethod
    def load(cls) -> Keywords:
        path = REPO_ROOT / "config" / "keywords.toml"
        with path.open("rb") as f:
            data = tomllib.load(f)
        return cls(
            codebook_en=[t.lower() for t in data["codebook_terms"]["en"]],
            codebook_de=[t.lower() for t in data["codebook_terms"]["de"]],
            topic_en=[t.lower() for t in data["topic_terms"]["en"]],
            topic_de=[t.lower() for t in data["topic_terms"]["de"]],
        )
