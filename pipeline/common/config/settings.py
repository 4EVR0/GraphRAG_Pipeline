import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)


@dataclass(frozen=True)
class Settings:
    """
    Application configuration loaded from .env
    """

    # Base
    base_dir: Path = BASE_DIR

    # PubMed
    ncbi_base: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    ncbi_email: str = os.getenv("NCBI_EMAIL", "")
    ncbi_tool: str = os.getenv("NCBI_TOOL", "graph_rag_ingestor")
    ncbi_api_key: str | None = os.getenv("NCBI_API_KEY")

    # Database
    database_url: str | None = os.getenv("DATABASE_URL")

    # Request control
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    # NCBI 허용량: API key 없으면 초당 3회, 있으면 초당 10회
    request_sleep: float = float(
        os.getenv("REQUEST_SLEEP", "0.11" if os.getenv("NCBI_API_KEY") else "0.34")
    )
    request_max_retries: int = int(os.getenv("REQUEST_MAX_RETRIES", "4"))

    # PubMed search (per-ingredient PMID cap; raise via SEARCH_LIMIT for larger corpora)
    search_limit: int = int(os.getenv("SEARCH_LIMIT", "50"))

    # PubMed 수집 방식: ingredient(성분별 상위 SEARCH_LIMIT건) | narrow(성분×효능 좁은 검색, 결과 전부)
    pubmed_collection_mode: str = os.getenv("PUBMED_COLLECTION_MODE", "ingredient")
    narrow_pairs_csv: str = os.getenv("NARROW_PAIRS_CSV", "config/pubmed_narrow/tier1_pairs.csv")
    narrow_effect_terms_csv: str = os.getenv(
        "NARROW_EFFECT_TERMS_CSV", "config/pubmed_narrow/effect_terms.csv"
    )
    narrow_ingredient_rules_csv: str = os.getenv(
        "NARROW_INGREDIENT_RULES_CSV", "config/pubmed_narrow/ingredient_rules.csv"
    )
    # 조합당 결과가 이 값 이하면 전부, 넘으면 사람 대상 임상·리뷰로 좁힌 뒤 전부 가져온다
    narrow_full_fetch_max: int = int(os.getenv("NARROW_FULL_FETCH_MAX", "300"))
    # 좁힌 뒤에도 넘으면 이 값까지만 가져오고 capped로 기록한다(esearch 상한 9,999)
    narrow_pair_cap: int = int(os.getenv("NARROW_PAIR_CAP", "5000"))

    # Target ingredients (repo-root relative or absolute; see target_ingredients_path)
    target_csv_path: str = os.getenv("TARGET_CSV_PATH", "config/target_ingredients.csv")

    # Bronze
    bronze_root_dir: str = os.getenv("BRONZE_ROOT_DIR", "bronze")
    bronze_domain_dir: str = os.getenv("BRONZE_DOMAIN_DIR", "pubmed")
    enable_db_upsert: bool = os.getenv("ENABLE_DB_UPSERT", "true").lower() == "true"

    # Silver
    silver_root_dir: str = os.getenv("SILVER_ROOT_DIR", "silver")
    silver_domain_dir: str = os.getenv("SILVER_DOMAIN_DIR", "paper")
    enable_chunk_db_upsert: bool = os.getenv("ENABLE_CHUNK_DB_UPSERT", "false").lower() == "true"

    # Gold
    gold_root_dir: str = os.getenv("GOLD_ROOT_DIR", "gold")
    gold_domain_dir: str = os.getenv("GOLD_DOMAIN_DIR", "claim")
    enable_claim_db_upsert: bool = os.getenv("ENABLE_CLAIM_DB_UPSERT", "false").lower() == "true"
    gold_test_chunk_limit: int = int(os.getenv("GOLD_TEST_CHUNK_LIMIT", "100000"))
    gold_debug_print_limit: int = int(os.getenv("GOLD_DEBUG_PRINT_LIMIT", "10"))
    extractor_version: str = os.getenv("EXTRACTOR_VERSION", "llm_claim_extractor_v1")
    validator_version: str = os.getenv("VALIDATOR_VERSION", "claim_validator_v1")
    mapping_version: str = os.getenv("MAPPING_VERSION", "taxonomy_mapping_v1")

    # Chunk policy
    chunk_max_chars: int = int(os.getenv("CHUNK_MAX_CHARS", "1000"))
    chunk_overlap_chars: int = int(os.getenv("CHUNK_OVERLAP_CHARS", "150"))
    chunk_version: str = os.getenv("CHUNK_VERSION", "abstract_char_window_v1")

    @property
    def bronze_pubmed_dir(self) -> Path:
        return self.base_dir / self.bronze_root_dir / self.bronze_domain_dir

    @property
    def silver_paper_dir(self) -> Path:
        return self.base_dir / self.silver_root_dir / self.silver_domain_dir

    @property
    def gold_claim_dir(self) -> Path:
        return self.base_dir / self.gold_root_dir / self.gold_domain_dir

    @property
    def target_ingredients_path(self) -> Path:
        """Resolved path to target ingredient CSV (cwd-independent)."""
        p = Path(self.target_csv_path)
        resolved = p if p.is_absolute() else self.base_dir / p
        config_path = self.base_dir / "config" / "target_ingredients.csv"
        strict = os.getenv("STRICT_TARGET_CSV", "").lower() in ("1", "true", "yes")

        # Many .env files still point at data/target_ingredients.csv (short list).
        # Prefer versioned config when both exist unless STRICT_TARGET_CSV is set.
        if not strict and config_path.exists():
            try:
                legacy_data = (self.base_dir / "data" / "target_ingredients.csv").resolve()
                if resolved.exists() and resolved.resolve() == legacy_data:
                    return config_path
            except OSError:
                pass

        if resolved.exists():
            return resolved
        if config_path.exists():
            return config_path
        return resolved


settings = Settings()