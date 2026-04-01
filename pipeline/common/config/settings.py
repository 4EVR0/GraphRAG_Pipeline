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
    request_sleep: float = float(os.getenv("REQUEST_SLEEP", "0.34"))

    # PubMed search
    search_limit: int = int(os.getenv("SEARCH_LIMIT", "20"))

    # Data
    target_csv_path: str = os.getenv("TARGET_CSV_PATH", "data/target_ingredients.csv")

    # Bronze
    bronze_root_dir: str = os.getenv("BRONZE_ROOT_DIR", "bronze")
    bronze_domain_dir: str = os.getenv("BRONZE_DOMAIN_DIR", "pubmed")
    enable_db_upsert: bool = os.getenv("ENABLE_DB_UPSERT", "true").lower() == "true"

    @property
    def bronze_pubmed_dir(self) -> Path:
        return self.base_dir / self.bronze_root_dir / self.bronze_domain_dir


settings = Settings()