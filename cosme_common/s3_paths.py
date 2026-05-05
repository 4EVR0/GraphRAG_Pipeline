BUCKET = "oliveyoung-crawl-data"

# Oliveyoung Crawling (Bronze)
BRONZE_PREFIX = "oliveyoung"
BRONZE_GLOB = f"s3://{BUCKET}/{BRONZE_PREFIX}/*/*/run_id=*/*.json"
MANIFEST_KEY_TEMPLATE = "oliveyoung/_manifests/run_id={run_id}/manifest.json"


def bronze_prefix(main_cat: str, sub_cat: str, run_id: str) -> str:
    """s3://bucket/oliveyoung/{main_cat}/{sub_cat}/run_id={run_id}"""
    return f"s3://{BUCKET}/{BRONZE_PREFIX}/{main_cat}/{sub_cat}/run_id={run_id}"


def manifest_key(run_id: str) -> str:
    """S3 manifest 경로 반환"""
    return MANIFEST_KEY_TEMPLATE.format(run_id=run_id)


# INCI Pipeline (Silver output → Iceberg pipeline이 읽는 경로)
KCIA_PREFIX = "INCI_data_silver/kcia_cosing"
KCIA_GLOB = f"s3://{BUCKET}/{KCIA_PREFIX}/batch=*/kcia_cosing_matched_final.csv"


def kcia_batch_key(batch_month: str) -> str:
    """batch_month: 'YYYY-MM' 형식. ex) '2026-05'"""
    return f"{KCIA_PREFIX}/batch={batch_month}/kcia_cosing_matched_final.csv"


# Iceberg Silver / Gold
SILVER_CURRENT_PATH = f"s3://{BUCKET}/silver/current/"
SILVER_HISTORY_PATH = f"s3://{BUCKET}/silver/history/"
SILVER_ERROR_PATH = f"s3://{BUCKET}/silver/error/raw/"
CATEGORY_MASTER_PATH = f"s3://{BUCKET}/olive_young_category_master/"
GOLD_PATH = f"s3://{BUCKET}/olive_young_gold/"
ICEBERG_METADATA_PATH = f"s3://{BUCKET}/olive_young_iceberg_metadata/"
DATA_CSV_PATH = f"s3://{BUCKET}/data_csv/"

# INCI Pipeline (자체 업로드 경로)
INCI_BRONZE_KCIA_PREFIX = "INCI_data_bronze/kcia"
INCI_BRONZE_COSING_PREFIX = "INCI_data_bronze/cosing"
INCI_SILVER_PREFIX = "INCI_data_silver/kcia_cosing"
INCI_GOLD_PREFIX = "INCI_data_gold/kcia_cosing"

# GraphRAG Pipeline
GRAPHRAG_BRONZE_PREFIX = "graphrag/bronze/pubmed"
GRAPHRAG_SILVER_PREFIX = "graphrag/silver/paper"
GRAPHRAG_GOLD_PREFIX = "graphrag/gold/claim"
