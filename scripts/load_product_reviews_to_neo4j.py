"""올리브영 리뷰 통계를 Product 노드에 적재 (부연 근거 + 정렬 신호).

S3 silver 제품 CSV(product_id, rating, review_count, review_stats)를 읽어
기존 Product 노드에 리뷰 속성을 증분 SET한다. 전체 재빌드가 아니라 속성만 얹음
(idempotent). 논문 근거(AFFECTS/CAUTION)는 그대로 두고 **리뷰는 부연/정렬용**.

- rating:float, review_count:int  → 정렬 타이브레이커 + 표시
- review_stats:string(JSON)        → 응답 부연("자극없이 순해요 75%" 등)

review_stats는 CSV에 파이썬 dict repr(작은따옴표)이라 ast로 파싱 후 JSON 문자열로 정규화.

사용:
    NEO4J_URI=... NEO4J_USER=neo4j NEO4J_PASSWORD=... \
    python scripts/load_product_reviews_to_neo4j.py \
        [--s3-key data_csv/oliveyoung_silver_current_...csv] [--dry-run]
"""

import argparse
import ast
import csv
import io
import json
import os

import boto3
from neo4j import GraphDatabase

BUCKET = "oliveyoung-crawl-data"


def resolve_latest_key(s3) -> str:
    """data_csv/의 silver_current 스냅샷 중 가장 최근(LastModified) 것을 자동 선택.
    특정 날짜를 하드코딩하면 오래된 데이터를 쓰는 실수가 생겨서, 항상 최신을 집는다."""
    objs = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data_csv/"):
        for o in page.get("Contents", []):
            k = o["Key"]
            if "silver_current" in k and "error" not in k and k.endswith(".csv"):
                objs.append(o)
    if not objs:
        raise RuntimeError("data_csv/에서 silver_current CSV를 찾지 못했습니다.")
    latest = max(objs, key=lambda o: o["LastModified"])
    print(f"[INFO] 최신 스냅샷 자동선택: {latest['Key'].split('/')[-1]} ({latest['LastModified'].date()})")
    return latest["Key"]

SET_CYPHER = """
UNWIND $rows AS row
MATCH (p:Product {product_id: row.product_id})
SET p.rating       = row.rating,
    p.review_count = row.review_count,
    p.review_stats = row.review_stats
RETURN count(p) AS updated
"""


def _clean_stats(raw: str | None) -> str | None:
    """CSV의 dict repr → 정규화된 JSON 문자열. null 값은 제거해 슬림하게."""
    if not raw or not raw.strip():
        return None
    try:
        d = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return None
    if not isinstance(d, dict):
        return None
    slim = {k: v for k, v in d.items() if v}  # null/빈 축(발색력 등) 제거
    return json.dumps(slim, ensure_ascii=False) if slim else None


def load_rows(text: str) -> list[dict]:
    rows = []
    for r in csv.DictReader(io.StringIO(text)):
        pid = (r.get("product_id") or "").strip()
        rating_raw = (r.get("rating") or "").strip()
        if not pid or not rating_raw:
            continue
        try:
            rating = float(rating_raw)
        except ValueError:
            continue
        try:
            rc = int(float((r.get("review_count") or "0").strip() or 0))
        except ValueError:
            rc = 0
        rows.append({
            "product_id": pid,
            "rating": rating,
            "review_count": rc,
            "review_stats": _clean_stats(r.get("review_stats")),
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-key", default=None, help="미지정 시 최신 silver_current 자동선택")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    s3 = boto3.client("s3")
    key = args.s3_key or resolve_latest_key(s3)
    text = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8-sig", errors="replace")
    rows = load_rows(text)
    print(f"CSV 리뷰 행(rating 유효): {len(rows)}")
    print(f"  샘플: {rows[0]['product_id']} rating={rows[0]['rating']} count={rows[0]['review_count']}")
    print(f"        stats={rows[0]['review_stats'][:90] if rows[0]['review_stats'] else None}...")

    driver = GraphDatabase.driver(os.environ["NEO4J_URI"],
                                  auth=(os.environ.get("NEO4J_USER", "neo4j"), os.environ["NEO4J_PASSWORD"]))
    with driver.session() as s:
        matched = s.run("UNWIND $ids AS i MATCH (p:Product {product_id:i}) RETURN count(DISTINCT i) AS n",
                        ids=[r["product_id"] for r in rows]).single()["n"]
        print(f"그래프 조인 가능: {matched}/{len(rows)}")
        if args.dry_run:
            print("dry-run: 적재 안 함")
            driver.close()
            return
        # 배치 SET
        updated = 0
        for i in range(0, len(rows), 500):
            updated += s.run(SET_CYPHER, rows=rows[i:i + 500]).single()["updated"]
        total = s.run("MATCH (p:Product) WHERE p.rating IS NOT NULL RETURN count(p) AS n").single()["n"]
        print(f"SET 완료: {updated}건 처리 → rating 보유 Product 총 {total}개")
    driver.close()


if __name__ == "__main__":
    main()
