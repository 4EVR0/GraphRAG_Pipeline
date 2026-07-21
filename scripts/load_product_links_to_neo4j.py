"""올리브영 상품 링크 적재 + goodsNo 교정.

S3 silver 제품 CSV의 product_url(올리브영 상세페이지)을 Product 노드에 SET하고,
URL의 goodsNo를 **권위값**으로 삼아 노드의 goods_no도 이 값으로 통일한다.
→ 링크·이미지(goods_no로 조립)·상품페이지가 서로 일관됨.

배경: 그래프의 기존 goods_no가 URL의 goodsNo와 일부(~11%) 어긋나, 이미지가 링크와
다른 변형 제품을 가리킬 수 있었음. product_url의 goodsNo가 실제 크롤 canonical이라 이를 채택.

- product_url:string  → "올리브영에서 보기" 링크
- goods_no:string     → URL goodsNo로 덮어씀(이미지 조립 키와 링크 일관화)

사용:
    NEO4J_URI=... NEO4J_USER=neo4j NEO4J_PASSWORD=... \
    python scripts/load_product_links_to_neo4j.py [--s3-key ...] [--dry-run]
"""

import argparse
import csv
import io
import os
import re

import boto3
from neo4j import GraphDatabase

BUCKET = "oliveyoung-crawl-data"
DEFAULT_KEY = "data_csv/oliveyoung_silver_current_oliveyoung_silver_20260606_093114.csv"
_GOODS_RE = re.compile(r"goodsNo=([A-Za-z0-9]+)")

SET_CYPHER = """
UNWIND $rows AS row
MATCH (p:Product {product_id: row.product_id})
SET p.product_url = row.product_url,
    p.goods_no    = row.goods_no
RETURN count(p) AS updated
"""


def load_rows(text: str) -> list[dict]:
    rows = []
    for r in csv.DictReader(io.StringIO(text)):
        pid = (r.get("product_id") or "").strip()
        url = (r.get("product_url") or "").strip()
        if not pid or not url:
            continue
        m = _GOODS_RE.search(url)
        if not m:
            continue
        rows.append({"product_id": pid, "product_url": url, "goods_no": m.group(1)})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-key", default=DEFAULT_KEY)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    text = boto3.client("s3").get_object(Bucket=BUCKET, Key=args.s3_key)["Body"].read().decode("utf-8-sig", errors="replace")
    rows = load_rows(text)
    print(f"CSV 링크 행(product_url+goodsNo 유효): {len(rows)}")
    print(f"  샘플: {rows[0]['product_id'][:8]}... goods_no={rows[0]['goods_no']} url={rows[0]['product_url'][:70]}")

    driver = GraphDatabase.driver(os.environ["NEO4J_URI"],
                                  auth=(os.environ.get("NEO4J_USER", "neo4j"), os.environ["NEO4J_PASSWORD"]))
    with driver.session() as s:
        # 교정 규모: 기존 goods_no와 다른 것 몇 개인지
        diff = s.run("""
            UNWIND $rows AS row
            MATCH (p:Product {product_id: row.product_id})
            WHERE p.goods_no <> row.goods_no
            RETURN count(p) AS n
        """, rows=rows).single()["n"]
        matched = s.run("UNWIND $ids AS i MATCH (p:Product {product_id:i}) RETURN count(DISTINCT i) AS n",
                        ids=[r["product_id"] for r in rows]).single()["n"]
        print(f"그래프 조인: {matched}/{len(rows)}, 그중 goods_no 교정 대상: {diff}")
        if args.dry_run:
            print("dry-run: 적재 안 함")
            driver.close()
            return
        updated = 0
        for i in range(0, len(rows), 500):
            updated += s.run(SET_CYPHER, rows=rows[i:i + 500]).single()["updated"]
        total = s.run("MATCH (p:Product) WHERE p.product_url IS NOT NULL RETURN count(p) AS n").single()["n"]
        print(f"SET 완료: {updated}건 처리 → product_url 보유 Product 총 {total}개 (goods_no {diff}개 교정)")
    driver.close()


if __name__ == "__main__":
    main()
