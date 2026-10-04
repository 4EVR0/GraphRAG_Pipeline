import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests

from pipeline.bronze.pubmed.collect_narrow import collect_narrow, load_existing_pmids
from pipeline.metadata.services.narrow_query import NARROW_FILTER, PairPlan
from pipeline.metadata.services.pubmed_client import PubMedClient


def _xml(pmids_with_abstract: dict[str, bool]) -> str:
    articles = []
    for pmid, has_abstract in pmids_with_abstract.items():
        abstract = "<Abstract><AbstractText>Result text.</AbstractText></Abstract>" if has_abstract else ""
        articles.append(
            f"<PubmedArticle><MedlineCitation><PMID>{pmid}</PMID><Article>"
            f"<ArticleTitle>T{pmid}</ArticleTitle>{abstract}</Article></MedlineCitation></PubmedArticle>"
        )
    return f"<PubmedArticleSet>{''.join(articles)}</PubmedArticleSet>"


class FakeClient:
    """query → (전체 건수, PMID 목록). retmax만큼 잘라서 돌려준다."""

    def __init__(self, results: dict[str, tuple[int, list[str]]], no_abstract: set[str] = frozenset()):
        self.results = results
        self.no_abstract = no_abstract
        self.searches: list[tuple[str, int]] = []
        self.fetched: list[list[str]] = []

    def search(self, query: str, retmax: int, retstart: int = 0):
        self.searches.append((query, retmax))
        count, ids = self.results[query]
        return count, ids[:retmax]

    def fetch_pubmed_xml(self, pmids: list[str]) -> str:
        self.fetched.append(list(pmids))
        return _xml({p: p not in self.no_abstract for p in pmids})


def _narrow(q: str) -> str:
    return f"{q} AND {NARROW_FILTER}"


class CountRuleTest(unittest.TestCase):
    def test_small_pair_takes_all_without_narrowing(self) -> None:
        client = FakeClient({"Q1": (3, ["1", "2", "3"])})
        result = collect_narrow(client, [PairPlan("A", "E", "Q1")], full_fetch_max=3, pair_cap=10)
        row = result.search_rows[0]
        self.assertEqual((row["count"], row["narrowed"], row["taken"], row["capped"]), (3, False, 3, False))
        self.assertEqual(client.searches, [("Q1", 3)])

    def test_large_pair_is_narrowed_and_taken_in_full(self) -> None:
        client = FakeClient({"Q1": (4, ["1", "2", "3", "4"]), _narrow("Q1"): (2, ["2", "4"])})
        result = collect_narrow(client, [PairPlan("A", "E", "Q1")], full_fetch_max=3, pair_cap=10)
        row = result.search_rows[0]
        self.assertEqual((row["count"], row["narrowed"], row["narrowed_count"], row["taken"]), (4, True, 2, 2))
        self.assertFalse(row["capped"])
        self.assertEqual(result.pmids, {"2", "4"})

    def test_narrowed_over_cap_is_logged_as_capped(self) -> None:
        client = FakeClient({"Q1": (9, list("123456789")), _narrow("Q1"): (5, list("12345"))})
        with self.assertLogs("pipeline.bronze.pubmed.collect_narrow", level="WARNING") as logs:
            result = collect_narrow(client, [PairPlan("A", "E", "Q1")], full_fetch_max=3, pair_cap=4)
        row = result.search_rows[0]
        self.assertTrue(row["capped"])
        self.assertEqual(row["taken"], 4)
        self.assertIn("상한 도달", logs.output[0])

    def test_skipped_plan_does_not_search(self) -> None:
        client = FakeClient({})
        result = collect_narrow(
            client, [PairPlan("ALCOHOL", "E", None, skip_reason="excluded_ingredient")], 3, 10
        )
        self.assertEqual(client.searches, [])
        self.assertEqual(result.search_rows[0]["skip_reason"], "excluded_ingredient")
        self.assertEqual(result.search_rows[0]["taken"], 0)


class DedupTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeClient(
            {"Q1": (3, ["1", "2", "3"]), "Q2": (2, ["3", "4"])}, no_abstract={"4"}
        )
        self.plans = [PairPlan("A", "E1", "Q1"), PairPlan("B", "E2", "Q2")]

    def test_pmids_are_deduplicated_and_existing_skipped(self) -> None:
        result = collect_narrow(self.client, self.plans, 10, 10, existing_pmids={"1"})
        self.assertEqual(result.pmids, {"1", "2", "3", "4"})
        self.assertEqual(self.client.fetched, [["2", "3", "4"]])
        self.assertEqual(result.skipped_existing, 1)
        self.assertEqual([r["new_pmids"] for r in result.search_rows], [2, 1])
        papers = {p["pmid"]: p for p in result.paper_rows}
        self.assertEqual(set(papers), {"2", "3"})
        self.assertEqual(papers["3"]["searched_pairs"], "A:E1|B:E2")
        self.assertEqual(papers["3"]["searched_ingredients"], "A|B")
        self.assertEqual(result.fetched_without_abstract, 1)
        self.assertEqual(len(result.pair_rows), 5)
        self.assertTrue(next(r for r in result.pair_rows if r["pmid"] == "1")["already_collected"])

    def test_search_only_does_not_fetch(self) -> None:
        result = collect_narrow(self.client, self.plans, 10, 10, search_only=True)
        self.assertEqual(self.client.fetched, [])
        self.assertEqual(result.paper_rows, [])
        self.assertEqual(len(result.pmids), 4)


class ExistingPmidsTest(unittest.TestCase):
    def test_reads_batch_dirs_json_and_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "batch=x").mkdir()
            (tmp / "batch=x" / "paper.csv").write_text("pmid,title\n11,a\n12,b\n", encoding="utf-8")
            (tmp / "batch=x" / "search_log.csv").write_text("query\nq\n", encoding="utf-8")
            (tmp / "ids.json").write_text(json.dumps([13, "14"]), encoding="utf-8")
            (tmp / "ids.txt").write_text("15\n\n16\n", encoding="utf-8")
            pmids = load_existing_pmids([tmp / "batch=x", tmp / "ids.json", tmp / "ids.txt"])
        self.assertEqual(pmids, {"11", "12", "13", "14", "15", "16"})


class _Response:
    def __init__(self, status: int, payload: dict | None = None) -> None:
        self.status_code = status
        self.payload = payload or {}
        self.text = ""

    def json(self) -> dict:
        return self.payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class ClientRetryTest(unittest.TestCase):
    def _client(self, responses: list) -> tuple[PubMedClient, mock.Mock]:
        session = mock.Mock()
        session.get.side_effect = responses
        return PubMedClient(session=session, sleep=0, max_retries=2), session

    @mock.patch("pipeline.metadata.services.pubmed_client.time.sleep")
    def test_retries_rate_limit_then_succeeds(self, _sleep) -> None:
        ok = _Response(200, {"esearchresult": {"count": "2", "idlist": ["1", "2"]}})
        client, session = self._client([_Response(429), requests.ConnectionError("x"), ok])
        self.assertEqual(client.search("q", 10), (2, ["1", "2"]))
        self.assertEqual(session.get.call_count, 3)
        self.assertEqual(client.request_count, 3)

    @mock.patch("pipeline.metadata.services.pubmed_client.time.sleep")
    def test_gives_up_after_max_retries(self, _sleep) -> None:
        client, _ = self._client([_Response(503)] * 3)
        with self.assertRaises(requests.HTTPError):
            client.search("q", 10)

    @mock.patch("pipeline.metadata.services.pubmed_client.time.sleep")
    def test_client_error_is_not_retried(self, _sleep) -> None:
        client, session = self._client([_Response(400)])
        with self.assertRaises(requests.HTTPError):
            client.search("q", 10)
        self.assertEqual(session.get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
