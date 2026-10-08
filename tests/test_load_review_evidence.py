import tempfile
import unittest
from pathlib import Path

from scripts import load_review_evidence_to_neo4j as loader


class _Record(dict):
    def single(self):
        return self


class _Session:
    """쿼리 문자열로 응답을 고르는 가짜 세션. 실행한 쿼리를 기록한다."""

    def __init__(self, ingredients, concerns):
        self.ingredients, self.concerns, self.queries = set(ingredients), set(concerns), []

    def run(self, query, **params):
        self.queries.append(query)
        if "MATCH (n:Ingredient" in query:
            return _Record(ok=[v for v in params["x"] if v in self.ingredients])
        if "MATCH (n:Concern" in query:
            return _Record(ok=[v for v in params["x"] if v in self.concerns])
        if "RETURN count(r) AS n" in query and "DELETE" not in query and "CREATE" not in query:
            return _Record(n=7)
        return _Record(n=len(params.get("rows", [])) or 3)


class ReadTest(unittest.TestCase):
    def test_reads_only_rows_with_49_properties(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ingredient.csv"
            path.write_text(
                "ingredient_id:ID(Ingredient),inci_name,kor_name,cosing_functions:string[],"
                "evidence_reviewed:boolean,sensitive_caution,sensitive_caution_with:string[]\n"
                "SALICYLIC ACID,SALICYLIC ACID,살리실릭애씨드,,true,exclude,ACNE;COMEDONES\n"
                "PANTHENOL,PANTHENOL,판테놀,,true,,\n"
                "WATER,WATER,정제수,,false,,\n"
                "LINALOOL,LINALOOL,리날룰,,false,exclude,\n", encoding="utf-8")
            rows = loader.read_ingredient_props(path)
        self.assertEqual(["SALICYLIC ACID", "PANTHENOL", "LINALOOL"], [r["inci"] for r in rows])
        self.assertEqual(["ACNE", "COMEDONES"], rows[0]["sensitive_caution_with"])
        self.assertFalse(rows[2]["evidence_reviewed"])

    def test_missing_properties_stop_the_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ingredient.csv"
            path.write_text("ingredient_id:ID(Ingredient),inci_name\nWATER,WATER\n", encoding="utf-8")
            with self.assertRaises(SystemExit):
                loader.read_ingredient_props(path)


class RunTest(unittest.TestCase):
    ING = [{"inci": "SALICYLIC ACID", "evidence_reviewed": True, "sensitive_caution": "exclude",
            "sensitive_caution_with": ["ACNE"]},
           {"inci": "NOT IN GRAPH", "evidence_reviewed": True, "sensitive_caution": "", "sensitive_caution_with": []}]
    EDGES = [{"ingredient": "SALICYLIC ACID", "concern": "ACNE", "evidence_type": "pubmed_review",
              "graph_score": 2.4, "paper_count": 53, "effects": "COMEDOLYTIC", "caution": ""},
             {"ingredient": "SALICYLIC ACID", "concern": "NOT_A_CONCERN", "evidence_type": "pubmed_review",
              "graph_score": 1.0, "paper_count": 1, "effects": "", "caution": ""}]

    def test_dry_run_reports_and_never_writes(self):
        session = _Session({"SALICYLIC ACID"}, {"ACNE"})
        summary = loader.run(session, self.ING, self.EDGES, "dry-run")
        self.assertEqual((1, 1), (summary["props_matched"], summary["edges_joinable"]))
        self.assertEqual(["NOT IN GRAPH"], summary["props_missing"])
        self.assertEqual([("SALICYLIC ACID", "NOT_A_CONCERN")], summary["edges_missing"])
        writes = [q for q in session.queries if any(w in q for w in ("DELETE", "REMOVE", "SET ", "CREATE"))]
        self.assertEqual([], writes)

    def test_apply_clears_then_writes_only_joinable_rows(self):
        session = _Session({"SALICYLIC ACID"}, {"ACNE"})
        summary = loader.run(session, self.ING, self.EDGES, "apply")
        self.assertEqual((1, 1), (summary["set_nodes"], summary["created_edges"]))
        order = [q for q in session.queries if any(w in q for w in ("DELETE", "REMOVE", "CREATE (i)", "SET i.evidence"))]
        self.assertIn("DELETE", order[0])
        self.assertIn("REMOVE", order[1])
        self.assertIn("CREATE (i)", order[-1])

    def test_rollback_touches_only_49_data(self):
        session = _Session(set(), set())
        loader.run(session, [], [], "rollback")
        self.assertEqual(2, len(session.queries))
        self.assertIn("EVIDENCE_FOR", session.queries[0])
        self.assertIn("REMOVE i.evidence_reviewed, i.sensitive_caution, i.sensitive_caution_with", session.queries[1])


if __name__ == "__main__":
    unittest.main()
