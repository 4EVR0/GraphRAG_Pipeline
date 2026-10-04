import json
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from pipeline.review.agreement import (
    agreement,
    paper_labels,
    read_human_sheet,
    sample_for_human_review,
    write_human_sheet,
)
from pipeline.review.batch import (
    ReviewItem,
    build_requests,
    collect,
    cost_usd,
    custom_id,
    review_key,
)
from pipeline.review.schema import ACNE_EFFECTS, OUTPUT_SCHEMA, prompt_sha
from pipeline.review.validate import judge, normalize, quote_in_source

ITEM = ReviewItem(
    pmid="111",
    ingredient="SALICYLIC ACID",
    title="2% salicylic acid gel for acne",
    source_text="RESULTS: Salicylic acid 2% gel reduced total lesion count by 40% (p<0.01).\nCONCLUSIONS: It was well tolerated.",
)


def _judgment(**overrides) -> dict:
    base = {
        "effect_code": "BLEMISH_CARE", "attribution": "single", "route": "topical_leave_on",
        "concentration": "2%", "population": "40 adults with acne", "study_type": "rct",
        "direction": "improves",
        "evidence_quote": "Salicylic acid 2% gel reduced total lesion count by 40% (p<0.01).",
    }
    return {**base, **overrides}


def _result(payload: dict | None = None, **overrides) -> dict:
    payload = payload if payload is not None else {
        "relevant": True, "needs_fulltext": False, "reason": "RCT", "judgments": [_judgment()]}
    return {"result_type": "succeeded", "stop_reason": "end_turn", "text": json.dumps(payload),
            "usage": {"input_tokens": 1000, "output_tokens": 500}, **overrides}


class RequestTest(unittest.TestCase):
    def test_custom_id_is_api_safe(self) -> None:
        item = ReviewItem("123", "ACETYL HEXAPEPTIDE-8 / (TEST)", "t", "s")
        self.assertRegex(custom_id(item, prompt_sha()), r"^[a-zA-Z0-9_-]{1,64}$")

    def test_already_reviewed_is_skipped(self) -> None:
        other = ReviewItem("222", "SALICYLIC ACID", "t", "s")
        done = {review_key("111", "salicylic acid", "claude-opus-5-5", prompt_sha())}
        requests, skipped = build_requests([ITEM, other], "claude-opus-5-5", "high", done)
        self.assertEqual([r["custom_id"].split("_")[0] for r in requests], ["222"])
        self.assertEqual(skipped, [ITEM])

    def test_other_model_or_prompt_is_not_skipped(self) -> None:
        done = {review_key("111", "SALICYLIC ACID", "claude-fable-5-1", prompt_sha()),
                review_key("111", "SALICYLIC ACID", "claude-opus-5-5", "old")}
        requests, _ = build_requests([ITEM], "claude-opus-5-5", None, done)
        self.assertEqual(len(requests), 1)

    def test_params_use_structured_output_and_effort(self) -> None:
        (req,), _ = build_requests([ITEM], "claude-opus-5-5", "high")
        params = req["params"]
        self.assertEqual(params["output_config"]["effort"], "high")
        self.assertEqual(params["output_config"]["format"], {"type": "json_schema", "schema": OUTPUT_SCHEMA})
        self.assertNotIn("thinking", params)
        self.assertIn("Target ingredient: SALICYLIC ACID", params["messages"][0]["content"])

    def test_haiku_gets_no_effort(self) -> None:
        (req,), _ = build_requests([ITEM], "claude-haiku-4-5", "high")
        self.assertNotIn("effort", req["params"]["output_config"])

    def test_schema_objects_disallow_extra_properties(self) -> None:
        self.assertFalse(OUTPUT_SCHEMA["additionalProperties"])
        self.assertFalse(OUTPUT_SCHEMA["properties"]["judgments"]["items"]["additionalProperties"])


class CollectTest(unittest.TestCase):
    def test_results_are_keyed_with_usage_and_refusal(self) -> None:
        def message(stop_reason, category=None):
            return SimpleNamespace(
                model="claude-opus-5-5", stop_reason=stop_reason,
                stop_details=SimpleNamespace(category=category) if category else None,
                content=[SimpleNamespace(type="thinking"), SimpleNamespace(type="text", text="{}")],
                usage=SimpleNamespace(input_tokens=10, output_tokens=20,
                                      cache_creation_input_tokens=None, cache_read_input_tokens=0),
            )

        results = [
            SimpleNamespace(custom_id="b", result=SimpleNamespace(type="succeeded", message=message("refusal", "bio"))),
            SimpleNamespace(custom_id="a", result=SimpleNamespace(type="succeeded", message=message("end_turn"))),
            SimpleNamespace(custom_id="c", result=SimpleNamespace(type="errored", error="overloaded")),
        ]
        client = SimpleNamespace(messages=SimpleNamespace(batches=SimpleNamespace(results=lambda _id: iter(results))))
        rows = {r["custom_id"]: r for r in collect(client, "batch_1")}
        self.assertEqual(rows["a"]["text"], "{}")
        self.assertEqual(rows["a"]["usage"]["output_tokens"], 20)
        self.assertEqual(rows["a"]["usage"]["cache_creation_input_tokens"], 0)
        self.assertEqual(rows["b"]["refusal_category"], "bio")
        self.assertEqual(rows["c"]["result_type"], "errored")

    def test_batch_cost_is_half_price(self) -> None:
        usage = {"input_tokens": 1_000_000, "output_tokens": 1_000_000}
        self.assertAlmostEqual(cost_usd("claude-opus-5-5", usage), 12.0)
        self.assertAlmostEqual(cost_usd("claude-opus-5-5", usage, batch=False), 24.0)


class ValidateTest(unittest.TestCase):
    def test_quote_matching_tolerates_whitespace_and_typographic_marks(self) -> None:
        self.assertTrue(quote_in_source("reduced  total lesion\ncount", "", ITEM.source_text))
        self.assertTrue(quote_in_source("“quoted” – text", "", 'He said "quoted" - text'))
        self.assertFalse(quote_in_source("reduced lesions by half", ITEM.title, ITEM.source_text))
        self.assertFalse(quote_in_source("", ITEM.title, ITEM.source_text))
        self.assertEqual(normalize(" a  b "), "a b")

    def test_valid_judgment_becomes_record(self) -> None:
        records, queue, summary = judge(ITEM, _result(), "claude-opus-5-5", "sha", "2026-10-04T00:00:00Z")
        self.assertEqual(queue, [])
        self.assertEqual(summary["status"], "ok")
        (record,) = records
        self.assertTrue(record["quote_verified"])
        self.assertEqual(record["source"], "abstract")
        self.assertIsNone(record["human_verdict"])
        self.assertEqual(record["prompt_sha"], "sha")

    def test_fabricated_quote_and_bad_value_go_to_human_queue(self) -> None:
        payload = {"relevant": True, "needs_fulltext": False, "reason": "",
                   "judgments": [_judgment(evidence_quote="SA cured acne."), _judgment(route="injection")]}
        records, queue, summary = judge(ITEM, _result(payload), "m", "sha")
        self.assertEqual(len(records), 2)
        self.assertEqual(sorted(q["reason"] for q in queue), ["invalid_value", "quote_not_in_source"])
        self.assertEqual((summary["quote_failures"], summary["invalid_values"]), (1, 1))

    def test_refusal_truncation_and_bad_json_go_to_human_queue(self) -> None:
        cases = {
            "refusal": _result(stop_reason="refusal", refusal_category="bio"),
            "max_tokens": _result(stop_reason="max_tokens"),
            "invalid_json": _result(text="{not json"),
            "batch_errored": {"result_type": "errored", "error": "x"},
        }
        for reason, result in cases.items():
            records, queue, summary = judge(ITEM, result, "m", "sha")
            self.assertEqual(records, [], reason)
            self.assertEqual(queue[0]["reason"], reason)
            self.assertEqual(summary["status"], reason)

    def test_needs_fulltext_is_queued(self) -> None:
        payload = {"relevant": True, "needs_fulltext": True, "reason": "abstract too short", "judgments": []}
        _, queue, _ = judge(ITEM, _result(payload), "m", "sha")
        self.assertEqual(queue[0]["reason"], "needs_fulltext")


class AgreementTest(unittest.TestCase):
    def test_paper_labels_majority_and_acne_direction(self) -> None:
        records = [
            _judgment(effect_code="BLEMISH_CARE", direction="improves"),
            _judgment(effect_code="COMEDOLYTIC", direction="improves", attribution="combination"),
            _judgment(effect_code="HYDRATING", direction="no_difference", attribution="combination"),
        ]
        labels = paper_labels({"relevant": True}, records, ACNE_EFFECTS)
        self.assertEqual(labels["attribution"], "combination")
        self.assertEqual(labels["direction"], "improves")
        self.assertEqual(labels["effect_codes"], "BLEMISH_CARE|COMEDOLYTIC|HYDRATING")
        self.assertEqual(paper_labels({"relevant": False}, [], ACNE_EFFECTS)["direction"], "none")

    def test_agreement_rates_skip_blank_and_support_prefix(self) -> None:
        labels = {
            "1": {"relevant": "yes", "attribution": "single", "route": "topical_leave_on",
                  "direction": "improves", "effect_codes": "BLEMISH_CARE"},
            "2": {"relevant": "yes", "attribution": "combination", "route": "peel",
                  "direction": "improves", "effect_codes": "BLEMISH_CARE|SEBUM_REGULATION"},
        }
        human = [
            {"pmid": "1", "human_relevant": "yes", "human_attribution": "single", "human_route": "topical*",
             "human_direction": "improves", "human_effect_codes": "BLEMISH_CARE"},
            {"pmid": "2", "human_relevant": "yes", "human_attribution": "single", "human_route": "",
             "human_direction": "Improves", "human_effect_codes": "BLEMISH_CARE"},
            {"pmid": "3", "human_relevant": "no", "human_attribution": "single"},
        ]
        report = agreement(human, labels)
        self.assertEqual(report["attribution"]["rate"], 0.5)
        self.assertEqual(report["attribution"]["mismatches"], [{"human": "single", "model": "combination"}])
        self.assertEqual(report["route"], {"matched": 1, "n": 1, "rate": 1.0, "mismatches": []})
        self.assertEqual(report["direction"]["rate"], 1.0)
        self.assertEqual(report["effect_codes_jaccard"], {"n": 2, "mean": 0.75})

    def test_sample_is_stratified_and_deterministic(self) -> None:
        papers = [{"pmid": str(i), "title": "", "source_text": "", "stratum": "true" if i < 40 else "false"}
                  for i in range(100)]
        first = sample_for_human_review(papers, 30, 49, "stratum")
        self.assertEqual(first, sample_for_human_review(papers, 30, 49, "stratum"))
        self.assertEqual(len(first), 30)
        self.assertEqual(sum(p["stratum"] == "true" for p in first), 12)

    def test_human_sheet_is_blind(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sheet.csv"
            write_human_sheet(path, [{"pmid": "1", "title": "T", "source_text": "A"}], "SALICYLIC ACID")
            (row,) = read_human_sheet(path)
        self.assertEqual(row["human_attribution"], "")
        self.assertFalse(any(re.match(r"model|judgment", k) for k in row))


if __name__ == "__main__":
    unittest.main()
