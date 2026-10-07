import json
import unittest
from types import SimpleNamespace

from pipeline.review.batch import ReviewItem
from pipeline.review.screen import SCREEN_PROMPT_VERSION, SCREEN_SCHEMA, screen_cost_usd, screen_one

ITEM = ReviewItem("111", "SALICYLIC ACID", "SA gel for acne", "Salicylic acid reduced lesions.")


class FakeOpenAI:
    def __init__(self, content=None, refusal=None):
        self.calls = []
        message = SimpleNamespace(content=content, refusal=refusal)
        self.response = SimpleNamespace(choices=[SimpleNamespace(message=message)],
                                        usage=SimpleNamespace(prompt_tokens=800, completion_tokens=40))
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class ScreenTest(unittest.TestCase):
    def test_keep_and_drop_are_parsed_with_usage(self) -> None:
        client = FakeOpenAI(json.dumps({"keep": False, "reason": "HPLC method"}))
        row = screen_one(client, ITEM, "gpt-4o-mini")
        self.assertEqual((row["keep"], row["reason"], row["status"]), (False, "HPLC method", "ok"))
        self.assertEqual(row["usage"], {"input_tokens": 800, "output_tokens": 40})
        call = client.calls[0]
        self.assertEqual(call["response_format"], {"type": "json_schema", "json_schema": SCREEN_SCHEMA})
        self.assertEqual(call["temperature"], 0.0)
        self.assertIn("Target ingredient: SALICYLIC ACID", call["messages"][1]["content"])

    def test_reasoning_models_get_no_temperature(self) -> None:
        client = FakeOpenAI(json.dumps({"keep": True, "reason": "ok"}))
        screen_one(client, ITEM, "gpt-5-mini")
        self.assertNotIn("temperature", client.calls[0])
        self.assertGreaterEqual(client.calls[0]["max_completion_tokens"], 1000)

    def test_prompt_version_changes_sha_and_prompt(self) -> None:
        client = FakeOpenAI(json.dumps({"keep": True, "reason": "ok"}))
        v1 = screen_one(client, ITEM, "gpt-4o-mini", "evidence-screen-v1")
        v2 = screen_one(client, ITEM, "gpt-4o-mini")
        self.assertNotEqual(v1["prompt_sha"], v2["prompt_sha"])
        self.assertNotEqual(client.calls[0]["messages"][0]["content"], client.calls[1]["messages"][0]["content"])
        self.assertEqual(v2["prompt_version"], SCREEN_PROMPT_VERSION)

    def test_unparseable_or_refused_answer_keeps_the_paper(self) -> None:
        for client, status in ((FakeOpenAI("{oops"), "invalid_json"), (FakeOpenAI(None, refusal="no"), "refusal")):
            row = screen_one(client, ITEM, "gpt-4o-mini")
            self.assertTrue(row["keep"])
            self.assertEqual(row["status"], status)

    def test_cost(self) -> None:
        self.assertAlmostEqual(screen_cost_usd("gpt-4o-mini", {"input_tokens": 1_000_000, "output_tokens": 1_000_000}), 0.75)


if __name__ == "__main__":
    unittest.main()


class BatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.items = [ReviewItem("1", "SALICYLIC ACID", "t1", "a1"), ReviewItem("2", "GLYCERIN", "t2", "a2")]

    def test_batch_file_lines_match_sync_body(self) -> None:
        import tempfile
        from pathlib import Path

        from pipeline.review.screen import screen_request_body, write_batch_file

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "req.jsonl"
            by_id = write_batch_file(path, self.items + [self.items[0]], "gpt-5-mini")
            lines = [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(lines), 2)
        self.assertEqual(set(by_id), {l["custom_id"] for l in lines})
        self.assertEqual(lines[0]["url"], "/v1/chat/completions")
        self.assertEqual(lines[0]["body"], screen_request_body(self.items[0], "gpt-5-mini"))
        self.assertNotIn("temperature", lines[0]["body"])

    def test_parse_output_handles_success_refusal_and_errors(self) -> None:
        from pipeline.review.screen import parse_batch_output, screen_custom_id

        ids = [screen_custom_id(i) for i in self.items]
        by_id = dict(zip(ids, self.items))
        ok = {"custom_id": ids[0], "response": {"status_code": 200, "body": {
            "choices": [{"message": {"content": json.dumps({"keep": False, "reason": "assay"})}}],
            "usage": {"prompt_tokens": 900, "completion_tokens": 300}}}, "error": None}
        refused = {"custom_id": ids[1], "response": {"status_code": 200, "body": {
            "choices": [{"message": {"content": None, "refusal": "no"}}], "usage": {}}}, "error": None}
        failed = {"custom_id": ids[1], "response": {"status_code": 500, "body": {"error": {"message": "x"}}}, "error": None}
        unknown = {"custom_id": "other", "response": {"status_code": 200, "body": {}}}
        rows = parse_batch_output([json.dumps(r) for r in (ok, refused, failed, unknown)] + [""], by_id, "gpt-5-mini")
        self.assertEqual([(r["keep"], r["status"]) for r in rows],
                         [(False, "ok"), (True, "refusal"), (True, "batch_error")])
        self.assertEqual(rows[0]["usage"], {"input_tokens": 900, "output_tokens": 300})
        self.assertAlmostEqual(screen_cost_usd("gpt-5-mini", rows[0]["usage"], batch=True),
                               (900 * 0.25 + 300 * 2.0) / 1e6 / 2)

    def test_submit_uploads_file_and_creates_batch(self) -> None:
        import tempfile
        from pathlib import Path

        from pipeline.review.screen import submit_batch

        calls = {}
        client = SimpleNamespace(
            files=SimpleNamespace(create=lambda **kw: calls.setdefault("file", kw) and SimpleNamespace(id="file_1")),
            batches=SimpleNamespace(create=lambda **kw: calls.setdefault("batch", kw) and SimpleNamespace(id="batch_1")),
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "req.jsonl"
            path.write_text("{}\n", encoding="utf-8")
            self.assertEqual(submit_batch(client, path), "batch_1")
        self.assertEqual(calls["file"]["purpose"], "batch")
        self.assertEqual(calls["batch"], {"input_file_id": "file_1", "endpoint": "/v1/chat/completions",
                                          "completion_window": "24h"})


class PipelineGlueTest(unittest.TestCase):
    def test_sources_from_bronze_and_screened_only(self) -> None:
        import tempfile
        from pathlib import Path

        from pipeline.review import run_review

        with tempfile.TemporaryDirectory() as tmp:
            bronze, out = Path(tmp) / "bronze", Path(tmp) / "out"
            bronze.mkdir()
            (bronze / "paper_raw.csv").write_text(
                "pmid,title,abstract_text\n1,T1,A1\n2,T2,\n", encoding="utf-8-sig")
            (bronze / "pair_pmids.csv").write_text(
                "inci_name,effect_code,pmid\nUREA,HYDRATING,1\nUREA,KERATOLYTIC,1\nGLYCERIN,HYDRATING,1\nUREA,HYDRATING,2\n",
                encoding="utf-8-sig")
            self.assertEqual(run_review.sources_from_bronze(bronze, out), 2)
            self.assertEqual(run_review.sources_from_bronze(bronze, out), 0)
            screen = [
                {"pmid": "1", "ingredient_inci": "UREA", "model": "gpt-5-mini", "prompt_sha": run_review.screen_prompt_sha(),
                 "keep": False, "status": "ok"},
                {"pmid": "1", "ingredient_inci": "UREA", "model": "gpt-5-mini", "prompt_sha": run_review.screen_prompt_sha(),
                 "keep": True, "status": "ok"},
                {"pmid": "1", "ingredient_inci": "GLYCERIN", "model": "gpt-5-mini", "prompt_sha": run_review.screen_prompt_sha(),
                 "keep": True, "status": "batch_error"},
            ]
            run_review.append_jsonl(out / run_review.SCREEN_FILE, screen)
            latest = run_review.latest_screen(out, "gpt-5-mini", run_review.SCREEN_PROMPT_VERSION)
            self.assertTrue(latest[("1", "UREA")]["keep"])
            args = SimpleNamespace(out_dir=out, pmids=None, screened_only=True, screen_model="gpt-5-mini",
                                   screen_version=run_review.SCREEN_PROMPT_VERSION, model="claude-sonnet-5-5",
                                   effort="medium", limit=None, dry_run=True)
            with _Capture() as captured:
                run_review.cmd_submit(args)
        self.assertIn("[submit] 1 requests", captured.text)


class _Capture:
    def __enter__(self):
        import contextlib
        import io

        self._buffer = io.StringIO()
        self._ctx = contextlib.redirect_stdout(self._buffer)
        self._ctx.__enter__()
        return self

    def __exit__(self, *exc):
        self._ctx.__exit__(*exc)
        self.text = self._buffer.getvalue()
        return False
