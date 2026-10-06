import json
import unittest
from types import SimpleNamespace

from pipeline.review.batch import ReviewItem
from pipeline.review.screen import SCREEN_SCHEMA, screen_cost_usd, screen_one

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

    def test_unparseable_or_refused_answer_keeps_the_paper(self) -> None:
        for client, status in ((FakeOpenAI("{oops"), "invalid_json"), (FakeOpenAI(None, refusal="no"), "refusal")):
            row = screen_one(client, ITEM, "gpt-4o-mini")
            self.assertTrue(row["keep"])
            self.assertEqual(row["status"], status)

    def test_cost(self) -> None:
        self.assertAlmostEqual(screen_cost_usd("gpt-4o-mini", {"input_tokens": 1_000_000, "output_tokens": 1_000_000}), 0.75)


if __name__ == "__main__":
    unittest.main()
