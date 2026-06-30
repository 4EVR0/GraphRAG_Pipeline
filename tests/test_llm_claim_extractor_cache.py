import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from pipeline.claim.services.llm_claim_extractor import LLMClaimExtractor


class LLMClaimExtractorCacheTest(unittest.TestCase):
    def test_reuses_cached_response(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            extractor = LLMClaimExtractor(
                model="test-model",
                api_key="test-key",
                cache_path=str(Path(temp_dir) / "cache.sqlite3"),
            )
            response = SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(content='{"claims": []}')
                    )
                ]
            )
            calls: list[dict] = []

            def create(**kwargs):
                calls.append(kwargs)
                return response

            extractor.client.chat.completions.create = create

            first = extractor._call_llm("Niacinamide improved skin.", ["Niacinamide"])
            second = extractor._call_llm("Niacinamide improved skin.", ["Niacinamide"])

            self.assertEqual('{"claims": []}', first)
            self.assertEqual(first, second)
            self.assertEqual(1, len(calls))


if __name__ == "__main__":
    unittest.main()
