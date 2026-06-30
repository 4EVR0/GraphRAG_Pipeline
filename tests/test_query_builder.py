import unittest

from pipeline.metadata.services.query_builder import build_pubmed_query


class PubMedQueryBuilderTest(unittest.TestCase):
    def test_required_and_excluded_contexts_are_separate_clauses(self) -> None:
        query = build_pubmed_query(
            query_name="Niacinamide",
            alias_list="Nicotinamide",
            concern_keywords="acne|sebum",
            required_context_keywords="sebum|sebaceous",
            excluded_context_keywords="scalp|hair",
        )

        self.assertIn(
            '("sebum"[Title/Abstract] OR "sebaceous"[Title/Abstract])',
            query,
        )
        self.assertIn(
            'NOT ("scalp"[Title/Abstract] OR "hair"[Title/Abstract])',
            query,
        )


if __name__ == "__main__":
    unittest.main()
