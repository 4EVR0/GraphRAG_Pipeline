import unittest

from pipeline.silver.paper.run_silver import build_silver_rows


class BuildSilverRowsTest(unittest.TestCase):
    def test_builds_sentence_chunks_with_source_offsets(self) -> None:
        papers = [
            {
                "pmid": "123",
                "title": "Example",
                "abstract_text": "First sentence. Second sentence.",
                "journal": "Journal",
                "publication_year": 2026,
                "source_url": "https://pubmed.ncbi.nlm.nih.gov/123/",
            }
        ]

        paper_rows, chunk_rows = build_silver_rows(papers, "full_v5")

        self.assertEqual(1, len(paper_rows))
        self.assertEqual(["First sentence.", "Second sentence."], [
            row["chunk_text"] for row in chunk_rows
        ])
        self.assertEqual([(0, 15), (16, 32)], [
            (row["source_start_offset"], row["source_end_offset"])
            for row in chunk_rows
        ])
        self.assertTrue(all(row["batch_id"] == "full_v5" for row in chunk_rows))


if __name__ == "__main__":
    unittest.main()
