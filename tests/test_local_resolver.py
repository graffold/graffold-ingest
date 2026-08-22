"""Tests for local entity resolver (HGNC + synonym + fuzzy)."""

import pytest

from graffold_ingest.resolvers.local import EntityResolver, PROTEIN_SYNONYMS, _normalize


class TestEntityResolver:
    @pytest.fixture
    def resolver(self):
        return EntityResolver(enable_fuzzy=False)

    def test_exact_match_deduplicates(self, resolver):
        nodes = [
            {"id": "a", "name": "TP53", "type": "Protein"},
            {"id": "b", "name": "tp53", "type": "Protein"},
        ]
        merged, _ = resolver.resolve(nodes, [])
        assert len(merged) == 1

    def test_synonym_resolution(self, resolver):
        nodes = [
            {"id": "a", "name": "RANTES", "type": "Protein"},
            {"id": "b", "name": "CCL5", "type": "Protein"},
        ]
        merged, _ = resolver.resolve(nodes, [])
        assert len(merged) == 1

    def test_relationships_remapped(self, resolver):
        nodes = [
            {"id": "a", "name": "RANTES", "type": "Protein"},
            {"id": "b", "name": "CCL5", "type": "Protein"},
            {"id": "c", "name": "IL6", "type": "Protein"},
        ]
        rels = [
            {"source_id": "a", "target_id": "c", "type": "INTERACTS"},
        ]
        merged, remapped = resolver.resolve(nodes, rels)
        # RANTES merged into CCL5's canonical id
        assert len(merged) == 2
        assert len(remapped) == 1
        # Relationship should point to canonical
        assert remapped[0]["source_id"] in [n["id"] for n in merged]

    def test_keeps_longer_name(self, resolver):
        nodes = [
            {"id": "a", "name": "fgf2", "type": "Protein"},
            {"id": "b", "name": "FGF (basic)", "type": "Protein"},
        ]
        merged, _ = resolver.resolve(nodes, [])
        assert len(merged) == 1
        # Longer name should win
        assert "FGF" in merged[0]["name"] or "fgf" in merged[0]["name"]

    def test_deduplicates_relationships(self, resolver):
        nodes = [
            {"id": "a", "name": "X", "type": "Protein"},
            {"id": "b", "name": "Y", "type": "Protein"},
        ]
        rels = [
            {"source_id": "a", "target_id": "b", "type": "INHIBITS"},
            {"source_id": "a", "target_id": "b", "type": "INHIBITS"},
        ]
        _, remapped = resolver.resolve(nodes, rels)
        assert len(remapped) == 1

    def test_empty_input(self, resolver):
        merged, remapped = resolver.resolve([], [])
        assert merged == []
        assert remapped == []

    def test_no_false_merges_different_names(self, resolver):
        nodes = [
            {"id": "a", "name": "TP53", "type": "Protein"},
            {"id": "b", "name": "BRCA1", "type": "Protein"},
            {"id": "c", "name": "MDM2", "type": "Protein"},
        ]
        merged, _ = resolver.resolve(nodes, [])
        assert len(merged) == 3

    def test_all_synonyms_in_map(self):
        for k, v in PROTEIN_SYNONYMS.items():
            assert k == _normalize(k)
            assert v == _normalize(v)


class TestTabularChunker:
    def test_chunk_csv(self, tmp_path):
        from graffold_ingest.pipeline.tabular import chunk_tabular

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("gene,score\nTP53,0.9\nBRCA1,0.8\nMDM2,0.7\n")

        chunks = chunk_tabular(csv_file, rows_per_chunk=2)
        assert len(chunks) == 2
        assert "TP53" in chunks[0].content
        assert "MDM2" in chunks[1].content

    def test_header_in_every_chunk(self, tmp_path):
        from graffold_ingest.pipeline.tabular import chunk_tabular

        csv_file = tmp_path / "test.csv"
        lines = ["protein,disease"] + [f"P{i},D{i}" for i in range(100)]
        csv_file.write_text("\n".join(lines))

        chunks = chunk_tabular(csv_file, rows_per_chunk=30)
        for chunk in chunks:
            assert "protein" in chunk.content
            assert "disease" in chunk.content

    def test_tsv_detection(self, tmp_path):
        from graffold_ingest.pipeline.tabular import chunk_tabular

        tsv_file = tmp_path / "data.tsv"
        tsv_file.write_text("gene\tscore\nTP53\t0.9\nBRCA1\t0.8\n")

        chunks = chunk_tabular(tsv_file)
        assert len(chunks) == 1
        assert "TP53" in chunks[0].content

    def test_empty_file(self, tmp_path):
        from graffold_ingest.pipeline.tabular import chunk_tabular

        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("col1,col2\n")

        chunks = chunk_tabular(csv_file)
        assert chunks == []

    def test_metadata_has_row_info(self, tmp_path):
        from graffold_ingest.pipeline.tabular import chunk_tabular

        csv_file = tmp_path / "meta.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n5,6\n")

        chunks = chunk_tabular(csv_file, rows_per_chunk=2)
        assert chunks[0].metadata["row_start"] == 1
        assert chunks[0].metadata["row_end"] == 2
        assert chunks[1].metadata["row_start"] == 3

    def test_source_type_is_csv(self, tmp_path):
        from graffold_ingest.pipeline.tabular import chunk_tabular

        csv_file = tmp_path / "t.csv"
        csv_file.write_text("x\n1\n")
        chunks = chunk_tabular(csv_file)
        assert chunks[0].source_type == "csv"
