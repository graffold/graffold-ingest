"""Tests for the public SDK surface (graffold_ingest top-level API)."""

import graffold_ingest as g


class TestPublicAPI:
    def test_version(self):
        assert g.__version__

    def test_all_exports_importable(self):
        # Every name in __all__ must resolve (lazy import) without error
        for name in g.__all__:
            if name == "__version__":
                continue
            obj = getattr(g, name)
            assert obj is not None

    def test_core_types_present(self):
        assert g.Document.__name__ == "Document"
        assert g.ExtractionResult.__name__ == "ExtractionResult"

    def test_verbs_present(self):
        for verb in (
            "chunk_documents", "chunk_tabular", "extract_entities",
            "harmonize_graph", "publish_to_parquet", "read_parquet_graph",
            "query_graph", "detect_communities", "get_backend",
        ):
            assert callable(getattr(g, verb))

    def test_connectors_present(self):
        assert g.PubMedConnector().name() == "pubmed"
        assert g.EuropePMCConnector().name() == "europepmc"

    def test_privates_not_exposed(self):
        for private in ("_call_anthropic", "_call_bedrock_llama", "_call_llm", "EXTRACTION_PROMPT"):
            assert not hasattr(g, private), f"{private} leaked into public API"

    def test_dir_is_clean(self):
        d = dir(g)
        assert "Document" in d
        assert "_call_anthropic" not in d
        assert all(not name.startswith("_") or name == "__version__" for name in d)

    def test_unknown_attr_raises(self):
        import pytest

        with pytest.raises(AttributeError):
            g.does_not_exist

    def test_end_to_end_types(self):
        # A minimal SDK flow using only public symbols (no network/LLM)
        doc = g.Document(id="1", content="TP53 inhibits MDM2.", source_type="text")
        chunks = g.chunk_documents([doc], chunk_size=100)
        assert len(chunks) == 1
        assert chunks[0].content == "TP53 inhibits MDM2."
