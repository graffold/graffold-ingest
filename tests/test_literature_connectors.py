"""Tests for literature connectors (PubMed, Europe PMC) and section parser."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from graffold_ingest.connectors import CONNECTORS
from graffold_ingest.connectors.pubmed import PubMedConnector, _parse_pubmed_xml
from graffold_ingest.connectors.europepmc import EuropePMCConnector
from graffold_ingest.pipeline.section_parser import parse_sections, remove_sections


SAMPLE_PUBMED_XML = """<?xml version="1.0"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>12345678</PMID>
      <Article>
        <ArticleTitle>F18 ETEC adhesion in piglets</ArticleTitle>
        <Abstract>
          <AbstractText Label="BACKGROUND">ETEC causes diarrhea.</AbstractText>
          <AbstractText Label="RESULTS">FedF mediates attachment.</AbstractText>
        </Abstract>
        <Journal><Title>Vet Microbiol</Title></Journal>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"""


class TestPubMedConnector:
    def test_registered(self):
        assert "pubmed" in CONNECTORS

    def test_name(self):
        assert PubMedConnector().name() == "pubmed"

    def test_parse_xml(self):
        docs = _parse_pubmed_xml(SAMPLE_PUBMED_XML)
        assert len(docs) == 1
        assert docs[0].metadata["pmid"] == "12345678"
        assert "F18 ETEC" in docs[0].title
        assert "FedF" in docs[0].content
        assert "BACKGROUND" in docs[0].content

    def test_parse_empty_xml(self):
        assert _parse_pubmed_xml("<empty/>") == []

    def test_parse_malformed_xml(self):
        assert _parse_pubmed_xml("not xml {{{") == []

    @pytest.mark.asyncio
    async def test_fetch_no_query_no_pmids(self):
        c = PubMedConnector()
        docs = await c.fetch()
        assert docs == []


class TestEuropePMCConnector:
    def test_registered(self):
        assert "europepmc" in CONNECTORS

    def test_name(self):
        assert EuropePMCConnector().name() == "europepmc"

    @pytest.mark.asyncio
    async def test_fetch_empty_query(self):
        c = EuropePMCConnector()
        docs = await c.fetch(query="")
        assert docs == []


class TestSectionParser:
    def test_parses_standard_sections(self):
        text = "Abstract\nWe studied ETEC.\n\nMethods\nIPEC-1 cells.\n\nResults\nFedF binds."
        sections = parse_sections(text)
        labels = [s.label for s in sections]
        assert "Abstract" in labels
        assert "Methods" in labels
        assert "Results" in labels

    def test_no_headings_returns_body(self):
        sections = parse_sections("Just plain text with no headings here.")
        assert len(sections) == 1
        assert sections[0].label == "Body"

    def test_remove_references(self):
        text = "Results\nFedF binds F18.\n\nReferences\n1. Yu et al 2017."
        cleaned = remove_sections(text, ["References"])
        assert "FedF binds" in cleaned
        assert "Yu et al" not in cleaned

    def test_normalizes_method_variants(self):
        text = "Materials and Methods\nWe used IPEC-1 cells."
        sections = parse_sections(text)
        assert any("Method" in s.label for s in sections)
