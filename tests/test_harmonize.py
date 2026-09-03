"""Tests for graph harmonization."""

from graffold_ingest.pipeline.harmonize import harmonize_graph, _apply_alias_rules


def _nodes():
    return [
        {"id": "n1", "name": "F18 ETEC", "type": "Organism"},
        {"id": "n2", "name": "F18 fimbriae", "type": "Organism"},
        {"id": "n3", "name": "Enterotoxigenic Escherichia coli", "type": "Organism"},
        {"id": "n4", "name": "Heat-labile enterotoxin (LT)", "type": "Target"},
        {"id": "n5", "name": "heat-labile toxin", "type": "Target"},
        {"id": "n6", "name": "FedF", "type": "Target"},
        {"id": "n7", "name": "GM Bacillus expressing FedF", "type": "InternalProgram"},
        {"id": "n8", "name": "LTB + FedF combo", "type": "Hypothesis"},
        {"id": "n9", "name": "Organic acids", "type": "Killed"},
    ]


def _edges():
    return [
        {"source_id": "n1", "target_id": "n4", "type": "PRODUCES"},
        {"source_id": "n2", "target_id": "n5", "type": "PRODUCES"},
    ]


class TestAliasRules:
    def test_etec_variants_merge(self):
        nodes = _nodes()
        remap, canonical = _apply_alias_rules(nodes)
        # F18 ETEC and F18 fimbriae both map to the F18 strain node
        assert remap["n1"] == remap["n2"] == "pathogen:f18-etec"
        # generic ETEC stays as the species node (correctly distinct from F18 strain)
        assert remap["n3"] == "pathogen:etec"

    def test_protected_types_never_merge(self):
        nodes = _nodes()
        remap, canonical = _apply_alias_rules(nodes)
        # InternalProgram, Hypothesis, Killed must keep their own ids
        assert remap["n7"] == "n7"  # GM Bacillus (InternalProgram)
        assert remap["n8"] == "n8"  # LTB+FedF (Hypothesis)
        assert remap["n9"] == "n9"  # Organic acids (Killed)

    def test_fedf_target_survives_program_with_fedf_in_name(self):
        nodes = _nodes()
        remap, canonical = _apply_alias_rules(nodes)
        # n6 FedF (Target) canonicalizes; n7 (InternalProgram) stays separate
        assert remap["n6"] != remap["n7"]


class TestHarmonizeGraph:
    def test_alias_only_reduces_entities(self):
        fn, fe, rep = harmonize_graph(_nodes(), _edges(), use_embeddings=False)
        assert rep.entities_after < rep.entities_before
        assert rep.alias_merges > 0

    def test_edges_remapped_and_deduped(self):
        fn, fe, rep = harmonize_graph(_nodes(), _edges(), use_embeddings=False)
        # both edges (n1→n4, n2→n5) collapse to one after ETEC + LT merge
        assert rep.edges_after <= rep.edges_before

    def test_no_self_loops_after_merge(self):
        # if two merged nodes had an edge between them, drop it
        nodes = [
            {"id": "a", "name": "F18 ETEC", "type": "Organism"},
            {"id": "b", "name": "F18 fimbriae", "type": "Organism"},
        ]
        edges = [{"source_id": "a", "target_id": "b", "type": "SAME"}]
        fn, fe, rep = harmonize_graph(nodes, edges, use_embeddings=False)
        assert len(fe) == 0  # a and b merge → self-loop dropped

    def test_protected_institutional_nodes_survive(self):
        fn, fe, rep = harmonize_graph(_nodes(), _edges(), use_embeddings=False)
        names = {n["name"] for n in fn}
        assert "GM Bacillus expressing FedF" in names
        assert "Organic acids" in names

    def test_report_has_examples(self):
        fn, fe, rep = harmonize_graph(_nodes(), _edges(), use_embeddings=False)
        assert len(rep.merge_examples) > 0
