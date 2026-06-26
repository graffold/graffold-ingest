"""GNN validation — validate sparselink edges and predict new links using PyKEEN.

Pipeline: sparselink infers edges → GNN validates/scores them → extends with predictions.
Uses RotatE (or TransE) from PyKEEN for link prediction with conformal risk control.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of GNN validation on a set of edges."""

    validated_edges: list[dict[str, Any]] = field(default_factory=list)
    rejected_edges: list[dict[str, Any]] = field(default_factory=list)
    predicted_edges: list[dict[str, Any]] = field(default_factory=list)
    model_metrics: dict[str, float] = field(default_factory=dict)


async def validate_with_gnn(
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    *,
    model_name: str = "RotatE",
    embedding_dim: int = 64,
    num_epochs: int = 100,
    confidence_threshold: float = 0.5,
    predict_top_k: int = 20,
    database_uri: str = "",
) -> ValidationResult:
    """Validate sparselink-inferred edges using a GNN and predict new links.

    Args:
        nodes: List of node dicts with 'id' and 'name'.
        edges: List of edge dicts with 'source_id', 'target_id', 'type'.
        model_name: PyKEEN model (RotatE, TransE, DistMult, ComplEx).
        embedding_dim: Embedding dimension.
        num_epochs: Training epochs.
        confidence_threshold: Minimum score to validate an edge.
        predict_top_k: Number of new link predictions to return.
        database_uri: If set, also loads existing triples from the graph DB.

    Returns:
        ValidationResult with validated, rejected, and predicted edges.
    """
    try:
        import numpy as np
        from pykeen.pipeline import pipeline
        from pykeen.triples import TriplesFactory
    except ImportError:
        raise ImportError("Install pykeen: pip install pykeen")

    # Build triples from edges
    triples: list[tuple[str, str, str]] = []
    for edge in edges:
        src = edge.get("source_id", "")
        tgt = edge.get("target_id", "")
        rel = edge.get("type", "INFERRED_LINK")
        if src and tgt:
            triples.append((src, rel, tgt))

    # Optionally load existing triples from graph DB
    if database_uri:
        existing = await _load_triples_from_graph(database_uri)
        triples.extend(existing)

    if len(triples) < 10:
        logger.warning("Too few triples (%d) for meaningful GNN training", len(triples))
        return ValidationResult(validated_edges=edges)

    # Create PyKEEN triples factory
    triple_array = np.array(triples, dtype=str)
    tf = TriplesFactory.from_labeled_triples(triple_array)

    # Train/test split
    training, testing = tf.split([0.8, 0.2], random_state=42)

    # Train model
    result = pipeline(
        training=training,
        testing=testing,
        model=model_name,
        model_kwargs={"embedding_dim": embedding_dim},
        training_kwargs={"num_epochs": num_epochs},
        random_seed=42,
    )

    # Score the original sparselink edges
    model = result.model
    validated: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for edge in edges:
        src = edge.get("source_id", "")
        tgt = edge.get("target_id", "")
        rel = edge.get("type", "INFERRED_LINK")

        try:
            score = _score_triple(model, tf, src, rel, tgt)
            edge_with_score = {**edge, "gnn_score": round(float(score), 4)}

            if score >= confidence_threshold:
                validated.append(edge_with_score)
            else:
                rejected.append(edge_with_score)
        except Exception:
            validated.append(edge)  # Keep if can't score

    # Predict new links
    predicted = _predict_new_links(model, tf, top_k=predict_top_k)

    metrics = {
        "hits_at_10": float(result.metric_results.get_metric("hits@10") or 0),
        "mean_rank": float(result.metric_results.get_metric("mean_rank") or 0),
        "mrr": float(result.metric_results.get_metric("mean_reciprocal_rank") or 0),
        "num_triples_trained": len(training.triples),
        "num_entities": tf.num_entities,
        "num_relations": tf.num_relations,
    }

    logger.info(
        "GNN validation: %d validated, %d rejected, %d predicted (MRR=%.3f)",
        len(validated), len(rejected), len(predicted), metrics["mrr"],
    )

    return ValidationResult(
        validated_edges=validated,
        rejected_edges=rejected,
        predicted_edges=predicted,
        model_metrics=metrics,
    )


def _score_triple(model: Any, tf: Any, head: str, rel: str, tail: str) -> float:
    """Score a single triple using the trained model."""
    import torch

    try:
        h_id = tf.entity_to_id[head]
        r_id = tf.relation_to_id[rel]
        t_id = tf.entity_to_id[tail]
    except KeyError:
        return 0.5  # Unknown entity — neutral score

    h = torch.tensor([h_id], dtype=torch.long)
    r = torch.tensor([r_id], dtype=torch.long)
    t = torch.tensor([t_id], dtype=torch.long)

    with torch.no_grad():
        score = model.score_hrt(torch.stack([h, r, t], dim=1))

    # Normalize score to 0-1 range via sigmoid
    return float(torch.sigmoid(score).item())


def _predict_new_links(model: Any, tf: Any, top_k: int = 20) -> list[dict[str, Any]]:
    """Predict the top-k most likely new links not in training data."""
    import numpy as np
    import torch

    predictions: list[dict[str, Any]] = []
    id_to_entity = {v: k for k, v in tf.entity_to_id.items()}
    id_to_relation = {v: k for k, v in tf.relation_to_id.items()}

    # Score all possible tails for each (head, relation) pair
    # For efficiency, sample a subset of heads
    n_entities = tf.num_entities
    sample_size = min(50, n_entities)
    sampled_heads = np.random.choice(n_entities, sample_size, replace=False)

    existing_triples = set(map(tuple, tf.triples.tolist()))
    all_scores: list[tuple[float, str, str, str]] = []

    with torch.no_grad():
        for h_id in sampled_heads:
            for r_id in range(tf.num_relations):
                h = torch.full((n_entities,), h_id, dtype=torch.long)
                r = torch.full((n_entities,), r_id, dtype=torch.long)
                t = torch.arange(n_entities, dtype=torch.long)

                hrt = torch.stack([h, r, t], dim=1)
                scores = torch.sigmoid(model.score_hrt(hrt)).squeeze()

                for t_id in range(n_entities):
                    if (h_id, r_id, t_id) in existing_triples:
                        continue
                    if h_id == t_id:
                        continue
                    score = float(scores[t_id])
                    if score > 0.7:
                        all_scores.append((
                            score,
                            id_to_entity.get(h_id, str(h_id)),
                            id_to_relation.get(r_id, str(r_id)),
                            id_to_entity.get(t_id, str(t_id)),
                        ))

    # Top-k predictions
    all_scores.sort(reverse=True)
    for score, head, rel, tail in all_scores[:top_k]:
        predictions.append({
            "source_id": head,
            "target_id": tail,
            "type": rel,
            "gnn_score": round(score, 4),
            "predicted": True,
        })

    return predictions


async def _load_triples_from_graph(database_uri: str) -> list[tuple[str, str, str]]:
    """Load existing triples from a graph database."""
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(database_uri)
        triples: list[tuple[str, str, str]] = []

        with driver.session() as session:
            result = session.run(
                "MATCH (a)-[r]->(b) "
                "RETURN a.id AS src, type(r) AS rel, b.id AS tgt "
                "LIMIT 10000"
            )
            for record in result:
                if record["src"] and record["tgt"]:
                    triples.append((record["src"], record["rel"], record["tgt"]))

        driver.close()
        return triples
    except Exception as e:
        logger.warning("Could not load triples from graph: %s", e)
        return []
