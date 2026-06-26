"""SparseLink connector — infer network structure from tabular data.

Uses sparselink's 20+ algorithms to discover relationships from
expression/feature matrices, then publishes edges to the knowledge graph.

Usage:
    graffold-ingest pipeline --source sparselink --path data.csv --method glasso
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .base import Document


class SparseLinkConnector:
    """Infer network structure from tabular data via sparselink."""

    def name(self) -> str:
        return "sparselink"

    async def fetch(
        self,
        *,
        path: str = "",
        method: str = "glasso",
        alpha: float = 0.01,
        threshold: float = 0.0,
        feature_names: list[str] | None = None,
        **kwargs: Any,
    ) -> list[Document]:
        """Run sparselink inference and return edges as a Document.

        Args:
            path: CSV/Parquet file with features as columns, samples as rows.
            method: sparselink method name (lasso, glasso, pc, notears, dag_gnn, etc.)
            alpha: Regularization parameter.
            threshold: Minimum edge weight to include.
            feature_names: Column names to use (None = all numeric columns).

        Returns:
            List with one Document containing the inferred edge list as JSON.
        """
        import json

        import numpy as np

        try:
            from sparselink import get_method
        except ImportError:
            raise ImportError("Install sparselink: pip install sparselink")

        file_path = Path(path)
        if not file_path.exists():
            return []

        # Load data
        if file_path.suffix == ".parquet":
            import polars as pl

            df = pl.read_parquet(file_path).to_pandas()
        else:
            import pandas as pd

            df = pd.read_csv(file_path)

        # Select numeric columns
        if feature_names:
            df = df[feature_names]
        else:
            df = df.select_dtypes(include=[np.number])

        columns = list(df.columns)
        X = df.values

        # Run inference
        inference = get_method(method)
        params: dict[str, Any] = {}
        if alpha and method in ("lasso", "glasso", "elastic_net"):
            params["alpha"] = alpha
        result = inference(**params).fit(X)

        # Convert to edge list with names
        edges = []
        for src_idx, tgt_idx, weight in result.edge_list:
            if abs(weight) >= threshold:
                edges.append({
                    "source": columns[src_idx],
                    "target": columns[tgt_idx],
                    "weight": round(float(weight), 6),
                    "method": method,
                })

        content = json.dumps({
            "edges": edges,
            "n_features": len(columns),
            "n_samples": X.shape[0],
            "n_edges": len(edges),
            "method": method,
            "features": columns,
        }, indent=2)

        doc_id = hashlib.sha256(f"{path}:{method}".encode()).hexdigest()[:16]
        return [
            Document(
                id=doc_id,
                content=content,
                source_url=str(file_path),
                source_type="sparselink",
                title=f"{file_path.stem} ({method})",
                metadata={
                    "method": method,
                    "n_features": len(columns),
                    "n_edges": len(edges),
                },
            )
        ]
