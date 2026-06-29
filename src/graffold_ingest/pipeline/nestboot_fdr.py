"""NestBoot FDR filter — bootstrap + shuffle to control false discovery rate.

Wraps any sparselink inference method with NestBoot's FDR procedure:
1. Bootstrap-resample the data → run inference → get real edge support
2. Shuffle columns → run inference → get null edge support
3. Find threshold where FDR(t) = #null≥t / #real≥t ≤ target_fdr
4. Only keep edges passing the threshold

Lightweight implementation — no pyGS dependency required.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class FDRResult:
    """Result of NestBoot FDR filtering."""

    edges: list[tuple[int, int, float]]  # (source_idx, target_idx, weight)
    threshold: float
    fdr_estimate: float
    n_total_candidates: int
    n_passing: int
    support_fractions: np.ndarray  # Per-edge support across bootstraps


def nestboot_fdr(
    X: np.ndarray,
    inference_fn: Any,
    *,
    fdr_target: float = 0.05,
    n_bootstraps: int = 50,
    n_shuffles: int = 50,
    seed: int = 42,
    inference_kwargs: dict[str, Any] | None = None,
) -> FDRResult:
    """Run NestBoot FDR on a data matrix using any sparselink method.

    Args:
        X: Data matrix (samples × features).
        inference_fn: A sparselink method instance with .fit(X) → InferenceResult.
        fdr_target: Target false discovery rate (default 5%).
        n_bootstraps: Number of bootstrap resamples for real data.
        n_shuffles: Number of column-shuffled runs for null.
        seed: Random seed.
        inference_kwargs: Extra kwargs passed to inference_fn.fit().

    Returns:
        FDRResult with FDR-filtered edges and diagnostics.
    """
    rng = np.random.default_rng(seed)
    n_samples, n_features = X.shape
    inference_kwargs = inference_kwargs or {}

    # ─── Real bootstrap runs ───────────────────────────────────────────
    real_counts = np.zeros((n_features, n_features))

    for _ in range(n_bootstraps):
        # Bootstrap resample rows
        idx = rng.choice(n_samples, size=n_samples, replace=True)
        X_boot = X[idx]

        result = inference_fn.fit(X_boot)
        adj = np.abs(result.adjacency_matrix)
        real_counts += (adj > 0).astype(float)

    real_support = real_counts / n_bootstraps  # Fraction of bootstraps each edge appeared

    # ─── Null (shuffled) runs ──────────────────────────────────────────
    null_counts = np.zeros((n_features, n_features))

    for _ in range(n_shuffles):
        # Shuffle each column independently (preserves marginals, destroys structure)
        X_shuf = X.copy()
        for col in range(n_features):
            rng.shuffle(X_shuf[:, col])

        result = inference_fn.fit(X_shuf)
        adj = np.abs(result.adjacency_matrix)
        null_counts += (adj > 0).astype(float)

    null_support = null_counts / n_shuffles

    # ─── FDR threshold search ──────────────────────────────────────────
    # Find lowest threshold t where FDR(t) = #null≥t / #real≥t ≤ target
    # Scan from high to low (most permissive passing threshold)
    t_values = np.linspace(0.05, 1.0, 96)
    best_t = 1.0
    best_fdr = 1.0

    for t in reversed(t_values):
        n_real = (real_support >= t).sum()
        if n_real == 0:
            continue
        n_null = (null_support >= t).sum()
        fdr_est = n_null / n_real

        if fdr_est <= fdr_target:
            best_t = t
            best_fdr = fdr_est
            break

    # ─── Extract passing edges ─────────────────────────────────────────
    passing_mask = real_support >= best_t
    np.fill_diagonal(passing_mask, False)  # No self-loops

    rows, cols = np.nonzero(passing_mask)
    edges = [
        (int(r), int(c), float(real_support[r, c]))
        for r, c in zip(rows, cols)
    ]

    logger.info(
        "NestBoot FDR: %d/%d edges pass (threshold=%.3f, FDR≈%.3f)",
        len(edges), int((real_support > 0).sum()), best_t, best_fdr,
    )

    return FDRResult(
        edges=edges,
        threshold=best_t,
        fdr_estimate=best_fdr,
        n_total_candidates=int((real_support > 0).sum()),
        n_passing=len(edges),
        support_fractions=real_support,
    )
