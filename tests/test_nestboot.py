"""Tests for NestBoot FDR filtering."""

import numpy as np
import pytest

from graffold_ingest.pipeline.nestboot_fdr import nestboot_fdr, FDRResult


class _InferenceResult:
    def __init__(self, adj):
        self.adjacency_matrix = adj


class _SimpleInference:
    """Correlation-based inference conforming to .fit(X) → .adjacency_matrix."""

    def fit(self, X: np.ndarray) -> _InferenceResult:
        corr = np.abs(np.corrcoef(X.T))
        np.fill_diagonal(corr, 0)
        return _InferenceResult(corr)


class TestNestBootFDR:
    def test_returns_fdr_result(self):
        np.random.seed(42)
        X = np.random.randn(50, 5)
        # Add a real signal
        X[:, 1] = X[:, 0] * 0.9 + np.random.randn(50) * 0.1
        result = nestboot_fdr(X, _SimpleInference(), n_bootstraps=10)
        assert isinstance(result, FDRResult)
        assert result.threshold >= 0
        assert result.fdr_estimate >= 0
        assert result.n_total_candidates > 0

    def test_strong_signal_produces_edges(self):
        np.random.seed(42)
        X = np.random.randn(100, 4)
        X[:, 1] = X[:, 0] + np.random.randn(100) * 0.01  # near-perfect correlation
        result = nestboot_fdr(X, _SimpleInference(), fdr_target=0.2, n_bootstraps=10)
        assert result.n_passing > 0
        # The 0-1 edge should be detected
        edge_pairs = [(e[0], e[1]) for e in result.edges]
        assert (0, 1) in edge_pairs or (1, 0) in edge_pairs

    def test_pure_noise_fdr_estimate_is_high(self):
        np.random.seed(123)
        X = np.random.randn(200, 5)  # More samples to reduce spurious correlations
        result = nestboot_fdr(X, _SimpleInference(), fdr_target=0.01, n_bootstraps=20)
        # With strict FDR on noise, threshold should be high (hard to pass)
        assert result.threshold >= 0.5

    def test_support_fractions_are_bounded(self):
        np.random.seed(42)
        X = np.random.randn(30, 3)
        result = nestboot_fdr(X, _SimpleInference(), n_bootstraps=10)
        assert np.all(result.support_fractions >= 0)
        assert np.all(result.support_fractions <= 1)

    def test_custom_fdr_target(self):
        np.random.seed(42)
        X = np.random.randn(50, 4)
        X[:, 1] = X[:, 0] * 0.8 + np.random.randn(50) * 0.2
        # Strict FDR
        strict = nestboot_fdr(X, _SimpleInference(), fdr_target=0.01, n_bootstraps=10)
        # Lenient FDR
        lenient = nestboot_fdr(X, _SimpleInference(), fdr_target=0.5, n_bootstraps=10)
        # Lenient should pass at least as many edges
        assert lenient.n_passing >= strict.n_passing

    def test_edge_weights_are_positive(self):
        np.random.seed(42)
        X = np.random.randn(50, 4)
        X[:, 1] = X[:, 0] + np.random.randn(50) * 0.05
        result = nestboot_fdr(X, _SimpleInference(), fdr_target=0.3, n_bootstraps=10)
        for _, _, weight in result.edges:
            assert weight > 0
