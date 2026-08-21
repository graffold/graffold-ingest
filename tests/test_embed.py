"""Tests for embedding generation."""

import json
from unittest.mock import patch, MagicMock

import pytest

from graffold_ingest.pipeline.embed import generate_embeddings, _BATCH_SIZE


class TestGenerateEmbeddings:
    def _mock_response(self, n_texts: int):
        """Create a mock CF Workers AI response."""
        embeddings = [[0.1] * 768 for _ in range(n_texts)]
        body = json.dumps({"success": True, "result": {"data": embeddings}}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = body
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    @patch.dict("os.environ", {"CF_ACCOUNT_ID": "test123", "CF_API_TOKEN": "tok"})
    @patch("urllib.request.urlopen")
    def test_single_text(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(1)
        result = generate_embeddings(["hello world"])
        assert len(result) == 1
        assert len(result[0]) == 768

    @patch.dict("os.environ", {"CF_ACCOUNT_ID": "test123", "CF_API_TOKEN": "tok"})
    @patch("urllib.request.urlopen")
    def test_batch_size_respected(self, mock_urlopen):
        n = _BATCH_SIZE + 5
        mock_urlopen.return_value = self._mock_response(_BATCH_SIZE)
        # Second call for remaining 5
        mock_urlopen.side_effect = [
            self._mock_response(_BATCH_SIZE),
            self._mock_response(5),
        ]
        result = generate_embeddings(["text"] * n)
        assert len(result) == n
        assert mock_urlopen.call_count == 2

    @patch.dict("os.environ", {"CF_ACCOUNT_ID": "", "CF_API_TOKEN": ""})
    def test_missing_credentials_raises(self):
        with pytest.raises(ValueError, match="CF_ACCOUNT_ID"):
            generate_embeddings(["test"])

    @patch.dict("os.environ", {"CF_ACCOUNT_ID": "test123", "CF_API_TOKEN": "tok"})
    @patch("urllib.request.urlopen")
    def test_empty_input(self, mock_urlopen):
        result = generate_embeddings([])
        assert result == []
        mock_urlopen.assert_not_called()
