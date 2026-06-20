"""CSV/Excel/Parquet connector."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .base import Document


class CsvConnector:
    """Load tabular data from CSV, Excel, or Parquet files."""

    def name(self) -> str:
        return "csv"

    async def fetch(self, *, path: str = "", **kwargs: Any) -> list[Document]:
        """Read a tabular file and return rows as a Document."""
        if not path:
            return []

        file_path = Path(path)
        if not file_path.exists():
            return []

        suffix = file_path.suffix.lower()
        if suffix == ".parquet":
            try:
                import polars as pl

                df = pl.read_parquet(file_path)
                content = df.write_csv()
            except ImportError:
                content = file_path.read_text(errors="ignore")
        elif suffix in (".xlsx", ".xls"):
            try:
                import polars as pl

                df = pl.read_excel(file_path)
                content = df.write_csv()
            except ImportError:
                content = file_path.read_text(errors="ignore")
        else:
            content = file_path.read_text(errors="ignore")

        doc_id = hashlib.sha256(str(file_path).encode()).hexdigest()[:16]
        return [
            Document(
                id=doc_id,
                content=content,
                source_url=str(file_path),
                source_type="csv",
                title=file_path.name,
                metadata={"rows": content.count("\n")},
            )
        ]
