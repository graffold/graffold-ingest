"""Tabular data chunking — splits CSV/TSV/Excel into row-group chunks for LLM extraction.

Unlike text chunking (sentence boundaries), tabular chunking:
- Preserves the header row in every chunk
- Groups rows together (default 50 rows per chunk)
- Converts to markdown table format for LLM comprehension
- Tracks row ranges for provenance

Ported from bioingest.pipeline.tabular.
"""

from __future__ import annotations

import csv
import hashlib
import logging
from pathlib import Path

from ..connectors.base import Document

logger = logging.getLogger(__name__)


def chunk_tabular(
    path: str | Path,
    rows_per_chunk: int = 50,
    delimiter: str | None = None,
    doc_id: str | None = None,
) -> list[Document]:
    """Read a tabular file and split into row-group Document chunks.

    Args:
        path: Path to CSV/TSV/Excel file
        rows_per_chunk: Number of data rows per chunk
        delimiter: Override delimiter (auto-detected from extension)
        doc_id: Document ID (defaults to filename hash)

    Returns:
        List of Document chunks with markdown table content
    """
    path = Path(path)
    if doc_id is None:
        doc_id = f"tab_{hashlib.sha256(str(path).encode()).hexdigest()[:10]}"

    ext = path.suffix.lower()

    if ext in (".xlsx", ".xls"):
        rows, header = _read_excel(path)
    else:
        if delimiter is None:
            delimiter = "\t" if ext == ".tsv" else ","
        rows, header = _read_csv(path, delimiter)

    if not rows:
        return []

    chunks: list[Document] = []
    total_rows = len(rows)

    for start in range(0, total_rows, rows_per_chunk):
        end = min(start + rows_per_chunk, total_rows)
        chunk_rows = rows[start:end]
        text = _rows_to_markdown(header, chunk_rows)

        chunks.append(Document(
            id=f"{doc_id}_rows_{start + 1}_{end}",
            content=text,
            source_url=str(path),
            source_type="csv",
            title=f"{path.name} rows {start + 1}-{end}",
            chunk_id=f"{doc_id}_rows_{start + 1}_{end}",
            metadata={
                "file_name": path.name,
                "total_rows": total_rows,
                "row_start": start + 1,
                "row_end": end,
                "columns": header,
            },
        ))

    logger.info("Chunked %s: %d rows → %d chunks", path.name, total_rows, len(chunks))
    return chunks


def _read_csv(path: Path, delimiter: str) -> tuple[list[list[str]], list[str]]:
    """Read CSV/TSV, return (rows, header)."""
    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader, [])
        rows = [row for row in reader if any(cell.strip() for cell in row)]
    return rows, header


def _read_excel(path: Path) -> tuple[list[list[str]], list[str]]:
    """Read Excel file, return (rows, header)."""
    try:
        import openpyxl

        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        all_rows = [[str(cell.value or "") for cell in row] for row in ws.iter_rows()]
        wb.close()
    except ImportError:
        raise ImportError("openpyxl required for Excel: uv add openpyxl")

    if not all_rows:
        return [], []
    return all_rows[1:], all_rows[0]


def _rows_to_markdown(header: list[str], rows: list[list[str]]) -> str:
    """Convert rows to markdown table with header."""
    lines = ["| " + " | ".join(header) + " |"]
    lines.append("| " + " | ".join("---" for _ in header) + " |")
    for row in rows:
        padded = (row + [""] * len(header))[: len(header)]
        padded = [cell[:200] if len(cell) > 200 else cell for cell in padded]
        lines.append("| " + " | ".join(padded) + " |")
    return "\n".join(lines)
