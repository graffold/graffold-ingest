"""PDF document connector."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .base import Document


class PdfConnector:
    """Extract text from PDF files."""

    def name(self) -> str:
        return "pdf"

    async def fetch(self, *, path: str = "", directory: str = "", **kwargs: Any) -> list[Document]:
        """Extract text from PDF files."""
        paths: list[Path] = []
        if path:
            paths.append(Path(path))
        elif directory:
            paths.extend(Path(directory).glob("**/*.pdf"))

        docs: list[Document] = []
        for pdf_path in paths:
            if not pdf_path.exists():
                continue
            try:
                # Try pdftotext or fallback to reading raw
                import subprocess

                result = subprocess.run(
                    ["pdftotext", str(pdf_path), "-"],
                    capture_output=True, text=True, timeout=30,
                )
                content = result.stdout if result.returncode == 0 else pdf_path.read_text(errors="ignore")
            except (FileNotFoundError, subprocess.TimeoutExpired):
                content = pdf_path.read_text(errors="ignore")

            doc_id = hashlib.sha256(str(pdf_path).encode()).hexdigest()[:16]
            docs.append(
                Document(
                    id=doc_id,
                    content=content,
                    source_url=str(pdf_path),
                    source_type="pdf",
                    title=pdf_path.stem,
                )
            )

        return docs
