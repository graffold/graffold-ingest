"""Document chunking."""

from __future__ import annotations

from ..connectors.base import Document


def chunk_documents(
    documents: list[Document],
    chunk_size: int = 2000,
    overlap: int = 200,
) -> list[Document]:
    """Split documents into smaller chunks for extraction."""
    chunks: list[Document] = []
    for doc in documents:
        text = doc.content
        if len(text) <= chunk_size:
            chunks.append(doc)
            continue

        start = 0
        idx = 0
        while start < len(text):
            end = start + chunk_size
            chunk_text = text[start:end]
            chunks.append(
                Document(
                    id=f"{doc.id}_chunk{idx}",
                    content=chunk_text,
                    source_url=doc.source_url,
                    source_type=doc.source_type,
                    title=doc.title,
                    chunk_id=f"{doc.id}_chunk{idx}",
                    metadata={**doc.metadata, "chunk_index": idx},
                )
            )
            start = end - overlap
            idx += 1

    return chunks
