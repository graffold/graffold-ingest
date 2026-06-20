"""Database connector — SQL databases."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .base import Document


class DatabaseConnector:
    """Fetch data from SQL databases."""

    def name(self) -> str:
        return "database"

    async def fetch(
        self,
        *,
        connection_string: str = "",
        query: str = "SELECT * FROM information_schema.tables LIMIT 100",
        **kwargs: Any,
    ) -> list[Document]:
        """Execute a SQL query and return results as a Document."""
        if not connection_string:
            return []

        try:
            import sqlite3

            # For now, support sqlite. Extend with sqlalchemy for others.
            conn = sqlite3.connect(connection_string)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query)
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()

            content = json.dumps(rows, indent=2, default=str)
            doc_id = hashlib.sha256(f"{connection_string}:{query}".encode()).hexdigest()[:16]

            return [
                Document(
                    id=doc_id,
                    content=content,
                    source_url=connection_string,
                    source_type="database",
                    title=query[:60],
                    metadata={"row_count": len(rows)},
                )
            ]
        except Exception:
            return []
