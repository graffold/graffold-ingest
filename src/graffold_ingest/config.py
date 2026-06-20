"""Configuration from environment."""

from __future__ import annotations

import os


class Settings:
    """Settings loaded from environment variables."""

    def __init__(self) -> None:
        self.database_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.database_name = os.getenv("NEO4J_DATABASE", "neo4j")
        self.database_user = os.getenv("NEO4J_USER", "neo4j")
        self.database_password = os.getenv("NEO4J_PASSWORD", "")
        self.llm_service = os.getenv("LLM_SERVICE", "bedrock")
        self.llm_model = os.getenv("LLM_MODEL", "")
        self.data_dir = os.getenv("DATA_DIR", "./data")
