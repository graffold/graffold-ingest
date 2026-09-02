"""Global configuration for graffold-ingest.

Config lives at ~/.graffold/config.toml (user-level) with env var overrides.
Handles API keys, default paths, LLM backend selection.

Precedence: env vars > config.toml > defaults
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONFIG_DIR = Path.home() / ".graffold"
CONFIG_FILE = CONFIG_DIR / "config.toml"
PARQUET_DIR = CONFIG_DIR / "parquet"


@dataclass
class Config:
    """Global graffold-ingest configuration."""

    # Paths
    config_dir: Path = field(default_factory=lambda: CONFIG_DIR)
    parquet_dir: Path = field(default_factory=lambda: PARQUET_DIR)
    atlas_programs_dir: str = ""

    # LLM
    llm_service: str = "anthropic"
    llm_model: str = "claude-haiku-4-5-20251001"
    anthropic_api_key: str = ""

    # Graph DB (optional)
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""

    # Daemon
    poll_interval: int = 60
    max_concurrent: int = 5

    @classmethod
    def load(cls) -> Config:
        """Load config from file + env vars."""
        cfg = cls()

        # Load from TOML if exists
        if CONFIG_FILE.exists():
            cfg._load_toml()

        # Env var overrides (only if non-empty)
        if os.getenv("PARQUET_DIR"):
            cfg.parquet_dir = Path(os.environ["PARQUET_DIR"])
        if os.getenv("ATLAS_PROGRAMS_DIR"):
            cfg.atlas_programs_dir = os.environ["ATLAS_PROGRAMS_DIR"]
        if os.getenv("LLM_SERVICE"):
            cfg.llm_service = os.environ["LLM_SERVICE"]
        if os.getenv("LLM_MODEL"):
            cfg.llm_model = os.environ["LLM_MODEL"]
        if os.getenv("ANTHROPIC_API_KEY"):
            cfg.anthropic_api_key = os.environ["ANTHROPIC_API_KEY"]
        if os.getenv("NEO4J_URI"):
            cfg.neo4j_uri = os.environ["NEO4J_URI"]
        if os.getenv("NEO4J_USER"):
            cfg.neo4j_user = os.environ["NEO4J_USER"]
        if os.getenv("NEO4J_PASSWORD"):
            cfg.neo4j_password = os.environ["NEO4J_PASSWORD"]
        if os.getenv("GRAFFOLD_POLL"):
            cfg.poll_interval = int(os.environ["GRAFFOLD_POLL"])
        if os.getenv("GRAFFOLD_CONCURRENCY"):
            cfg.max_concurrent = int(os.environ["GRAFFOLD_CONCURRENCY"])

        return cfg

    def _load_toml(self) -> None:
        """Parse config.toml into self."""
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib  # type: ignore[no-redef]

        text = CONFIG_FILE.read_text()
        data = tomllib.loads(text)

        # [paths]
        paths = data.get("paths", {})
        if paths.get("parquet_dir"):
            self.parquet_dir = Path(paths["parquet_dir"]).expanduser()
        if paths.get("atlas_programs_dir"):
            self.atlas_programs_dir = paths["atlas_programs_dir"]

        # [llm]
        llm = data.get("llm", {})
        if llm.get("service"):
            self.llm_service = llm["service"]
        if llm.get("model"):
            self.llm_model = llm["model"]
        if llm.get("anthropic_api_key"):
            self.anthropic_api_key = llm["anthropic_api_key"]

        # [graph]
        graph = data.get("graph", {})
        if graph.get("uri"):
            self.neo4j_uri = graph["uri"]
        if graph.get("user"):
            self.neo4j_user = graph["user"]
        if graph.get("password"):
            self.neo4j_password = graph["password"]

        # [daemon]
        daemon = data.get("daemon", {})
        if daemon.get("poll_interval"):
            self.poll_interval = daemon["poll_interval"]
        if daemon.get("max_concurrent"):
            self.max_concurrent = daemon["max_concurrent"]

    def save(self) -> None:
        """Write current config to config.toml."""
        self.config_dir.mkdir(parents=True, exist_ok=True)

        lines = [
            "# graffold-ingest configuration",
            "# Env vars override these values (e.g. ANTHROPIC_API_KEY, LLM_SERVICE)",
            "",
            "[paths]",
            f'parquet_dir = "{self.parquet_dir}"',
        ]
        if self.atlas_programs_dir:
            lines.append(f'atlas_programs_dir = "{self.atlas_programs_dir}"')

        lines += [
            "",
            "[llm]",
            f'service = "{self.llm_service}"',
            f'model = "{self.llm_model}"',
        ]
        if self.anthropic_api_key:
            lines.append(f'anthropic_api_key = "{self.anthropic_api_key}"')

        lines += [
            "",
            "[graph]",
            f'uri = "{self.neo4j_uri}"',
            "",
            "[daemon]",
            f"poll_interval = {self.poll_interval}",
            f"max_concurrent = {self.max_concurrent}",
        ]

        CONFIG_FILE.write_text("\n".join(lines) + "\n")

    @property
    def is_configured(self) -> bool:
        """Check if minimum config exists (API key set)."""
        return bool(self.anthropic_api_key)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for display."""
        return {
            "config_dir": str(self.config_dir),
            "parquet_dir": str(self.parquet_dir),
            "atlas_programs_dir": self.atlas_programs_dir or "(not set)",
            "llm_service": self.llm_service,
            "llm_model": self.llm_model,
            "api_key": f"{self.anthropic_api_key[:12]}..." if self.anthropic_api_key else "(not set)",
            "neo4j_uri": self.neo4j_uri,
            "poll_interval": self.poll_interval,
            "max_concurrent": self.max_concurrent,
        }
