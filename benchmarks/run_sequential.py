"""Sequential enrichment driver — runs multiple programs back-to-back.

Avoids concurrent Bedrock/Ollama contention. Each program: fetch → extract →
resolve → harmonize. Logs to demo/<slug>.log via the caller's redirection.

Usage:
    AWS_REGION=us-east-1 python benchmarks/run_sequential.py alltech-blinded-a elanco-coccidiosis zoetis-mastitis
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from multi_program import enrich  # noqa: E402


async def main(slugs: list[str], papers: int) -> None:
    for slug in slugs:
        t0 = time.time()
        print(f"\n{'#' * 60}\n# START {slug}  ({time.strftime('%H:%M:%S')})\n{'#' * 60}", flush=True)
        try:
            await enrich(slug, papers)
        except Exception as e:
            print(f"!! {slug} FAILED: {e}", flush=True)
        print(f"# DONE {slug} in {(time.time() - t0) / 60:.1f} min", flush=True)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.isdigit()]
    papers = next((int(a) for a in sys.argv[1:] if a.isdigit()), 1000)
    asyncio.run(main(args, papers))
