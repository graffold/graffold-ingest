"""Vision-based PDF processor — extracts content from scanned/image-heavy PDFs.

Uses markitdown as primary extractor, falls back to vision LLM (sends page
images to Claude/GPT-4V) for PDFs where text extraction fails.
"""

from __future__ import annotations

import base64
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

VISION_PROMPT = (
    "Extract all content from this PDF page. Return JSON with:\n"
    '- "text": all readable text on the page\n'
    '- "figures": list of figure descriptions (empty list if none)\n'
    '- "tables": list of tables, each as a list of rows (empty list if none)\n'
    "Return ONLY valid JSON."
)


def _parse_vision_response(text: str) -> dict[str, Any]:
    """Parse JSON from vision model response, stripping markdown fences."""
    cleaned = re.sub(r"^```(?:json)?\s*", "", text.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        return {"text": text, "figures": [], "tables": []}
    return {
        "text": data.get("text", ""),
        "figures": data.get("figures", []),
        "tables": data.get("tables", []),
    }


class VisionExtractor:
    """Extract content from PDFs using markitdown + vision LLM fallback."""

    def __init__(
        self,
        service: str = "bedrock",
        model_id: str = "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        region: str = "us-east-1",
    ):
        self.service = service
        self.model_id = model_id
        self.region = region

    async def extract(self, pdf_path: str) -> dict[str, Any]:
        """Extract text, figures, and tables from a PDF.

        Tries markitdown first. Falls back to vision LLM for image-heavy PDFs.
        """
        text = self._try_markitdown(pdf_path)
        if text and len(text) > 200:
            return {"text": text, "figures": [], "tables": []}

        return self._extract_with_vision(pdf_path)

    def _try_markitdown(self, pdf_path: str) -> str | None:
        try:
            from markitdown import MarkItDown

            result = MarkItDown().convert(pdf_path)
            return result.text_content
        except Exception:
            return None

    def _extract_with_vision(self, pdf_path: str) -> dict[str, Any]:
        """Send each page as an image to the vision LLM."""
        try:
            import fitz
        except ImportError:
            logger.error("pymupdf not installed — run: pip install pymupdf")
            return {"text": "", "figures": [], "tables": []}

        doc = fitz.open(pdf_path)
        all_text: list[str] = []
        all_figures: list[str] = []
        all_tables: list[list] = []

        for i, page in enumerate(doc):
            try:
                pix = page.get_pixmap(dpi=150)
                image_b64 = base64.b64encode(pix.tobytes("png")).decode()
                response_text = self._call_vision(image_b64)
                parsed = _parse_vision_response(response_text)
                if parsed["text"]:
                    all_text.append(parsed["text"])
                all_figures.extend(parsed["figures"])
                all_tables.extend(parsed["tables"])
            except Exception as e:
                logger.warning("Vision failed for page %d: %s", i, e)

        doc.close()
        return {
            "text": "\n\n".join(all_text),
            "figures": all_figures,
            "tables": all_tables,
        }

    def _call_vision(self, image_b64: str) -> str:
        """Send image to vision LLM and return response text."""
        if self.service == "bedrock":
            return self._call_bedrock_vision(image_b64)
        elif self.service == "openai":
            return self._call_openai_vision(image_b64)
        raise ValueError(f"Vision not supported for service: {self.service}")

    def _call_bedrock_vision(self, image_b64: str) -> str:
        import boto3

        client = boto3.client("bedrock-runtime", region_name=self.region)
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": image_b64}},
                    {"type": "text", "text": VISION_PROMPT},
                ],
            }],
        })
        resp = client.invoke_model(modelId=self.model_id, body=body)
        result = json.loads(resp["body"].read())
        return result["content"][0]["text"]

    def _call_openai_vision(self, image_b64: str) -> str:
        import openai

        client = openai.OpenAI()
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                    {"type": "text", "text": VISION_PROMPT},
                ],
            }],
        )
        return resp.choices[0].message.content or ""
