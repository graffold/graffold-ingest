"""Parse scientific paper text into labeled sections.

Ported from bioingest.pipeline.section_parser. Improves LLM extraction
by splitting papers into Abstract/Methods/Results and dropping References.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

STANDARD_SECTIONS = [
    "Abstract",
    "Introduction",
    "Methods",
    "Materials and Methods",
    "Results",
    "Discussion",
    "Conclusion",
    "References",
    "Acknowledgements",
]

_HEADING_RE = re.compile(
    r"^(?:"
    r"#{1,6}\s+(.+)"  # markdown headings
    r"|(\d+\.?\s+[A-Z][A-Za-z &/-]+)"  # numbered headings
    r"|([A-Z][A-Z &/-]{2,})"  # ALL CAPS (3+ chars)
    r"|([A-Z][a-z]+(?:\s+(?:and|of|the|in|for|&)\s+[A-Za-z]+|\s+[A-Z][a-z]+)*)$"  # Title Case
    r")",
    re.MULTILINE,
)


@dataclass
class PaperSection:
    """A labeled section of a scientific paper."""

    label: str
    text: str
    start_offset: int
    end_offset: int


def parse_sections(text: str) -> list[PaperSection]:
    """Parse text into labeled sections based on detected headings."""
    headings: list[tuple[int, int, str]] = []

    for m in _HEADING_RE.finditer(text):
        label = next(g for g in m.groups() if g is not None).strip()
        label = re.sub(r"^\d+\.?\s+", "", label)
        headings.append((m.start(), m.end(), _normalize_label(label)))

    if not headings:
        return [PaperSection(label="Body", text=text, start_offset=0, end_offset=len(text))]

    sections: list[PaperSection] = []

    if headings[0][0] > 0:
        pre = text[: headings[0][0]].strip()
        if pre:
            sections.append(PaperSection("Body", pre, 0, headings[0][0]))

    for i, (start, end, label) in enumerate(headings):
        next_start = headings[i + 1][0] if i + 1 < len(headings) else len(text)
        body = text[end:next_start].strip()
        sections.append(PaperSection(label, body, start, next_start))

    return sections


def remove_sections(text: str, sections_to_remove: list[str] | None = None) -> str:
    """Strip specified sections (default: References, Acknowledgements)."""
    if sections_to_remove is None:
        sections_to_remove = ["References", "Acknowledgements"]
    sections = parse_sections(text)
    remove_lower = {s.lower() for s in sections_to_remove}
    keep = [s for s in sections if s.label.lower() not in remove_lower]
    return "\n\n".join(s.text for s in keep)


def _normalize_label(raw: str) -> str:
    """Map heading text to a standard label if possible."""
    lower = raw.lower().strip()
    for std in STANDARD_SECTIONS:
        if std.lower() == lower:
            return std
    if "method" in lower or "material" in lower:
        return "Materials and Methods" if "material" in lower else "Methods"
    if "result" in lower:
        return "Results"
    if "discuss" in lower:
        return "Discussion"
    if "introduc" in lower:
        return "Introduction"
    if "abstract" in lower:
        return "Abstract"
    if "conclus" in lower:
        return "Conclusion"
    return raw
