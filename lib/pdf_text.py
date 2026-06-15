"""PDF text extraction via PyMuPDF (fitz).

Used by the precedents backfill for records the courthouse serves as a PDF
(domar/beslut since March 2025) instead of inline HTML.
"""
from __future__ import annotations

import re

import fitz  # PyMuPDF


def extract_text(pdf_bytes: bytes) -> str:
    """Extract plain text from an in-memory PDF. Returns '' on any failure."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:  # noqa: BLE001 — corrupt / non-PDF payload
        return ""
    try:
        parts = [page.get_text("text") for page in doc]
    finally:
        doc.close()
    return _clean("\n".join(parts))


def _clean(text: str) -> str:
    """Collapse the ragged whitespace PDF extraction leaves behind."""
    text = text.replace("\x00", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
