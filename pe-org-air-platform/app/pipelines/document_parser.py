from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pdfplumber
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

SECTION_PATTERNS = {
    "item_1": r"(?is)\bITEM\s*1[.\s]*BUSINESS\b",
    "item_1a": r"(?is)\bITEM\s*1A[.\s]*RISK\s*FACTORS\b",
    "item_7": r"(?is)\bITEM\s*7[.\s]*MANAGEMENT",
    "item_7a": r"(?is)\bITEM\s*7A\b",
}


ITEM_1 = "Item 1"
ITEM_1A = "Item 1A"
ITEM_7 = "Item 7"


@dataclass(frozen=True)
class ParsedDocument:
    content_hash: str
    full_text: str
    sections: Dict[str, str]  # {"Item 1": "...", "Item 1A": "...", "Item 7": "..."}
    word_count: int


@dataclass(frozen=True)
class TextChunk:
    chunk_index: int
    section: Optional[str]
    content: str
    start_char: int
    end_char: int
    word_count: int


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _parse_html_bytes(b: bytes) -> str:
    soup = BeautifulSoup(b, "lxml")
    # remove scripts/styles/nav noise
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_pdf_bytes(b: bytes) -> str:
    # pdfplumber expects a file-like object; easiest is to write temp or use BytesIO
    from io import BytesIO

    text_parts: List[str] = []
    with pdfplumber.open(BytesIO(b)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t:
                text_parts.append(t)
    text = "\n".join(text_parts)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_filing_bytes(content: bytes, file_hint: str) -> ParsedDocument:
    """
    file_hint: filename or path; used to guess html vs pdf.
    """
    hint = file_hint.lower()
    if hint.endswith(".pdf"):
        full_text = _parse_pdf_bytes(content)
    else:
        # many SEC primary docs end with .htm / .html / .txt (HTML-ish)
        full_text = _parse_html_bytes(content)

    h = sha256_text(full_text)
    wc = len(full_text.split())
    sections = extract_key_sections(full_text)
    return ParsedDocument(content_hash=h, full_text=full_text, sections=sections, word_count=wc)


def _find_all(pattern: str, text: str):
    return [m.start() for m in re.finditer(pattern, text)]

def extract_key_sections(full_text: str) -> Dict[str, str]:
    text = re.sub(r"[ \t]+", " ", full_text)

    def last_match(pat):
        hits = _find_all(pat, text)
        return hits[-1] if hits else None

    i1 = last_match(SECTION_PATTERNS["item_1"]) or last_match(r"(?is)\bitem\s+1\b")
    i1a = last_match(SECTION_PATTERNS["item_1a"]) or last_match(r"(?is)\bitem\s+1a\b")
    i7 = last_match(SECTION_PATTERNS["item_7"]) or last_match(r"(?is)\bitem\s+7\b")
    i7a = last_match(SECTION_PATTERNS["item_7a"]) or last_match(r"(?is)\bitem\s+7a\b")

    def slice_section(start, end):
        if start is None:
            return ""
        if end is None:
            return text[start:start + 80000]   # cap
        if end <= start:
            return text[start:start + 80000]
        return text[start:end]


    sections = {
    "Item 1": slice_section(i1, i1a or i7),
    "Item 1A": slice_section(i1a, i7),
    "Item 7": slice_section(i7, i7a),
    "Item 7A": slice_section(i7a, None),
    }


    # Drop tiny false positives
    for k in list(sections.keys()):
        if len(sections[k]) < 1000:
            sections[k] = ""

    return sections


def chunk_text(
    text: str,
    section: Optional[str],
    chunk_size_chars: int = 4000,
    overlap_chars: int = 400,
) -> List[TextChunk]:
    """
    Simple chunking with overlap. This satisfies “chunking with overlap”.
    You can later replace with true semantic chunking if needed.
    """
    if not text:
        return []
    chunks: List[TextChunk] = []
    n = len(text)
    start = 0
    idx = 0
    while start < n:
        end = min(n, start + chunk_size_chars)
        content = text[start:end].strip()
        if content:
            chunks.append(
                TextChunk(
                    chunk_index=idx,
                    section=section,
                    content=content,
                    start_char=start,
                    end_char=end,
                    word_count=len(content.split()),
                )
            )
            idx += 1
        if end >= n:
            break
        start = max(0, end - overlap_chars)
    return chunks


def chunk_document(parsed: ParsedDocument) -> List[TextChunk]:
    """
    Prefer chunking extracted sections; fallback to full text if sections missing.
    """
    chunks: List[TextChunk] = []
    # If at least one section has content, chunk by section
    non_empty = {k: v for k, v in parsed.sections.items() if v}
    if non_empty:
        idx_offset = 0
        for sec, sec_text in non_empty.items():
            sec_chunks = chunk_text(sec_text, section=sec)
            # reindex to keep global ordering
            for c in sec_chunks:
                chunks.append(
                    TextChunk(
                        chunk_index=idx_offset + c.chunk_index,
                        section=c.section,
                        content=c.content,
                        start_char=c.start_char,
                        end_char=c.end_char,
                        word_count=c.word_count,
                    )
                )
            idx_offset = len(chunks)
    else:
        chunks = chunk_text(parsed.full_text, section=None)
    return chunks
