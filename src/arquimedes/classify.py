"""Deterministic classifiers for domain, document type, and keyword extraction.

These run during extract-raw (no LLM needed). They provide a reliable first pass
that the LLM enrichment can later refine or override.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from pathlib import Path

from arquimedes.models import Page


# --- Keyword Extraction (TF-IDF) ---

# Common stop words (English + Spanish, architecture domain)
STOP_WORDS = frozenset({
    # English
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "of",
    "with", "by", "from", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could", "should",
    "may", "might", "can", "shall", "this", "that", "these", "those", "it", "its",
    "not", "no", "nor", "as", "if", "then", "than", "too", "very", "just",
    "about", "above", "after", "again", "all", "also", "am", "any", "because",
    "before", "between", "both", "during", "each", "few", "he", "her", "here",
    "him", "his", "how", "into", "me", "more", "most", "my", "new", "now",
    "only", "other", "our", "out", "over", "own", "same", "she", "so", "some",
    "still", "such", "there", "they", "their", "them", "through", "under", "up",
    "us", "we", "what", "when", "where", "which", "while", "who", "whom", "why",
    "you", "your", "one", "two", "use", "used", "using", "well", "also", "however",
    "see", "fig", "figure", "table", "page", "pp", "vol", "no", "ed", "eds",
    # Spanish
    "el", "la", "los", "las", "un", "una", "unos", "unas", "de", "del", "al",
    "en", "con", "por", "para", "es", "son", "fue", "ser", "estar", "hay",
    "que", "se", "su", "sus", "como", "más", "pero", "sin", "sobre",
})


def extract_keywords(pages: list[Page], max_keywords: int = 15) -> list[str]:
    """Extract keywords from document text using TF-IDF scoring.

    Simple but effective: tokenize, remove stop words, score by term frequency
    weighted against inverse document frequency (treating each page as a document).

    Returns top keywords sorted by score.
    """
    if not pages:
        return []

    # Tokenize all pages
    page_tokens: list[list[str]] = []
    all_tokens: list[str] = []

    for page in pages:
        tokens = _tokenize(page.text)
        page_tokens.append(tokens)
        all_tokens.extend(tokens)

    if not all_tokens:
        return []

    # Term frequency across entire document
    tf = Counter(all_tokens)
    total_terms = len(all_tokens)

    # Document frequency (how many pages contain this term)
    df: Counter[str] = Counter()
    for tokens in page_tokens:
        for term in set(tokens):
            df[term] += 1

    num_pages = len(page_tokens)

    # TF-IDF scoring
    scores: dict[str, float] = {}
    for term, count in tf.items():
        if len(term) < 3:  # skip very short terms
            continue
        tf_score = count / total_terms
        idf_score = math.log((num_pages + 1) / (df[term] + 1)) + 1
        scores[term] = tf_score * idf_score

    # Also boost multi-word terms (bigrams) that appear frequently
    bigram_scores = _score_bigrams(pages, tf, total_terms)
    scores.update(bigram_scores)

    # Sort by score, return top N
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [term for term, _ in ranked[:max_keywords]]


def _tokenize(text: str) -> list[str]:
    """Tokenize text into lowercase words, filtering stop words and noise."""
    # Remove soft hyphens first — PDF text often has them mid-word
    text = text.replace("\xad", "")
    words = re.findall(r"[a-zA-ZÀ-ÿ]{3,}", text.lower())
    return [w for w in words if w not in STOP_WORDS and not w.isdigit()]


def _score_bigrams(
    pages: list[Page], unigram_tf: Counter, total_terms: int
) -> dict[str, float]:
    """Score meaningful bigrams (two-word phrases) from the text."""
    bigram_counts: Counter[str] = Counter()

    for page in pages:
        tokens = _tokenize(page.text)
        for i in range(len(tokens) - 1):
            bigram = f"{tokens[i]} {tokens[i+1]}"
            bigram_counts[bigram] += 1

    scores: dict[str, float] = {}
    for bigram, count in bigram_counts.items():
        if count < 2:  # only keep bigrams that appear more than once
            continue
        # Score: frequency * bonus for being a multi-word term
        scores[bigram] = (count / total_terms) * 2.0

    return scores


# --- Document Type Classification ---

DOC_TYPE_PATTERNS: list[tuple[str, list[str], list[str]]] = [
    # (type, text_signals, filename_signals)
    ("regulation", [
        "article ", "section ", "compliance", "shall ", "regulation",
        "building code", "normativa", "reglamento",
    ], ["regulation", "code", "norm", "standard"]),
    ("paper", [
        "abstract", "keywords:", "introduction", "methodology",
        "findings", "conclusion", "references", "doi",
        "published by", "university press", "volume", "journal",
    ], ["paper", "article", "journal"]),
    ("catalogue", [
        "product catalogue", "product catalog", "catalogue number",
        "catalog number", "specifications", "dimensions", "weight",
        "material:", "order code", "order number",
    ], ["catalogue", "catalog", "product"]),
    ("lecture_note", [
        "lecture", "class ", "students", "assignment", "exam",
        "semester", "course", "syllabus",
    ], ["lecture", "class", "course", "notes"]),
    ("monograph", [
        "chapter ", "chapters", "preface", "foreword",
        "acknowledgments", "index",
    ], ["monograph"]),
    ("technical_spec", [
        "specification", "performance", "testing", "load",
        "thermal", "acoustic", "fire resistance", "u-value",
    ], ["spec", "technical", "datasheet"]),
]


def classify_document_type(
    pages: list[Page], title: str = "", filename: str = ""
) -> str | None:
    """Classify a document by type based on structural and textual cues.

    Returns a document_type string or None if unclear.
    """
    # Check first 3 pages for structural cues
    text_lower = " ".join(p.text.replace("\xad", "").lower() for p in pages[:3])
    filename_lower = filename.lower()

    scores: dict[str, int] = {}

    for doc_type, text_signals, filename_signals in DOC_TYPE_PATTERNS:
        score = 0
        for signal in text_signals:
            if signal in text_lower:
                score += 1
        for signal in filename_signals:
            if signal in filename_lower:
                score += 2  # filename signals are stronger
        if score > 0:
            scores[doc_type] = score

    if not scores:
        return None

    best_type = max(scores, key=scores.get)
    best_score = scores[best_type]

    # Need a clear signal
    if best_score >= 3:
        return best_type

    return None  # ambiguous, defer to LLM
