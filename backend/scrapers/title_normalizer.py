"""
Title normalizer for Israeli cinema movie names.

Handles Hebrew language/version suffixes that cinema sites append to movie titles,
e.g., "צעקה 7 מדובב לעברית" → base title "צעקה 7", language "dubbed_hebrew".
"""
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Ordered longest-first so "מדובב לעברית" matches before "עברית"
LANGUAGE_SUFFIXES = [
    # Dubbed variants
    (r"מדובב לעברית", "dubbed_hebrew"),
    (r"דיבוב לעברית", "dubbed_hebrew"),
    (r"מדובב", "dubbed"),
    # Subtitled variants
    (r"עם כתוביות בעברית", "subtitled_hebrew"),
    (r"כתוביות בעברית", "subtitled_hebrew"),
    (r"עם כתוביות", "subtitled"),
    (r"כתוביות", "subtitled"),
    # Language labels
    (r"עברית", "hebrew"),
    (r"אנגלית", "english"),
    (r"רוסית", "russian"),
    (r"ערבית", "arabic"),
    (r"צרפתית", "french"),
    (r"ספרדית", "spanish"),
]

# Compiled patterns: match suffix at end of string, optionally preceded by separator
_SUFFIX_PATTERNS = [
    (re.compile(r"\s*[-–—:|/]\s*" + pat + r"\s*$"), lang)
    for pat, lang in LANGUAGE_SUFFIXES
] + [
    (re.compile(r"\s+" + pat + r"\s*$"), lang)
    for pat, lang in LANGUAGE_SUFFIXES
]


def normalize_title(title: str) -> str:
    """Normalize a movie title for comparison.

    Strips language suffixes, extra whitespace, and common punctuation variations.
    """
    text = title.strip()
    # Strip language suffixes (multiple passes for titles with multiple suffixes)
    for _ in range(2):
        for pattern, _ in _SUFFIX_PATTERNS:
            text = pattern.sub("", text)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_language(title: str) -> Optional[str]:
    """Extract language info from a movie title suffix.

    Returns a language code like 'dubbed_hebrew', 'english', etc., or None.
    """
    text = title.strip()
    for pattern, lang in _SUFFIX_PATTERNS:
        if lang and pattern.search(text):
            return lang
    return None


def match_title(scraped_title: str, allowed_cache: dict) -> Optional[tuple]:
    """Try to match a scraped title against the allowed movies whitelist.

    Args:
        scraped_title: Raw title from scraper
        allowed_cache: dict mapping normalized_title -> AllowedMovie object

    Returns:
        (canonical_title, language) tuple, or None if no match
    """
    normalized = normalize_title(scraped_title)
    language = extract_language(scraped_title)

    # Tier 1: Exact normalized match
    if normalized in allowed_cache:
        return (allowed_cache[normalized].title, language)

    # Tier 2: Case-insensitive match (for English titles)
    normalized_lower = normalized.lower()
    for norm_key, allowed in allowed_cache.items():
        if norm_key.lower() == normalized_lower:
            return (allowed.title, language)

    # Tier 3: Containment match — scraped title contains an allowed title
    # or vice versa (handles minor differences)
    for norm_key, allowed in allowed_cache.items():
        if len(norm_key) >= 3 and (norm_key in normalized or normalized in norm_key):
            return (allowed.title, language)

    return None
