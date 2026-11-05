"""
Utility functions for Operations Summarizer v2.
"""

import re


def strip_html_tags(text: str) -> str:
    """Remove HTML tags from text while preserving markdown formatting."""
    if not text:
        return text

    clean_text = re.sub(r'<[^>]+>', '', text)
    clean_text = clean_text.replace('&nbsp;', ' ')
    clean_text = clean_text.replace('&amp;', '&')
    clean_text = clean_text.replace('&lt;', '<')
    clean_text = clean_text.replace('&gt;', '>')
    clean_text = clean_text.replace('&quot;', '"')
    clean_text = clean_text.replace('&#39;', "'")
    clean_text = re.sub(r'[ \t]+', ' ', clean_text)
    clean_text = re.sub(r'\n\s*\n', '\n\n', clean_text)
    clean_text = clean_text.strip()

    return clean_text
