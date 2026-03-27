"""
Input sanitization utilities to prevent stored XSS and injection attacks.
Uses Python's built-in html.escape — no external dependencies needed.
"""
import html
import re


def sanitize_text(value: str | None) -> str | None:
    """Sanitize user-provided text by escaping HTML special characters.
    
    Returns None if input is None, otherwise returns escaped string.
    Preserves normal text content while neutralizing HTML/script payloads.
    """
    if value is None:
        return None
    return html.escape(value.strip(), quote=True)


def sanitize_filename(value: str | None) -> str | None:
    """Sanitize a filename by removing directory traversal characters
    and escaping HTML entities.
    
    Returns None if input is None.
    """
    if value is None:
        return None
    # Remove path separators to prevent directory traversal
    cleaned = re.sub(r'[/\\]', '', value.strip())
    return html.escape(cleaned, quote=True)
