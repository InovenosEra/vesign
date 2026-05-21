"""Resolve a usable PNG for a given ticker by trying multiple sources in order.

Pure HTTP — no filesystem writes, no DB writes. Caller is responsible for
persistence. Each source returns either bytes (the image) or None.
"""
from __future__ import annotations

from typing import Callable, Optional
from urllib.parse import urlparse

import requests

from utils.logo_overrides import LOGO_OVERRIDES

UA = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 8
MIN_BYTES = 256
LOGO_DEV_TOKEN = "pk_X-1ZO13GSgeOoUrIuJ6GMQ"


def _fetch(url: str) -> Optional[bytes]:
    """GET url, return bytes if response is a valid image >= MIN_BYTES, else None."""
    try:
        r = requests.get(url, timeout=TIMEOUT, headers=UA, allow_redirects=True)
    except Exception:
        return None
    if r.status_code != 200:
        return None
    ct = r.headers.get("content-type", "").lower()
    if not ct.startswith("image/"):
        return None
    if len(r.content) < MIN_BYTES:
        return None
    return r.content


def _domain_of(domain: Optional[str]) -> Optional[str]:
    if not domain:
        return None
    if "://" not in domain:
        domain = "https://" + domain
    host = urlparse(domain).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


# ── Source functions ────────────────────────────────────────────────────────

def from_override(ticker: str, _domain: Optional[str]) -> Optional[bytes]:
    url = LOGO_OVERRIDES.get(ticker)
    return _fetch(url) if url else None


def from_fmp(ticker: str, _domain: Optional[str]) -> Optional[bytes]:
    return _fetch(f"https://images.financialmodelingprep.com/symbol/{ticker}.png")


def from_parqet(ticker: str, _domain: Optional[str]) -> Optional[bytes]:
    return _fetch(f"https://assets.parqet.com/logos/symbol/{ticker}?format=png")


def from_logo_dev(ticker: str, _domain: Optional[str]) -> Optional[bytes]:
    return _fetch(f"https://img.logo.dev/ticker/{ticker}?token={LOGO_DEV_TOKEN}")


def from_google_favicon(_ticker: str, domain: Optional[str]) -> Optional[bytes]:
    domain = _domain_of(domain)
    if not domain:
        return None
    return _fetch(f"https://www.google.com/s2/favicons?domain={domain}&sz=128")


SOURCES: list[Callable[[str, Optional[str]], Optional[bytes]]] = [
    from_override,
    from_fmp,
    from_parqet,
    from_logo_dev,
    from_google_favicon,
]


def resolve(ticker: str, domain: Optional[str] = None) -> tuple[Optional[bytes], Optional[str]]:
    """Try each source in order. Return (bytes, source_name) on first hit, else (None, None)."""
    for src in SOURCES:
        data = src(ticker, domain)
        if data is not None:
            return data, src.__name__
    return None, None
