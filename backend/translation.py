"""On-demand translation cache for short user-facing texts.

Used to translate values that come from external sources (news headlines from
FMP) or are stored as English-only in the DB (company health reasons) so the
frontend can render them in the user's selected language.

Design:
- Lazy: nothing is translated until an endpoint asks for it.
- Cached in process memory by (lang_code, hash(text)). No eviction — short
  texts; cache stays small. uvicorn restart clears it.
- Falls back to the original English text if the API call fails, so a
  Claude outage never breaks the page.
- Uses Claude Haiku (fast + cheap) since translation of short factual text
  doesn't need the larger models.
"""
import os
import re
from typing import Optional

from anthropic import Anthropic

LANG_NAMES = {
    "en": "English",
    "he": "Hebrew",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
}

_cache: dict[tuple[str, int], str] = {}
_client: Optional[Anthropic] = None
_MODEL = "claude-haiku-4-5-20251001"


def _client_singleton() -> Anthropic:
    global _client
    if _client is None:
        _client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


def _is_passthrough(target_lang: Optional[str]) -> bool:
    """English (or unknown lang) → return original text without API call."""
    if not target_lang:
        return True
    code = target_lang.lower().split("-")[0]  # 'en-US' → 'en'
    name = LANG_NAMES.get(code)
    return not name or name == "English"


def translate(text: str, target_lang: Optional[str]) -> str:
    """Translate a single short string. Returns original on en/error."""
    if not text or _is_passthrough(target_lang):
        return text or ""
    code = target_lang.lower().split("-")[0]
    lang_name = LANG_NAMES.get(code, "English")

    key = (code, hash(text))
    if key in _cache:
        return _cache[key]

    try:
        msg = _client_singleton().messages.create(
            model=_MODEL,
            max_tokens=1500,
            messages=[{
                "role": "user",
                "content": (
                    f"Translate the following text to {lang_name}. "
                    f"Keep ticker symbols (e.g. AAPL, NVDA), currency symbols ($, €), "
                    f"numbers, percentages, and company names unchanged. "
                    f"Use natural {lang_name} phrasing. Output ONLY the translation, "
                    f"no preamble, no quotes, no notes.\n\n{text}"
                ),
            }],
        )
        out = msg.content[0].text.strip()
        # Defensive: strip surrounding quotes that some models add
        if len(out) >= 2 and out[0] in '"\'' and out[-1] == out[0]:
            out = out[1:-1].strip()
        _cache[key] = out
        return out
    except Exception:
        return text


def translate_batch(texts: list[str], target_lang: Optional[str]) -> list[str]:
    """Translate a list of strings in one Claude call. Cached per-item."""
    if not texts or _is_passthrough(target_lang):
        return list(texts)
    code = target_lang.lower().split("-")[0]
    lang_name = LANG_NAMES.get(code, "English")

    # Resolve from cache; collect what's left to translate
    out: list[Optional[str]] = []
    pending: list[tuple[int, str]] = []
    for i, t in enumerate(texts):
        if not t:
            out.append(t)
            continue
        key = (code, hash(t))
        if key in _cache:
            out.append(_cache[key])
        else:
            out.append(None)
            pending.append((i, t))

    if not pending:
        return [x or "" for x in out]

    # Build a numbered prompt — easy for Claude to return the same shape
    numbered = "\n".join(f"{i+1}. {t}" for i, (_, t) in enumerate(pending))
    try:
        msg = _client_singleton().messages.create(
            model=_MODEL,
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": (
                    f"Translate each numbered line to {lang_name}. "
                    f"Keep ticker symbols, currency symbols, numbers, percentages, "
                    f"and company names unchanged. Output ONLY the same numbered list "
                    f"in {lang_name}, one item per line, nothing else.\n\n{numbered}"
                ),
            }],
        )
        raw = msg.content[0].text.strip()
        parsed: dict[int, str] = {}
        for line in raw.split("\n"):
            m = re.match(r"\s*(\d+)[.\)]\s*(.*)", line)
            if m:
                idx = int(m.group(1)) - 1
                if 0 <= idx < len(pending):
                    parsed[idx] = m.group(2).strip()

        for batch_i, (orig_i, orig_text) in enumerate(pending):
            translated = parsed.get(batch_i, orig_text)
            _cache[(code, hash(orig_text))] = translated
            out[orig_i] = translated
    except Exception:
        # Fall back to originals
        for orig_i, orig_text in pending:
            out[orig_i] = orig_text

    return [x or "" for x in out]
