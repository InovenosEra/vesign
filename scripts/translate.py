#!/usr/bin/env python3
"""
Auto-translate frontend/src/locales/en.json → he.json, es.json, fr.json
using the Anthropic Claude API.

Usage:
    python scripts/translate.py
"""
import json
import os
import sys
import anthropic

LANGS = {
    'he': 'Hebrew',
    'es': 'Spanish',
    'fr': 'French',
}

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EN_PATH = os.path.join(ROOT, 'frontend', 'src', 'locales', 'en.json')
LOCALES_DIR = os.path.join(ROOT, 'frontend', 'src', 'locales')


def main():
    with open(EN_PATH, encoding='utf-8') as f:
        en = json.load(f)

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print('ERROR: ANTHROPIC_API_KEY not set', file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    for code, lang_name in LANGS.items():
        print(f'Translating to {lang_name} ({code})…', flush=True)
        prompt = (
            f'Translate these UI strings from English to {lang_name}. '
            f'Rules:\n'
            f'- Keep {{{{var}}}} placeholders exactly as-is (e.g. {{{{count}}}}, {{{{countdown}}}}, {{{{total}}}})\n'
            f'- Keep "BUY", "SELL", "HOLD" in English (they are signal labels)\n'
            f'- Keep currency symbols ($, ₪) unchanged\n'
            f'- Keep ticker symbols unchanged\n'
            f'- Return ONLY valid JSON with the exact same keys, no extra text\n\n'
            f'{json.dumps(en, indent=2, ensure_ascii=False)}'
        )
        msg = client.messages.create(
            model='claude-opus-4-6',
            max_tokens=8096,
            messages=[{'role': 'user', 'content': prompt}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1]
            if raw.endswith('```'):
                raw = raw[:-3].rstrip()

        translated = json.loads(raw)

        out_path = os.path.join(LOCALES_DIR, f'{code}.json')
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(translated, f, ensure_ascii=False, indent=2)
        print(f'  ✓ Wrote {out_path}')

    print('Done.')


if __name__ == '__main__':
    main()
