// Stable FAQ identifiers (for GA4's landing_faq_open {question_id} and for
// looking up each question/answer pair's i18n keys) — the copy itself lives
// in the locale files under landing.faq.items.<id>.{q,a} so it never drifts
// from what's on screen. Order here is render order.
export const FAQ_IDS = ['advice', 'howGenerated', 'performanceSource', 'dataSource', 'freePlan']

// schema.org FAQPage JSON-LD from the currently-rendered (already-translated)
// question/answer pairs — pure function, no i18n coupling, so it can't drift
// from what a visitor actually sees and is trivial to unit test.
export function generateFaqJsonLd(faqs) {
  return {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: faqs.map((f) => ({
      '@type': 'Question',
      name: f.q,
      acceptedAnswer: { '@type': 'Answer', text: f.a },
    })),
  }
}
