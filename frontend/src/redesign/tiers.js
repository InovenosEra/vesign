/* Shared Free/Pro/Max tier copy — used by the landing pricing section and the
 * sign-up plan explanation. TODO: wire to backend — placeholder tiers/prices,
 * final pricing TBD.
 *
 * `id` (added for the landing page rebuild) is the only new field — the
 * landing Pricing section uses it to pull localized blurb/features/cta text
 * from the locale files (ld.pricing.tiers.<id>.*) while price/period/
 * featured stay sourced from here. SignUpPage.jsx reads name/price/period/
 * blurb/features/featured directly and is untouched — this stays the
 * single structure/price source for both. */
export const TIERS = [
  {
    id: 'free', name: 'Free', price: '$0', period: 'forever',
    blurb: 'See how the platform thinks.',
    features: ['Market overview & news', 'A sample of daily signals', 'Public track record', 'Basic research pages'],
    cta: 'Sign up free', featured: false,
  },
  {
    id: 'pro', name: 'Pro', price: '$19', period: '/ month',
    blurb: 'The full signal feed, explained.',
    features: ['Every BUY & SELL signal, same-day', 'Plain-language signal explanations', 'Portfolio tracking & alerts', 'Full research & screening tools'],
    cta: 'Start with Pro', featured: true,
  },
  {
    id: 'max', name: 'Max', price: '$49', period: '/ month',
    blurb: 'Everything, first.',
    features: ['Everything in Pro', 'Earliest signal access', 'Extended signal history & exports', 'Priority support'],
    cta: 'Go Max', featured: false,
  },
]
