/* Shared Free/Pro/Max tier copy — used by the landing pricing section and the
 * sign-up plan explanation. TODO: wire to backend — placeholder tiers/prices,
 * final pricing TBD. */
export const TIERS = [
  {
    name: 'Free', price: '$0', period: 'forever',
    blurb: 'See how the platform thinks.',
    features: ['Market overview & news', 'A sample of daily signals', 'Public track record', 'Basic research pages'],
    cta: 'Sign up free', featured: false,
  },
  {
    name: 'Pro', price: '$19', period: '/ month',
    blurb: 'The full signal feed, explained.',
    features: ['Every BUY & SELL signal, same-day', 'Plain-language signal explanations', 'Portfolio tracking & alerts', 'Full research & screening tools'],
    cta: 'Start with Pro', featured: true,
  },
  {
    name: 'Max', price: '$49', period: '/ month',
    blurb: 'Everything, first.',
    features: ['Everything in Pro', 'Earliest signal access', 'Extended signal history & exports', 'Priority support'],
    cta: 'Go Max', featured: false,
  },
]
