import { Trans } from 'react-i18next'
import { useReveal, useSectionView } from './hooks'

/* ── Statement: a single full-bleed typographic beat between Proof and
 * Platform — no stats, no CTA, just one line at hero scale. Purely a pacing
 * device (the way Stripe/Linear punctuate long pages with a big-type
 * interstitial) so the page doesn't read as an unbroken chain of card-grid
 * sections. Reveals once via the shared [data-reveal] recipe, then sits
 * static — no infinite motion. */
export function Statement() {
  const [revealRef, visible] = useReveal({ threshold: 0.4 })
  const gaRef = useSectionView('statement')
  return (
    <section
      className={'ld-statement' + (visible ? ' visible' : '')}
      ref={(el) => { revealRef.current = el; gaRef.current = el }}
    >
      <p>
        <Trans i18nKey="ld.statement.text" components={{ g: <span className="g" /> }} />
      </p>
    </section>
  )
}
