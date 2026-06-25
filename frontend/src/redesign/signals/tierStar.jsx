/* BUY-signal quality-tier star (Prime / Strong / Promising), shown on each
 * signal card next to the ticker, plus a legend in the page-head. The star
 * colours + glow live in App.css (.tier-star--prime/strong/potential). */
const TIER = {
  1: { cls: 'prime',     label: 'Prime' },          // purple, shining
  2: { cls: 'strong',    label: 'Strong' },          // gold
  3: { cls: 'potential', label: 'Promising' },        // silver
}

export function TierStar({ tier, size = 14 }) {
  const t = TIER[tier]
  if (!t) return null
  return (
    <span aria-hidden="true" title={`${t.label} signal`}
      className={`tier-star tier-star--${t.cls}`} style={{ fontSize: size }}>★</span>
  )
}

// `counts` (optional) keyed by tier number {1,2,3} → renders the breakdown number
// before each label, e.g. "★ 3 Strong". Zero-count tiers are dimmed.
export function TierLegend({ counts }) {
  return (
    <div className="tier-legend">
      {[1, 2, 3].map((tr) => {
        const n = counts ? (counts[tr] ?? 0) : null
        return (
          <span key={tr} className={'tl-item' + (n === 0 ? ' tl-zero' : '')}>
            <TierStar tier={tr} size={12} />
            {n != null && <b className="tl-n">{n}</b>}
            {TIER[tr].label}
          </span>
        )
      })}
    </div>
  )
}
