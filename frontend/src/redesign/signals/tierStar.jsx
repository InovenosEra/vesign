/* BUY-signal quality-tier star (Prime / Strong / High Potential), shown on each
 * signal card next to the ticker, plus a legend in the page-head. The star
 * colours + glow live in App.css (.tier-star--prime/strong/potential). */
const TIER = {
  1: { cls: 'prime',     label: 'Prime' },          // purple, shining
  2: { cls: 'strong',    label: 'Strong' },          // gold
  3: { cls: 'potential', label: 'High Potential' },  // silver
}

export function TierStar({ tier, size = 14 }) {
  const t = TIER[tier]
  if (!t) return null
  return (
    <span aria-hidden="true" title={`${t.label} signal`}
      className={`tier-star tier-star--${t.cls}`} style={{ fontSize: size }}>★</span>
  )
}

export function TierLegend() {
  return (
    <div className="tier-legend">
      {[1, 2, 3].map((tr) => (
        <span key={tr} className="tl-item"><TierStar tier={tr} size={12} /> {TIER[tr].label}</span>
      ))}
    </div>
  )
}
