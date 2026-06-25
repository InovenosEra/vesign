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

// `counts` (optional) keyed by tier {1,2,3} → renders the breakdown number before
// each label, e.g. "★ 3 Strong". `buys` (optional) keyed by tier → that tier is a
// buy BUTTON ("★ 3 Strong $0.60") that calls buys[tier].onUnlock on click. A tier
// with no count is dimmed and never a button.
export function TierLegend({ counts, buys }) {
  return (
    <div className="tier-legend">
      {[1, 2, 3].map((tr) => {
        const n = counts ? (counts[tr] ?? 0) : null
        const buy = n ? buys?.[tr] : null            // only sell a non-empty tier
        const inner = (
          <>
            <TierStar tier={tr} size={12} />
            {n != null && <b className="tl-n">{n}</b>}
            {TIER[tr].label}
            {buy && <span className="tl-price">{buy.price}</span>}
          </>
        )
        return buy ? (
          <button key={tr} type="button" className="tl-item tl-buy" onClick={buy.onUnlock}>
            {inner}
          </button>
        ) : (
          <span key={tr} className={'tl-item' + (n === 0 ? ' tl-zero' : '')}>{inner}</span>
        )
      })}
    </div>
  )
}
