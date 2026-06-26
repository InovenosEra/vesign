/* BUY-signal quality tier as ascending "signal-strength" bars — Prime ▮▮▮ /
 * Strong ▮▮▯ / Promising ▮▯▯ — shown on each card next to the ticker (with the
 * tier name) and in the page-head legend. Bar/name colours live in signals.css
 * (.tier-bars--* / .tier-name--*), the violet/amber/sky triad. */
const TIER = {
  1: { cls: 'prime',     label: 'Prime' },      // violet, all 3 bars lit
  2: { cls: 'strong',    label: 'Strong' },     // amber, 2 lit
  3: { cls: 'potential', label: 'Promising' },  // sky, 1 lit
}

// Ascending 3-bar signal-strength glyph; CSS lights the bars by tier (fill from
// the short bar up, so more lit = higher conviction).
export function TierBars({ tier, size = 13 }) {
  const t = TIER[tier]
  if (!t) return null
  const w = Math.round((size * 16) / 14)   // keep the 16×14 viewBox aspect
  return (
    <svg aria-hidden="true" className={`tier-bars tier-bars--${t.cls}`}
      width={w} height={size} viewBox="0 0 16 14">
      <rect className="b1" x="1" y="9" width="3.4" height="5" rx="1" />
      <rect className="b2" x="6.3" y="5" width="3.4" height="9" rx="1" />
      <rect className="b3" x="11.6" y="1" width="3.4" height="13" rx="1" />
    </svg>
  )
}

// Bars + the coloured tier name, for use on a card next to the ticker.
export function TierMark({ tier }) {
  const t = TIER[tier]
  if (!t) return null
  return (
    <span className="tier-mark">
      <TierBars tier={tier} size={13} />
      <span className={`tier-name tier-name--${t.cls}`}>{t.label}</span>
    </span>
  )
}

// `counts` (optional) keyed by tier {1,2,3} → renders the breakdown number before
// each label, e.g. "▮▮▯ 3 Strong". `buys` (optional) keyed by tier → that tier is
// a buy BUTTON that calls buys[tier].onUnlock. A tier with no count is dimmed and
// never a button.
export function TierLegend({ counts, buys }) {
  return (
    <div className="tier-legend">
      {[1, 2, 3].map((tr) => {
        const n = counts ? (counts[tr] ?? 0) : null
        const buy = n ? buys?.[tr] : null            // only sell a non-empty tier
        const inner = (
          <>
            <TierBars tier={tr} size={12} />
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
