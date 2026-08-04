/* "Vesign's read on this watchlist" — per-list analysis panel. Adapts the
 * VesignRead.jsx pattern (whole-portfolio) to a single watchlist's rows.
 * Signal mix + avg health are Vesign-model output (gated for Free, same rule
 * as every other page); near-target count is the user's own data and biggest
 * upside is analyst-derived — neither is ever gated. Each gated cell carries
 * its own small lock scrim (never the whole 4-cell panel); the "Upgrade" CTA
 * renders once, below the grid, so it can never cover the ungated cells. */
import { useMe } from '../../context/MeContext'
import { LOGO } from '../fmt'

const LockGlyph = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4"
    strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <rect x="4.5" y="11" width="15" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" />
  </svg>
)

export default function WatchlistRead({ card }) {
  const me = useMe()
  const modelLocked = me.plan !== 'max'
  const { signalMix, avgHealth, biggestUpside, nearTargetCount, tickerCount } = card
  const rated = signalMix.BUY + signalMix.HOLD + signalMix.SELL

  return (
    <div className="wl-read-wrap">
      <div className="wl-read">
        <div className="wl-read-cell">
          <div className="lbl">Signal mix</div>
          <div className="chips">
            {modelLocked ? (
              <span className="chip hold wl-read-hazed">— BUY</span>
            ) : (
              <>
                {signalMix.BUY > 0 && <span className="chip buy">{signalMix.BUY} BUY</span>}
                {signalMix.HOLD > 0 && <span className="chip hold">{signalMix.HOLD} HOLD</span>}
                {signalMix.SELL > 0 && <span className="chip sell">{signalMix.SELL} SELL</span>}
                {rated === 0 && <span className="val">No signals</span>}
              </>
            )}
          </div>
          {modelLocked && <div className="wl-read-cell-scrim" aria-hidden="true"><LockGlyph /></div>}
        </div>

        <div className="wl-read-cell">
          <div className="lbl">Near target</div>
          <div className="val">{tickerCount === 0 ? '—' : `${nearTargetCount} of ${tickerCount}`}</div>
        </div>

        <div className="wl-read-cell">
          <div className="lbl">Avg health</div>
          <div className="val">
            {modelLocked ? (
              <span className="wl-read-hazed">—/5</span>
            ) : avgHealth == null ? '—' : (
              <>{avgHealth.toFixed(1)}<span style={{ color: 'var(--ink-3)' }}>/5</span></>
            )}
          </div>
          {modelLocked && <div className="wl-read-cell-scrim" aria-hidden="true"><LockGlyph /></div>}
        </div>

        <div className="wl-read-cell">
          <div className="lbl">Biggest upside</div>
          <div className="val">
            {biggestUpside == null ? '—' : (
              <>
                <img className="logo-mini" src={LOGO(biggestUpside.ticker)} alt={biggestUpside.ticker}
                  style={{ width: 14, height: 14, borderRadius: 3, verticalAlign: 'middle', marginRight: 4 }} />
                {biggestUpside.ticker} <span style={{ color: 'var(--green)' }}>+{biggestUpside.upside.toFixed(0)}%</span>
              </>
            )}
          </div>
        </div>
      </div>

      {modelLocked && (
        <div className="wl-read-locked-strip">
          <span className="msg">Signal + health analysis is a Max feature</span>
          <span className="cta">Upgrade to unlock →</span>
        </div>
      )}
    </div>
  )
}
