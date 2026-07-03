/* "Vesign's read on this watchlist" — per-list analysis panel. Adapts the
 * VesignRead.jsx pattern (whole-portfolio) to a single watchlist's rows.
 * Signal mix + avg health are Vesign-model output (gated for Free, same rule
 * as every other page); near-target count is the user's own data and biggest
 * upside is analyst-derived — neither is ever gated. */
import { useMe } from '../../context/MeContext'
import { LOGO } from '../fmt'

export default function WatchlistRead({ card }) {
  const me = useMe()
  const modelLocked = me.plan !== 'pro' && me.plan !== 'max'
  const { signalMix, avgHealth, biggestUpside, nearTargetCount, tickerCount } = card
  const rated = signalMix.BUY + signalMix.HOLD + signalMix.SELL

  return (
    <div className="wl-read-wrap">
      <div className="wl-read">
        <div>
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
        </div>

        <div>
          <div className="lbl">Near target</div>
          <div className="val">{tickerCount === 0 ? '—' : `${nearTargetCount} of ${tickerCount}`}</div>
        </div>

        <div>
          <div className="lbl">Avg health</div>
          <div className="val">
            {modelLocked ? (
              <span className="wl-read-hazed">—/5</span>
            ) : avgHealth == null ? '—' : (
              <>{avgHealth.toFixed(1)}<span style={{ color: 'var(--ink-3)' }}>/5</span></>
            )}
          </div>
        </div>

        <div>
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
        <div className="wl-read-locked-overlay">
          <div className="msg">Signal + health analysis is a Pro feature</div>
          <div className="cta">Upgrade to unlock →</div>
        </div>
      )}
    </div>
  )
}
