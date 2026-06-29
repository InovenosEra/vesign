/* "Vesign's read on your holdings" — the differentiator. Layers the model's view
 * (signal mix, health, ML quality, biggest upside, watch-outs) onto the user's
 * own current positions. Pure reads from derive.js; no extra fetch. */
import { LOGO } from '../fmt'
import { signalMix, avgHealthWeighted, topUpside, weakestHealth } from './derive'

const HEALTH_COLOR = { 1: '#ff4d5c', 2: '#c2660c', 3: '#ff9500', 4: '#00d97e', 5: '#0a8f54' }
function HealthDots({ score }) {
  const n = score == null ? 0 : Math.max(0, Math.min(5, Math.round(score)))
  const c = HEALTH_COLOR[n] || '#6b7280'
  return (
    <span className="vr-dots">
      {[0, 1, 2, 3, 4].map(i => <span key={i} className={'vd' + (i < n ? '' : ' off')} style={i < n ? { background: c } : undefined} />)}
    </span>
  )
}

export default function VesignRead({ rows }) {
  const mix = signalMix(rows)
  const avgH = avgHealthWeighted(rows)
  const mlVals = rows.map(r => r.prediction_score).filter(v => v != null)
  const avgML = mlVals.length ? mlVals.reduce((s, v) => s + v, 0) / mlVals.length * 100 : null
  const up = topUpside(rows)
  const weak = weakestHealth(rows)
  const sells = rows.filter(r => r.signal === 'SELL').map(r => r.ticker)
  const rated = mix.BUY + mix.HOLD + mix.SELL

  return (
    <>
      <div className="section-h">
        <h2>Vesign's read on your holdings</h2>
        <span className="sub">the model's view of your current positions</span>
      </div>
      <div className="vr-panel">
        <div className="vr-cell">
          <div className="vr-lbl">Signal mix</div>
          <div className="vr-chips">
            {mix.BUY > 0 && <span className="vr-chip buy">{mix.BUY} BUY</span>}
            {mix.HOLD > 0 && <span className="vr-chip hold">{mix.HOLD} HOLD</span>}
            {mix.SELL > 0 && <span className="vr-chip sell">{mix.SELL} SELL</span>}
            {rated === 0 && <span className="vr-muted">No signals</span>}
          </div>
        </div>

        <div className="vr-cell">
          <div className="vr-lbl">Avg health</div>
          <div className="vr-val">
            {avgH == null ? '—' : <>{avgH.toFixed(1)}<span className="vr-of">/5</span> <HealthDots score={avgH} /></>}
          </div>
        </div>

        <div className="vr-cell">
          <div className="vr-lbl">Avg ML quality</div>
          <div className="vr-val">{avgML == null ? '—' : `${avgML.toFixed(0)}%`}</div>
        </div>

        <div className="vr-cell">
          <div className="vr-lbl">Biggest upside</div>
          <div className="vr-val">
            {up == null ? '—' : (
              <span className="vr-tk">
                <img className="logo-mini" src={LOGO(up.ticker)} alt={up.ticker} /> {up.ticker}
                <span className="up"> +{up.upside.toFixed(0)}%</span>
              </span>
            )}
          </div>
        </div>

        <div className="vr-cell">
          <div className="vr-lbl">Watch-outs</div>
          <div className="vr-val">
            {sells.length > 0
              ? <span className="down">SELL · {sells.join(', ')}</span>
              : weak ? <span><span className="vr-tk">{weak.ticker}</span> <span className="vr-muted">health {weak.health_score}/5</span></span>
              : '—'}
          </div>
        </div>
      </div>
    </>
  )
}
