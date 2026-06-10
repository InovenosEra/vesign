/* Inline BUY signal cards for the Signals page.
 *  - BuySignalCard: unlocked → full card (metrics + AI explanation + open modal).
 *  - LockedBuyCard: locked → blurred card shape + unlock CTA. No data or model
 *    call is made for locked cards (placeholder text only). */
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { fmtCents } from './gating'
import { logoCls } from './util'
import SignalExplanation from '../SignalExplanation'
import { FAKE_SIG } from './locked-fixtures'

function healthDots(score) {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  return [0, 1, 2, 3, 4].map(i => <span key={i} className={'s' + (i < n ? '' : ' off')} />)
}

// Real metrics (unlocked card only). The locked card renders fake strings
// directly (below) — never through num()/pct(), which expect numbers.
function Metrics({ close, upside, mlPct, health }) {
  return (
    <div className="buy-card-metrics">
      <div className="m"><span className="l">Price</span><span className="v">{close == null ? '—' : num(close)}</span></div>
      <div className="m"><span className="l">Upside</span><span className={'v ' + dirClass(upside)}>{pct(upside)}</span></div>
      <div className="m"><span className="l">Health</span><span className="v health">{healthDots(health)}</span></div>
      <div className="m"><span className="l">ML</span><span className={'v ' + dirClass(mlPct)}>{pct(mlPct)}</span></div>
    </div>
  )
}

export function BuySignalCard({ s }) {
  const open = useTickerModal()
  const upside = s.fair_value_upside == null ? null : s.fair_value_upside * 100
  const mlPct = s.prediction_score == null ? null : s.prediction_score * 100
  return (
    <div className="buy-card">
      <div className="buy-card-head">
        <div className="ticker-cell">
          <img className={logoCls(s.ticker)} src={LOGO(s.ticker)} alt={s.ticker} />
          <span className="tk">{s.ticker}</span><span className="co">{s.company || ''}</span>
        </div>
        <Metrics close={s.close} upside={upside} mlPct={mlPct} health={s.health_score} />
      </div>
      <SignalExplanation ticker={s.ticker} />
      <button className="buy-card-more" onClick={() => open(s.ticker, s.company)}>Open full analysis →</button>
    </div>
  )
}

export function LockedBuyCard({ s, onUnlock, idx = 0 }) {
  const me = useMe()
  const f = FAKE_SIG[idx % FAKE_SIG.length]
  const canPayRow = s.reason === 'pay'
  return (
    <div className="buy-card locked">
      <div className="buy-card-head">
        <div className="ticker-cell lock-blur" aria-hidden="true">
          <span className="logo-skel" />
          <span className="tk">{f.tk}</span><span className="co">{f.co}</span>
        </div>
        <div className="buy-card-metrics lock-blur" aria-hidden="true">
          <div className="m"><span className="l">Price</span><span className="v">{f.price}</span></div>
          <div className="m"><span className="l">Upside</span><span className="v up">{f.up}</span></div>
          <div className="m"><span className="l">Health</span><span className="v health">{healthDots(f.h)}</span></div>
          <div className="m"><span className="l">ML</span><span className="v up">{f.ml}</span></div>
        </div>
      </div>
      <div className="sig-why lock-blur" aria-hidden="true">
        <p className="sig-why-headline">AI rationale: strong analyst upside and healthy fundamentals support this signal.</p>
        <ul className="sig-why-list str"><li>Placeholder strength one for blur</li><li>Placeholder strength two for blur</li></ul>
      </div>
      <div className="buy-card-cta">
        {canPayRow
          ? <button className="lock-pill" onClick={() => onUnlock(s)} title="Unlock this signal">🔓 Unlock · {fmtCents(s.unlock_price_cents ?? me.per_row_price_cents)}</button>
          : <span className="lock-pill">🔒 {s.reason === 'pay' ? 'See all' : 'Upgrade'}</span>}
      </div>
    </div>
  )
}
