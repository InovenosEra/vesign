/* Inline signal cards for the Signals page — one card per signal, BUY (green) or
 * SELL (red), each with metrics + the AI explanation inline.
 *  - SignalCard: unlocked → header (Current Price / Price Target / Health / 5D ML)
 *    + <SignalExplanation> + "Full analysis →" (opens the modal).
 *  - LockedSignalCard: locked → blurred card shape + unlock CTA. No data or model
 *    call is made for locked cards (placeholder text only). */
import { useState } from 'react'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { fmtCents } from './gating'
import { logoCls } from './util'
import { FAKE_SIG } from './locked-fixtures'
import SignalExplanation from '../SignalExplanation'

function healthDots(score) {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  return [0, 1, 2, 3, 4].map(i => <span key={i} className={'d' + (i < n ? '' : ' off')} />)
}

export function SignalCard({ s }) {
  const open = useTickerModal()
  const [expanded, setExpanded] = useState(false)
  const kind = (s.signal || '').toUpperCase() === 'SELL' ? 'sell' : 'buy'
  // Price target = current close grown by the analyst-upside fraction; the % is
  // that same upside (positive = below target, negative = above it).
  const upFrac = s.fair_value_upside
  const target = (s.close != null && upFrac != null) ? s.close * (1 + upFrac) : null
  const upPct = upFrac == null ? null : upFrac * 100
  const mlPct = s.prediction_score == null ? null : s.prediction_score * 100
  return (
    <div className={'sigcard ' + kind}>
      <div className="sc-head">
        <img className={'sc-logo ' + logoCls(s.ticker)} src={LOGO(s.ticker)} alt={s.ticker} />
        <div className="sc-id">
          <div className="trow"><span className="tk">{s.ticker}</span><span className={'pill ' + kind}>{kind.toUpperCase()}</span></div>
          <div className="co">{s.company || ''}</div>
        </div>
        <div className="sc-metrics">
          <div className="m"><div className="l">Current Price</div><div className="v num">{s.close == null ? '—' : '$' + num(s.close)}</div></div>
          <div className="m"><div className="l">Price Target</div><div className="v num">{target == null ? '—' : '$' + num(target)}</div><div className={'sub2 num ' + dirClass(upPct)}>{pct(upPct)}</div></div>
          <div className="m"><div className="l">Health</div><div className="v"><span className="health">{healthDots(s.health_score)}</span></div></div>
          <div className="m"><div className="l">5D ML</div><div className={'v num ' + dirClass(mlPct)}>{pct(mlPct)}</div></div>
        </div>
      </div>
      <div className="sc-why">
        <button className="sc-toggle" onClick={() => setExpanded(v => !v)}>
          {expanded
            ? <><span className="sc-caret">▲</span>Less details</>
            : <><span className="sc-caret">▼</span>More details</>}
        </button>
        <SignalExplanation ticker={s.ticker} collapsed={!expanded} />
        {expanded && (
          <div className="sc-foot">
            <button className="sc-more" onClick={() => open(s.ticker, s.company)}>Full analysis →</button>
          </div>
        )}
      </div>
    </div>
  )
}

export function LockedSignalCard({ s, kind, onUnlock, idx = 0 }) {
  const me = useMe()
  const f = FAKE_SIG[idx % FAKE_SIG.length]
  const k = (kind || '').toUpperCase() === 'SELL' ? 'sell' : 'buy'
  const fakeTarget = (parseFloat(f.price) * 1.1).toFixed(2)
  const canPayRow = s.reason === 'pay'
  return (
    <div className={'sigcard locked ' + k}>
      <div className="sc-head">
        <span className="sc-logo logo-skel" aria-hidden="true" />
        <div className="sc-id lock-blur" aria-hidden="true">
          <div className="trow"><span className="tk">{f.tk}</span></div>
          <div className="co">{f.co}</div>
        </div>
        <div className="sc-metrics lock-blur" aria-hidden="true">
          <div className="m"><div className="l">Current Price</div><div className="v num">${f.price}</div></div>
          <div className="m"><div className="l">Price Target</div><div className="v num">${fakeTarget}</div><div className="sub2 num up">{f.up}</div></div>
          <div className="m"><div className="l">Health</div><div className="v"><span className="health">{healthDots(f.h)}</span></div></div>
          <div className="m"><div className="l">5D ML</div><div className="v num up">{f.ml}</div></div>
        </div>
      </div>
      <div className="sc-cta">
        {canPayRow
          ? <button className="lock-pill" onClick={() => onUnlock(s)} title="Unlock this signal">🔓 Unlock · {fmtCents(s.unlock_price_cents ?? me.per_row_price_cents)}</button>
          : <span className="lock-pill">🔒 {s.reason === 'pay' ? 'See all' : 'Upgrade'}</span>}
      </div>
    </div>
  )
}
