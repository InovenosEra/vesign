/* Inline signal cards for the Signals page — one card per signal, BUY (green) or
 * SELL (red). Cockpit layout: identity + the AI headline on top, a bottom
 * "cockpit" strip of indicators (Current Price / Price Target / 5D ML / Health)
 * with a "More details" button that drops the ✓/⚠ rationale + numbers below it.
 *  - SignalCard: unlocked, fully interactive.
 *  - LockedSignalCard: locked → blurred shape + unlock CTA, no data/model call. */
import { useState } from 'react'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { fmtCents } from './gating'
import { logoCls } from './util'
import { FAKE_SIG } from './locked-fixtures'
import SlideToUnlock from './SlideToUnlock'
import SignalExplanation from '../SignalExplanation'

// Health dot colour by 5-point score: 1 red → 2 dark-orange → 3 bright-orange
// → 4 bright-green → 5 dark-green. Empty dots stay grey (CSS).
const HEALTH_COLOR = { 1: '#ff4d5c', 2: '#c2660c', 3: '#ff9500', 4: '#00d97e', 5: '#0a8f54' }

function healthDots(score) {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  const c = HEALTH_COLOR[n] || '#6b7280'
  return (
    <span className="health">
      {[0, 1, 2, 3, 4].map(i => (
        <span key={i} className={'d' + (i < n ? '' : ' off')} style={i < n ? { background: c } : undefined} />
      ))}
    </span>
  )
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
  // Projected 5-day price = current close grown by the model's 5-day prediction.
  const mlPrice = (s.close != null && s.prediction_score != null) ? s.close * (1 + s.prediction_score) : null
  return (
    <div className={'sigcard ' + kind}>
      <div className="sc-head">
        <img className={'sc-logo ' + logoCls(s.ticker)} src={LOGO(s.ticker)} alt={s.ticker} />
        <div className="sc-id">
          <div className="trow"><span className="tk">{s.ticker}</span></div>
          <div className="co">{s.company || ''}</div>
        </div>
      </div>

      <SignalExplanation ticker={s.ticker} part="headline" />

      <div className="sc-cockpit">
        <div className="cell"><div className="l">Current Price</div><div className="v num">{s.close == null ? '—' : '$' + num(s.close)}</div></div>
        <div className="cell"><div className="l">Price Target</div><div className="v num">{target == null ? '—' : '$' + num(target)}</div><div className={'sub2 num ' + dirClass(upPct)}>{pct(upPct)}</div></div>
        <div className="cell"><div className="l">5D ML</div><div className="v num">{mlPrice == null ? '—' : '$' + num(mlPrice)}</div><div className={'sub2 num ' + dirClass(mlPct)}>{pct(mlPct)}</div></div>
        <div className="cell"><div className="l">Health</div>{healthDots(s.health_score)}</div>
        <div className="more-cell">
          <button className="more-btn" onClick={() => setExpanded(v => !v)}>
            <span className="caret">{expanded ? '▲' : '▼'}</span>{expanded ? 'Less details' : 'More details'}
          </button>
        </div>
      </div>

      {expanded && (
        <div className="sc-detail">
          <SignalExplanation ticker={s.ticker} part="body" />
          <div className="sc-full"><button className="sc-more" onClick={() => open(s.ticker, s.company)}>Full analysis →</button></div>
        </div>
      )}
    </div>
  )
}

export function LockedSignalCard({ s, kind, onUnlock, idx = 0, isFree = false }) {
  const me = useMe()
  const f = FAKE_SIG[idx % FAKE_SIG.length]
  const k = (kind || '').toUpperCase() === 'SELL' ? 'sell' : 'buy'
  const fakeTarget = (parseFloat(f.price) * 1.1).toFixed(2)
  const canPayRow = s.reason === 'pay'
  return (
    <div className={'sigcard locked ' + k}>
      <div className="sc-head">
        <span className="sc-logo logo-skel lock-blur" aria-hidden="true" />
        <div className="sc-id lock-blur" aria-hidden="true">
          <div className="trow"><span className="tk">{f.tk}</span></div>
          <div className="co">{f.co}</div>
        </div>
      </div>
      <div className="sig-why lock-blur" aria-hidden="true">
        <div className="sig-why-head">AI rationale available after unlock — strong analyst upside and healthy fundamentals.</div>
      </div>
      <div className="sc-cockpit lock-blur" aria-hidden="true">
        <div className="cell"><div className="l">Current Price</div><div className="v num">${f.price}</div></div>
        <div className="cell"><div className="l">Price Target</div><div className="v num">${fakeTarget}</div><div className="sub2 num up">{f.up}</div></div>
        <div className="cell"><div className="l">5D ML</div><div className="v num">${(parseFloat(f.price) * 1.01).toFixed(2)}</div><div className="sub2 num up">{f.ml}</div></div>
        <div className="cell"><div className="l">Health</div>{healthDots(f.h)}</div>
      </div>
      {/* Free users get a single page-level upgrade CTA (in SignalsSplit) instead
          of a per-card pill, so skip the card CTA here. Pro/Max keep the
          wallet pay-per-row unlock button. */}
      {!isFree && (
        <div className="sc-cta">
          {canPayRow
            ? <SlideToUnlock priceLabel={fmtCents(s.unlock_price_cents ?? me.per_row_price_cents)} onUnlock={() => onUnlock(s)} />
            : <span className="lock-pill"><svg className="lock-ico" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><rect x="4.5" y="11" width="15" height="9.5" rx="2" /><path d="M8 11V7.5a4 4 0 0 1 8 0V11" /></svg>{s.reason === 'pay' ? 'See all' : 'Upgrade'}</span>}
        </div>
      )}
    </div>
  )
}
