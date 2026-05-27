/* BUY + SELL signal lists, side by side. Ported from the trades-v5.html
 * .signals-split block + its signalRow()/healthDots() renderers.
 * Data: getSignalsToday(signal,'US'). */
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getSignalsToday, unlockSignal } from '../../api'
import { num, pct, dirClass, dateFmt, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents } from './gating'
import { logoCls } from './util'
import PagedTable from './Pager'

function healthDots(score) {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  return [0, 1, 2, 3, 4].map(i => <span key={i} className={'s' + (i < n ? '' : ' off')} />)
}

const HEAD = (
  <tr>
    <th>Ticker</th>
    <th className="r">Price</th>
    <th className="r">Upside</th>
    <th className="r">Health</th>
    <th className="r">VQS</th>
    <th className="r" style={{ paddingRight: 18 }}>ML</th>
  </tr>
)

function LockedRow({ s, kind, onUnlock }) {
  const me = useMe()
  const canPay = s.reason === 'pay'
  const label = canPay && kind === 'BUY'
    ? `Unlock · ${fmtCents(s.unlock_price_cents ?? me.per_row_price_cents)}`
    : canPay ? 'Locked — see all below' : 'Upgrade to unlock'
  return (
    <tr className="locked-row">
      <td colSpan={6}>
        <div className="lock-cell">
          <span className="lock-veil" aria-hidden>████  ███████████</span>
          {canPay && kind === 'BUY' ? (
            <button className="lock-cta" onClick={() => onUnlock(s)}>{label}</button>
          ) : (
            <span className="lock-note">{label}</span>
          )}
        </div>
      </td>
    </tr>
  )
}

function FullRow({ s }) {
  const open = useTickerModal()
  const upside = s.fair_value_upside == null ? null : s.fair_value_upside * 100
  const mlPct = s.prediction_score == null ? null : s.prediction_score * 100
  return (
    <tr data-ticker={s.ticker} data-company={s.company || ''} onClick={() => open(s.ticker, s.company)}>
      <td><div className="ticker-cell">
        <img className={logoCls(s.ticker)} src={LOGO(s.ticker)} alt={s.ticker} />
        <span className="tk">{s.ticker}</span><span className="co">{s.company || ''}</span>
      </div></td>
      <td className="r">{s.close == null ? '—' : num(s.close)}</td>
      <td className={'r ' + dirClass(upside)}>{pct(upside)}</td>
      <td className="r"><span className="health">{healthDots(s.health_score)}</span></td>
      <td className="r"><span className="vqs-pill">{s.vqs ?? '—'}</span></td>
      <td className={'r ' + dirClass(mlPct)} style={{ paddingRight: 18 }}>{pct(mlPct)}</td>
    </tr>
  )
}

function SignalColumn({ kind }) {
  const isBuy = kind === 'BUY'
  const me = useMe()
  const qc = useQueryClient()
  const { data } = useQuery({
    queryKey: ['signals-today', kind, 'US'],
    queryFn: () => getSignalsToday(kind, 'US'),
    refetchInterval: 3_000,
  })
  const rows = Array.isArray(data) ? data : []

  async function unlockRow(s) {
    try {
      await unlockSignal({ kind: kind.toLowerCase(), scope: 'row', lock_token: s.lock_token, market: 'US' })
      qc.invalidateQueries({ queryKey: ['signals-today', kind, 'US'] })
      qc.invalidateQueries({ queryKey: ['me'] })
    } catch (e) {
      if (String(e.message).startsWith('402')) alert('Not enough wallet balance.')
    }
  }
  async function unlockAll() {
    try {
      await unlockSignal({ kind: kind.toLowerCase(), scope: 'all', market: 'US' })
      qc.invalidateQueries({ queryKey: ['signals-today', kind, 'US'] })
      qc.invalidateQueries({ queryKey: ['me'] })
    } catch (e) {
      if (String(e.message).startsWith('402')) alert('Not enough wallet balance.')
    }
  }

  const dateStr = rows.length ? dateFmt((rows[0].signal_date || rows[0].date || '').split(' ')[0]) : ''
  const sub = rows.length ? `${dateStr} · ${rows.length} ${rows.length === 1 ? 'signal' : 'signals'}` : '—'
  const showSeeAll = me.plan === 'pro' && hasMoreLocked(rows)

  return (
    <div>
      <div className="section-h" style={{ marginTop: 0 }}>
        <h2>
          <span className={'sig-tag ' + (isBuy ? 'buy' : 'sell')} style={{ marginRight: 8 }}>{kind}</span>
          {isBuy ? 'Buy signals' : 'Sell signals'}
        </h2>
        <span className="sub">{sub}</span>
        {showSeeAll && (
          <button className="see-all-cta" onClick={unlockAll}>
            {isBuy ? 'Unlock all today' : 'See all'} · {fmtCents(me.see_all_price_cents)}
          </button>
        )}
      </div>
      <PagedTable
        head={HEAD}
        rows={rows}
        row={(s, i) => isLocked(s)
          ? <LockedRow key={i} s={s} kind={kind} onUnlock={unlockRow} />
          : <FullRow key={i} s={s} />}
        emptyLabel={isBuy ? 'No buy signals today.' : 'No sell signals today.'}
        colspan={6}
      />
    </div>
  )
}

export default function SignalsSplit() {
  return (
    <div className="signals-split">
      <SignalColumn kind="BUY" />
      <SignalColumn kind="SELL" />
    </div>
  )
}
