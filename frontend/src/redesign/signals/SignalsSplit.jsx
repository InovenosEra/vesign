/* Signals page body: BUY signals as full-width cards (metrics + inline AI
 * explanation), stacked above the compact SELL table. Data: getSignalsToday. */
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getSignalsToday, unlockSignal } from '../../api'
import { num, pct, dirClass, dateFmt, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents } from './gating'
import { logoCls } from './util'
import PagedTable from './Pager'
import { BuySignalCard, LockedBuyCard } from './BuySignalCard'
import { FAKE_SIG } from './locked-fixtures'

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
    <th className="r" style={{ paddingRight: 18 }}>ML</th>
  </tr>
)

function LockedRow({ s, kind, onUnlock, idx = 0 }) {
  const me = useMe()
  const f = FAKE_SIG[idx % FAKE_SIG.length]
  const canPayRow = s.reason === 'pay' && kind === 'BUY'
  return (
    <tr className="locked-row">
      <td><div className="ticker-cell lock-blur" aria-hidden="true">
        <span className="logo-skel" /><span className="tk">{f.tk}</span><span className="co">{f.co}</span>
      </div></td>
      <td className="r"><span className="lock-blur" aria-hidden="true">{f.price}</span></td>
      <td className="r up"><span className="lock-blur" aria-hidden="true">{f.up}</span></td>
      <td className="r"><span className="health lock-blur" aria-hidden="true">{healthDots(f.h)}</span></td>
      <td className="r" style={{ paddingRight: 18 }}>
        {canPayRow
          ? <button className="lock-pill" onClick={() => onUnlock(s)} title="Unlock this signal">🔓 {fmtCents(s.unlock_price_cents ?? me.per_row_price_cents)}</button>
          : <span className="lock-pill">🔒 {s.reason === 'pay' ? 'See all' : 'Upgrade'}</span>}
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
      <td className={'r ' + dirClass(mlPct)} style={{ paddingRight: 18 }}>{pct(mlPct)}</td>
    </tr>
  )
}

// Shared: query a side + provide unlock handlers + header bits.
function useSignalSection(kind) {
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
    } catch (e) { if (String(e.message).startsWith('402')) alert('Not enough wallet balance.') }
  }
  async function unlockAll() {
    try {
      await unlockSignal({ kind: kind.toLowerCase(), scope: 'all', market: 'US' })
      qc.invalidateQueries({ queryKey: ['signals-today', kind, 'US'] })
      qc.invalidateQueries({ queryKey: ['me'] })
    } catch (e) { if (String(e.message).startsWith('402')) alert('Not enough wallet balance.') }
  }
  const dateStr = rows.length ? dateFmt((rows[0].signal_date || rows[0].date || '').split(' ')[0]) : ''
  const sub = rows.length ? `${dateStr} · ${rows.length} ${rows.length === 1 ? 'signal' : 'signals'}` : '—'
  const showSeeAll = me.plan === 'pro' && hasMoreLocked(rows)
  return { me, rows, unlockRow, unlockAll, sub, showSeeAll }
}

function SectionHead({ kind, sub, showSeeAll, onSeeAll, seeAllPrice }) {
  const isBuy = kind === 'BUY'
  return (
    <div className="section-h" style={{ marginTop: 0 }}>
      <h2>
        <span className={'sig-tag ' + (isBuy ? 'buy' : 'sell')} style={{ marginRight: 8 }}>{kind}</span>
        {isBuy ? 'Buy signals' : 'Sell signals'}
      </h2>
      <span className="sub">{sub}</span>
      {showSeeAll && (
        <button className="see-all-cta" onClick={onSeeAll}>
          {isBuy ? 'Unlock all today' : 'See all'} · {fmtCents(seeAllPrice)}
        </button>
      )}
    </div>
  )
}

function BuySection() {
  const { me, rows, unlockRow, unlockAll, sub, showSeeAll } = useSignalSection('BUY')
  return (
    <div className="signals-section">
      <SectionHead kind="BUY" sub={sub} showSeeAll={showSeeAll} onSeeAll={unlockAll} seeAllPrice={me.see_all_price_cents} />
      {rows.length === 0
        ? <div className="signals-empty">No buy signals today.</div>
        : rows.map((s, i) => isLocked(s)
          ? <LockedBuyCard key={i} s={s} onUnlock={unlockRow} idx={i} />
          : <BuySignalCard key={s.ticker || i} s={s} />)}
    </div>
  )
}

function SellSection() {
  const { me, rows, unlockRow, unlockAll, sub, showSeeAll } = useSignalSection('SELL')
  return (
    <div className="signals-section">
      <SectionHead kind="SELL" sub={sub} showSeeAll={showSeeAll} onSeeAll={unlockAll} seeAllPrice={me.see_all_price_cents} />
      <PagedTable
        head={HEAD}
        rows={rows}
        row={(s, i) => isLocked(s)
          ? <LockedRow key={i} s={s} kind="SELL" onUnlock={unlockRow} idx={i} />
          : <FullRow key={i} s={s} />}
        emptyLabel="No sell signals today."
        colspan={5}
      />
    </div>
  )
}

export default function SignalsSplit() {
  return (
    <div className="signals-stack">
      <BuySection />
      <SellSection />
    </div>
  )
}
