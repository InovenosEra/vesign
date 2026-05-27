import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getOpenTrades, unlockSignal } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents } from './gating'
import { logoCls, ymd } from './util'
import PagedTable from './Pager'

const HEAD = (
  <tr>
    <th>Ticker</th><th className="r">Bought</th><th className="r">Entry</th>
    <th className="r">Price</th><th className="r">Days held</th>
    <th className="r">Unrealised P&amp;L</th>
    <th className="r" style={{ paddingRight: 18 }}>Yield</th>
  </tr>
)

// Locked open row: ONLY the Yield is ever shown. Every other column is hidden
// (no decoy, no blur) — Free's top-10 reveal the real yield (sharp) as the
// teaser; deeper/locked rows show a lock glyph. Nothing identifying is rendered.
function LockedOpenRow({ p }) {
  const yld = p.reveal?.includes('yield') ? p.unrealized_pct : null
  return (
    <tr className="locked-row">
      <td><span className="lock-pill">🔒 Locked</span></td>
      <td className="r" />
      <td className="r" />
      <td className="r" />
      <td className="r" />
      <td className="r" />
      <td className={'r ' + (yld != null ? dirClass(yld) : '')} style={{ paddingRight: 18 }}>
        {yld == null ? <span className="lock-pill">🔒</span> : <strong>{pct(yld)}</strong>}
      </td>
    </tr>
  )
}

function FullOpenRow({ p }) {
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()
  const yld = p.unrealized_pct
  const dollarPnL = p.buy_price && p.current_price
    ? (p.current_price - p.buy_price) * 1000 / p.buy_price : null
  return (
    <tr data-ticker={p.ticker} data-company={p.company || ''} onClick={() => open(p.ticker, p.company)}>
      <td><div className="ticker-cell">
        <img className={logoCls(p.ticker)} src={LOGO(p.ticker)} alt={p.ticker} />
        <span className="tk">{p.ticker}</span><span className="co">{p.company || ''}</span>
      </div></td>
      <td className="r muted">{ymd(p.buy_date)}</td>
      <td className="r">{p.buy_price == null ? '—' : num(p.buy_price)}</td>
      <td className="r">{p.current_price == null ? '—' : num(p.current_price)}</td>
      <td className="r muted">{p.days_held ?? '—'}</td>
      <td className={'r ' + dirClass(dollarPnL)}>
        {dollarPnL == null ? '—' : (dollarPnL >= 0 ? '+' : '-') + fmtPrice(Math.abs(dollarPnL), 0)}
      </td>
      <td className={'r ' + dirClass(yld)} style={{ paddingRight: 18 }}><strong>{pct(yld)}</strong></td>
    </tr>
  )
}

export default function OpenTrades() {
  const me = useMe()
  const qc = useQueryClient()
  const { data } = useQuery({ queryKey: ['open-trades', 'US'], queryFn: () => getOpenTrades('US') })
  const rows = Array.isArray(data) ? data : []          // server-sorted by yield desc

  async function unlockAll() {
    try {
      await unlockSignal({ kind: 'open', scope: 'all', market: 'US' })
      qc.invalidateQueries({ queryKey: ['open-trades', 'US'] })
      qc.invalidateQueries({ queryKey: ['me'] })
    } catch (e) {
      if (String(e.message).startsWith('402')) alert('Not enough wallet balance.')
    }
  }
  const showSeeAll = me.plan === 'pro' && hasMoreLocked(rows)

  return (
    <>
      <div className="section-h">
        <h2>Open trades <span className="sub" style={{ fontFamily: 'var(--mono)', marginLeft: 6 }}>{Array.isArray(data) ? rows.length : '—'}</span></h2>
        {showSeeAll && (
          <button className="see-all-cta" onClick={unlockAll}>See all · {fmtCents(me.see_all_price_cents)}</button>
        )}
      </div>
      <PagedTable
        head={HEAD}
        rows={rows}
        row={(p, i) => isLocked(p)
          ? <LockedOpenRow key={i} p={p} />
          : <FullOpenRow key={i} p={p} />}
        emptyLabel="No open positions."
        colspan={7}
      />
    </>
  )
}
