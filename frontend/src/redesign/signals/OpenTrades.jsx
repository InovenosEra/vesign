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

// Decoy values for locked open rows — never real data (CSS-blurred). The Yield
// column is the exception: Free's top-10 rows reveal the real yield (sharp) as
// the teaser; deeper/locked rows show a lock glyph.
const FAKE_OPEN = [
  { tk: 'ABCD',  co: 'Holdings Inc',    bought: '12 May 26', entry: '142.30', price: '158.90', days: 14, pnl: '+$116' },
  { tk: 'ABC',   co: 'Capital Group',   bought: '04 May 26', entry: '88.10',  price: '94.50',  days: 22, pnl: '+$72' },
  { tk: 'ABCDE', co: 'Technologies',    bought: '28 Apr 26', entry: '245.30', price: '231.05', days: 29, pnl: '-$58' },
  { tk: 'ABCD',  co: 'Industries Ltd',  bought: '19 May 26', entry: '57.90',  price: '63.40',  days: 8,  pnl: '+$95' },
  { tk: 'ABCD',  co: 'Global Partners', bought: '07 May 26', entry: '134.05', price: '149.70', days: 19, pnl: '+$117' },
]

function LockedOpenRow({ p, idx = 0 }) {
  const f = FAKE_OPEN[idx % FAKE_OPEN.length]
  // Free: top-10 keep yield (reveal includes 'yield'); rest fully faded.
  const yld = p.reveal?.includes('yield') ? p.unrealized_pct : null
  return (
    <tr className="locked-row">
      <td>
        <div className="ticker-cell lock-blur" aria-hidden="true">
          <span className="logo-skel" />
          <span className="tk">{f.tk}</span><span className="co">{f.co}</span>
        </div>
      </td>
      <td className="r muted"><span className="lock-blur" aria-hidden="true">{f.bought}</span></td>
      <td className="r"><span className="lock-blur" aria-hidden="true">{f.entry}</span></td>
      <td className="r"><span className="lock-blur" aria-hidden="true">{f.price}</span></td>
      <td className="r muted"><span className="lock-blur" aria-hidden="true">{f.days}</span></td>
      <td className="r up"><span className="lock-blur" aria-hidden="true">{f.pnl}</span></td>
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
          ? <LockedOpenRow key={i} p={p} idx={i} />
          : <FullOpenRow key={i} p={p} />}
        emptyLabel="No open positions."
        colspan={7}
      />
    </>
  )
}
