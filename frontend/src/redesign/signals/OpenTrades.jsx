import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getOpenTrades, unlockSignal } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents, lockedCount } from './gating'
import { logoCls, ymd } from './util'
import { FAKE_SIG } from './locked-fixtures'
import { UnlockAllButton, ConfirmUnlockDialog } from './UnlockAll'
import PagedTable from './Pager'

const FREE_PREVIEW = 10           // free users see the top-10 by yield, yield-only, no pager
const OPEN_UNLOCK_ALL_CENTS = 200 // flat $2 to unlock ALL open trades — mirrors backend ent.OPEN_UNLOCK_ALL_CENTS

const HEAD = (
  <tr>
    <th>Ticker</th><th className="r">Bought</th><th className="r">Entry</th>
    <th className="r">Price</th><th className="r">Days held</th>
    <th className="r">Unrealised P&amp;L</th>
    <th className="r" style={{ paddingRight: 18 }}>Yield</th>
  </tr>
)

// Frosted locked row: every column is fake data hazed via text-shadow (NOT
// filter:blur, which smears the page background in Chrome). aria-hidden + fake
// values — nothing identifying. Free's top-10 reveal the real Yield (sharp) as
// the teaser; Pro's locked rows haze the Yield too (no revealed value).
function BlurredOpenRow({ p, idx = 0 }) {
  const f = FAKE_SIG[idx % FAKE_SIG.length]
  const yld = p.reveal?.includes('yield') ? p.unrealized_pct : null   // server reveals yield for the top teaser rows
  const fakeEntry = (parseFloat(f.price) * 0.9).toFixed(2)
  const fakeDays = [12, 34, 7, 21, 45][idx % 5]
  const fakeYld = [18.4, 9.2, 31.7, 5.6, 22.1][idx % 5]   // hazed when the row has no revealed yield (Pro)
  return (
    <tr className="locked-row">
      <td><div className="ticker-cell lock-haze" aria-hidden="true">
        <span className="logo-skel" />
        <span className="tk">{f.tk}</span><span className="co">{f.co}</span>
      </div></td>
      <td className="r lock-haze" aria-hidden="true">2026-05-12</td>
      <td className="r lock-haze" aria-hidden="true">{fakeEntry}</td>
      <td className="r lock-haze" aria-hidden="true">{f.price}</td>
      <td className="r lock-haze" aria-hidden="true">{fakeDays}</td>
      <td className="r lock-haze" aria-hidden="true">+$120</td>
      {yld == null
        ? <td className="r lock-haze" aria-hidden="true" style={{ paddingRight: 18 }}><strong>+{fakeYld}%</strong></td>
        : <td className={'r ' + dirClass(yld)} style={{ paddingRight: 18 }}><strong>{pct(yld)}</strong></td>}
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
  const isFree = me.plan === 'free'
  const qc = useQueryClient()
  const [confirming, setConfirming] = useState(false)
  const { data } = useQuery({ queryKey: ['open-trades', 'US'], queryFn: () => getOpenTrades('US') })
  const rows = Array.isArray(data) ? data : []          // server-sorted by yield desc

  // Free: top-10 by yield, yield-only teaser, no pager, no per-row CTA.
  if (isFree) {
    const shown = rows.slice(0, FREE_PREVIEW)
    return (
      <>
        <div className="section-h ot-head">
          <h2>Open trades <span className="sub" style={{ fontFamily: 'var(--mono)', marginLeft: 6 }}>{Array.isArray(data) ? rows.length : '—'}</span></h2>
        </div>
        <table className="data-table">
          <thead>{HEAD}</thead>
          <tbody>
            {shown.length
              ? shown.map((p, i) => <BlurredOpenRow key={i} p={p} idx={i} />)
              : <tr><td colSpan={7} className="muted" style={{ textAlign: 'center', padding: 24 }}>No open positions.</td></tr>}
          </tbody>
        </table>
      </>
    )
  }

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
        {/* Same compact button + confirm as the BUY/SELL section heads. One flat-$2
            bundle for ALL locked open trades (no per-row unlock); $2.00 must match
            backend ent.OPEN_UNLOCK_ALL_CENTS. */}
        {showSeeAll && (
          <UnlockAllButton
            price={OPEN_UNLOCK_ALL_CENTS}
            onClick={() => setConfirming(true)}
            label="Unlock all"
          />
        )}
      </div>
      {confirming && (() => {
        const count = lockedCount(rows)
        return (
          <ConfirmUnlockDialog
            title="Unlock all open trades?"
            body={<>This unlocks {count} locked open {count === 1 ? 'trade' : 'trades'} and charges{' '}
              <b>{fmtCents(OPEN_UNLOCK_ALL_CENTS)}</b> from your wallet.</>}
            price={OPEN_UNLOCK_ALL_CENTS}
            onConfirm={async () => { await unlockAll(); setConfirming(false) }}
            onCancel={() => setConfirming(false)}
          />
        )
      })()}
      <PagedTable
        head={HEAD}
        rows={rows}
        row={(p, i) => isLocked(p)
          ? <BlurredOpenRow key={i} p={p} idx={i} />
          : <FullOpenRow key={i} p={p} />}
        emptyLabel="No open positions."
        colspan={7}
      />
    </>
  )
}
