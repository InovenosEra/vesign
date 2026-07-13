import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getOpenTrades, unlockSignal } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useTickerModal } from '../TickerModalContext'
import { useMe } from '../../context/MeContext'
import { useLivePrices } from '../../hooks/useLivePrices'
import { isLocked, hasMoreLocked, fmtCents, lockedCount } from './gating'
import { logoCls, ymd } from './util'
import { FAKE_SIG } from './locked-fixtures'
import { UnlockAllButton, ConfirmUnlockDialog } from './UnlockAll'

const PAGE_SIZE = 10
const FREE_PREVIEW = 10           // free users see the top-10 by yield, yield-only, no pager
const OPEN_UNLOCK_ALL_CENTS = 100 // flat $1 to unlock ALL open trades, permanently — mirrors backend ent.OPEN_UNLOCK_ALL_CENTS
const COLSPAN = 10

// Market cap in billions, 1 decimal (matches ve-sign.com's open-trades column).
const mcapB = (mc) => (mc == null ? '—' : (mc / 1e9).toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 }))

// Sortable header cell — click to sort, ▲/▼ shows the active column + direction.
function Th({ label, col, sort, onSort, className, style }) {
  const active = sort?.key === col
  return (
    <th onClick={() => onSort(col)} className={'sortable' + (className ? ' ' + className : '')} style={style}>
      {label}{active ? <span className="sort-ar">{sort.dir === 'asc' ? '▲' : '▼'}</span> : null}
    </th>
  )
}

// Fixed column widths so the table doesn't reflow width-per-column when paging
// (auto layout was resizing to each page's longest content, e.g. company names).
function OtColGroup() {
  return (
    <colgroup>
      <col style={{ width: '3%' }} />
      <col style={{ width: '6%' }} />
      <col style={{ width: '19%' }} />
      <col style={{ width: '9%' }} />
      <col style={{ width: '9%' }} />
      <col style={{ width: '9%' }} />
      <col style={{ width: '13%' }} />
      <col style={{ width: '8%' }} />
      <col style={{ width: '12%' }} />
      <col style={{ width: '12%' }} />
    </colgroup>
  )
}

// Columns mirror ve-sign.com (TradesPage open-trades), in the same order:
// logo · Ticker · Company · Market Cap · Buy date · Buy price · Last day price ·
// Days held · Live price (phase-aware) · Yield. Live price is the only non-sortable
// data column (it's client-side live, not in the row array).
function HeadRow({ phase, sort, onSort }) {
  const liveLabel = phase === 'pre' ? 'Pre-market' : phase === 'post' ? 'Post-market' : 'Live price'
  const H = (label, col, className, style) => onSort
    ? <Th label={label} col={col} sort={sort} onSort={onSort} className={className} style={style} />
    : <th className={className} style={style}>{label}</th>
  return (
    <tr>
      <th></th>
      {H('Ticker', 'ticker')}
      {H('Company', 'company')}
      {H('Market Cap (B)', 'market_cap', 'r')}
      {H('Buy date', 'buy_date', 'r')}
      {H('Buy price', 'buy_price', 'r')}
      {H('Last day price', 'current_price', 'r')}
      {H('Days held', 'days_held', 'r')}
      <th className="r">{liveLabel}</th>
      {H('Yield', 'unrealized_pct', 'r', { paddingRight: 18 })}
    </tr>
  )
}

// Phase-aware live price: last close while loading/closed, else the live quote
// with its move vs the prior close.
function LiveCell({ p, live, phase }) {
  const close = p.current_price
  if (phase === 'idle') return <span className="muted" style={{ fontSize: 12 }}>Closed</span>
  if (phase == null || live == null) return <span className="muted">{close == null ? '—' : num(close)}</span>
  const diff = close != null ? live - close : null
  const pv = diff != null && close ? (diff / close) * 100 : null
  return (
    <div>
      <div>{num(live)}</div>
      {diff != null && <div className={dirClass(diff)} style={{ fontSize: 11 }}>{diff >= 0 ? '▲' : '▼'} {pv != null ? Math.abs(pv).toFixed(2) + '%' : ''}</div>}
    </div>
  )
}

function FullOpenRow({ p, live, phase }) {
  const open = useTickerModal()
  // Yield off the SAME live price the live column shows, so they stay consistent
  // (for a trade opened today, yield == the day's change). Fall back to the
  // server's value when there's no live price (market closed/loading).
  const liveYld = (phase && phase !== 'idle' && live != null && p.buy_price)
    ? (live - p.buy_price) / p.buy_price * 100
    : null
  const yld = liveYld != null ? liveYld : p.unrealized_pct
  return (
    <tr data-ticker={p.ticker} data-company={p.company || ''} onClick={() => open(p.ticker, p.company)}>
      <td><img className={logoCls(p.ticker)} src={LOGO(p.ticker)} alt={p.ticker} /></td>
      <td><span className="tk">{p.ticker}</span></td>
      <td><span className="co">{p.company || '—'}</span></td>
      <td className="r">{mcapB(p.market_cap)}</td>
      <td className="r muted">{ymd(p.buy_date)}</td>
      <td className="r">{p.buy_price == null ? '—' : num(p.buy_price)}</td>
      <td className="r">{p.current_price == null ? '—' : num(p.current_price)}</td>
      <td className="r muted">{p.days_held ?? '—'}</td>
      <td className="r"><LiveCell p={p} live={live} phase={phase} /></td>
      <td className={'r ' + dirClass(yld)} style={{ paddingRight: 18 }}><strong>{pct(yld)}</strong></td>
    </tr>
  )
}

// Frosted locked row: every column is fake data hazed via text-shadow (NOT
// filter:blur). Free's top-10 reveal the real Yield (sharp) as the teaser.
function BlurredOpenRow({ p, idx = 0 }) {
  const f = FAKE_SIG[idx % FAKE_SIG.length]
  const yld = p.reveal?.includes('yield') ? p.unrealized_pct : null
  const fakeEntry = (parseFloat(f.price) * 0.9).toFixed(2)
  const fakeDays = [12, 34, 7, 21, 45][idx % 5]
  const fakeYld = [18.4, 9.2, 31.7, 5.6, 22.1][idx % 5]
  const fakeMcap = ['12.4', '88.1', '5.6', '240.3', '31.0'][idx % 5]
  const fakeLive = (parseFloat(f.price) * 1.004).toFixed(2)
  return (
    <tr className="locked-row">
      <td><span className="logo-skel" aria-hidden="true" /></td>
      <td className="lock-haze" aria-hidden="true"><span className="tk">{f.tk}</span></td>
      <td className="lock-haze" aria-hidden="true"><span className="co">{f.co}</span></td>
      <td className="r lock-haze" aria-hidden="true">{fakeMcap}</td>
      <td className="r lock-haze" aria-hidden="true">2026-05-12</td>
      <td className="r lock-haze" aria-hidden="true">{fakeEntry}</td>
      <td className="r lock-haze" aria-hidden="true">{f.price}</td>
      <td className="r lock-haze" aria-hidden="true">{fakeDays}</td>
      <td className="r lock-haze" aria-hidden="true">{fakeLive}</td>
      {yld == null
        ? <td className="r lock-haze" aria-hidden="true" style={{ paddingRight: 18 }}><strong>+{fakeYld}%</strong></td>
        : <td className={'r ' + dirClass(yld)} style={{ paddingRight: 18 }}><strong>{pct(yld)}</strong></td>}
    </tr>
  )
}

export default function OpenTrades() {
  const me = useMe()
  const isFree = me.plan === 'free'
  const qc = useQueryClient()
  const [page, setPage] = useState(0)
  const [confirming, setConfirming] = useState(false)
  const [sort, setSort] = useState({ key: 'unrealized_pct', dir: 'desc' })
  // Sort is SERVER-side (it sorts all 649 rows before gating), so the unlocked
  // preview reflects the chosen order even though most rows are redacted. Flip
  // direction on the same column, else default to desc; reset to page 1.
  const toggle = (key) => {
    setSort(s => (s.key === key ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'desc' }))
    setPage(0)
  }
  const { data } = useQuery({
    queryKey: ['open-trades', 'US', sort.key, sort.dir],
    queryFn: () => getOpenTrades('US', false, sort.key, sort.dir),
  })
  const rows = Array.isArray(data) ? data : []          // already sorted by the server

  const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE))
  const safePage = Math.min(page, pages - 1)
  const slice = isFree
    ? rows.slice(0, FREE_PREVIEW)
    : rows.slice(safePage * PAGE_SIZE, safePage * PAGE_SIZE + PAGE_SIZE)
  // Live prices only for the unlocked rows on the current page (matches prod — no
  // 649-ticker fetch). Hooks run unconditionally (called before the free return).
  const pageTickers = slice.filter(p => !isLocked(p) && p.ticker).map(p => p.ticker)
  const { prices, phase } = useLivePrices(pageTickers)

  const count = Array.isArray(data) ? rows.length : '—'
  const header = (extra) => (
    <div className="section-h ot-head">
      <h2>Open trades <span className="sub" style={{ fontFamily: 'var(--mono)', marginLeft: 6 }}>{count}</span></h2>
      {extra}
    </div>
  )

  // Free: top-10 by yield, yield-only teaser, no pager, no CTA.
  if (isFree) {
    return (
      <>
        {header()}
        <table className="data-table fixed-table">
          <OtColGroup />
          <thead><HeadRow phase={phase} /></thead>
          <tbody>
            {slice.length
              ? slice.map((p, i) => <BlurredOpenRow key={i} p={p} idx={i} />)
              : <tr><td colSpan={COLSPAN} className="muted" style={{ textAlign: 'center', padding: 24 }}>No open positions.</td></tr>}
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
  // One flat-$2 bundle for ALL locked open trades; show it only when the current
  // page actually has a locked row (the first page is the open preview).
  const showSeeAll = me.plan === 'pro' && hasMoreLocked(rows) && slice.some(isLocked)

  return (
    <>
      {header(showSeeAll && (
        <UnlockAllButton price={OPEN_UNLOCK_ALL_CENTS} onClick={() => setConfirming(true)} label="Unlock all" />
      ))}
      {confirming && (() => {
        const n = lockedCount(rows)
        return (
          <ConfirmUnlockDialog
            title="Unlock all open trades?"
            body={<>This unlocks {n} locked open {n === 1 ? 'trade' : 'trades'} and charges{' '}
              <b>{fmtCents(OPEN_UNLOCK_ALL_CENTS)}</b> from your wallet.</>}
            price={OPEN_UNLOCK_ALL_CENTS}
            onConfirm={async () => { await unlockAll(); setConfirming(false) }}
            onCancel={() => setConfirming(false)}
          />
        )
      })()}
      <table className="data-table fixed-table">
        <OtColGroup />
        <thead><HeadRow phase={phase} sort={sort} onSort={toggle} /></thead>
        <tbody>
          {rows.length === 0
            ? <tr><td colSpan={COLSPAN} className="muted" style={{ textAlign: 'center', padding: 24 }}>No open positions.</td></tr>
            : slice.map((p, i) => isLocked(p)
              ? <BlurredOpenRow key={i} p={p} idx={safePage * PAGE_SIZE + i} />
              : <FullOpenRow key={i} p={p} live={prices[p.ticker]} phase={phase} />)}
        </tbody>
      </table>
      {pages > 1 && (
        <div className="pagination">
          <button disabled={safePage === 0} onClick={() => setPage(0)} aria-label="First page">«</button>
          <button disabled={safePage === 0} onClick={() => setPage(safePage - 1)}>‹ Prev</button>
          <span>{safePage + 1} / {pages}</span>
          <button disabled={safePage >= pages - 1} onClick={() => setPage(safePage + 1)}>Next ›</button>
          <button disabled={safePage >= pages - 1} onClick={() => setPage(pages - 1)} aria-label="Last page">»</button>
        </div>
      )}
    </>
  )
}
