/* Signals page body: two columns — BUY (left) and SELL (right). Every signal is
 * a card with metrics + the AI explanation inline (SignalCard). SELL is paginated
 * so only a page of explanations is fetched at a time. Data: getSignalsToday. */
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getSignalsToday, unlockSignal } from '../../api'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents, seeAllCents } from './gating'
import { SignalCard, LockedSignalCard } from './SignalCard'

const SELL_PAGE = 5
const FREE_PREVIEW = 4   // free users see a short locked teaser per column (no pager)

// Query one side + the unlock handlers + header counts.
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
      qc.invalidateQueries({ queryKey: ['me'] })
      // Defer the reveal so the locked card can fade out before it's replaced.
      setTimeout(() => qc.invalidateQueries({ queryKey: ['signals-today', kind, 'US'] }), 300)
      return true
    } catch (e) {
      if (String(e.message).startsWith('402')) alert('Not enough wallet balance.')
      return false
    }
  }
  async function unlockAll() {
    try {
      await unlockSignal({ kind: kind.toLowerCase(), scope: 'all', market: 'US' })
      qc.invalidateQueries({ queryKey: ['signals-today', kind, 'US'] })
      qc.invalidateQueries({ queryKey: ['me'] })
    } catch (e) { if (String(e.message).startsWith('402')) alert('Not enough wallet balance.') }
  }
  const sub = rows.length ? `${rows.length} ${rows.length === 1 ? 'signal' : 'signals'}` : '—'
  const showSeeAll = me.plan === 'pro' && hasMoreLocked(rows)
  const seeAllPrice = seeAllCents(rows.length, me.per_row_price_cents)
  return { me, rows, unlockRow, unlockAll, sub, showSeeAll, seeAllPrice }
}

function SectionHead({ kind, sub, showSeeAll, onSeeAll, seeAllPrice }) {
  const isBuy = kind === 'BUY'
  return (
    <div className="sig-sec-h">
      <span className={'tag ' + (isBuy ? 'buy' : 'sell')}>{kind}</span>
      <span className="sub">{sub}</span>
      {showSeeAll && (
        <button className="see-all-cta" onClick={onSeeAll}>
          {isBuy ? 'Unlock all today' : 'See all'} · {fmtCents(seeAllPrice)}
        </button>
      )}
    </div>
  )
}

function renderCard(s, i, kind, unlockRow, isFree) {
  return isLocked(s)
    ? <LockedSignalCard key={i} s={s} kind={kind} onUnlock={unlockRow} idx={i} isFree={isFree} />
    : <SignalCard key={s.ticker || i} s={s} />
}

function BuyColumn({ isFree }) {
  const { rows, unlockRow, unlockAll, sub, showSeeAll, seeAllPrice } = useSignalSection('BUY')
  const shown = isFree ? rows.slice(0, FREE_PREVIEW) : rows
  return (
    <div className="sig-col">
      <SectionHead kind="BUY" sub={sub} showSeeAll={showSeeAll} onSeeAll={unlockAll} seeAllPrice={seeAllPrice} />
      <div className="sig-cards">
        {rows.length === 0
          ? <div className="sig-empty">No buy signals today.</div>
          : shown.map((s, i) => renderCard(s, i, 'BUY', unlockRow, isFree))}
      </div>
    </div>
  )
}

function SellColumn({ isFree }) {
  const { rows, unlockRow, unlockAll, sub, showSeeAll, seeAllPrice } = useSignalSection('SELL')
  const [page, setPage] = useState(0)
  const pages = Math.max(1, Math.ceil(rows.length / SELL_PAGE))
  const safePage = Math.min(page, pages - 1)
  // Free: a short locked teaser (no paging — everything is locked anyway).
  const slice = isFree
    ? rows.slice(0, FREE_PREVIEW)
    : rows.slice(safePage * SELL_PAGE, safePage * SELL_PAGE + SELL_PAGE)
  return (
    <div className="sig-col">
      <SectionHead kind="SELL" sub={sub} showSeeAll={showSeeAll} onSeeAll={unlockAll} seeAllPrice={seeAllPrice} />
      <div className="sig-cards">
        {rows.length === 0
          ? <div className="sig-empty">No sell signals today.</div>
          : slice.map((s, i) => renderCard(s, isFree ? i : safePage * SELL_PAGE + i, 'SELL', unlockRow, isFree))}
      </div>
      {!isFree && pages > 1 && (
        <div className="sig-pager">
          <button disabled={safePage === 0} onClick={() => setPage(safePage - 1)}>‹ Prev</button>
          <span><b>{safePage + 1}</b> / {pages}</span>
          <button disabled={safePage >= pages - 1} onClick={() => setPage(safePage + 1)}>Next ›</button>
        </div>
      )}
    </div>
  )
}

// Single page-level CTA for free users — replaces the per-card unlock pills
// (which did nothing for free users since the whole feed is upgrade-gated).
function UpgradeBanner() {
  const navigate = useNavigate()
  return (
    <button className="sig-upsell" onClick={() => navigate('/account/plan')}>
      <span className="lk" aria-hidden="true">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
          <rect x="4.5" y="11" width="15" height="9.5" rx="2" /><path d="M8 11V7.5a4 4 0 0 1 8 0V11" />
        </svg>
      </span>
      <span className="txt">
        <span className="t1">Upgrade to unlock signals</span>
        <span className="t2">Get every BUY &amp; SELL signal, same-day, with full AI explanations.</span>
      </span>
      <span className="cta-arrow" aria-hidden="true">→</span>
    </button>
  )
}

export default function SignalsSplit() {
  const me = useMe()
  const isFree = me.plan === 'free'
  return (
    <>
      {isFree && <UpgradeBanner />}
      <div className="sig-twocol">
        <BuyColumn isFree={isFree} />
        <SellColumn isFree={isFree} />
      </div>
    </>
  )
}
