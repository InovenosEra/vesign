/* Signals page body: two columns — BUY (left) and SELL (right). Every signal is
 * a card with metrics + the AI explanation inline (SignalCard). Both columns are
 * paginated (5 cards per page) so only a page of explanations is shown at a time.
 * Data: getSignalsToday. */
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getSignalsToday, unlockSignal } from '../../api'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents, seeAllCents } from './gating'
import { SignalCard, LockedSignalCard } from './SignalCard'

const PAGE_SIZE = 5
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
      // Refetch now; react-query keeps the previous rows during the refetch, so the
      // card stays mounted long enough for its fade-out before the real card swaps in.
      qc.invalidateQueries({ queryKey: ['signals-today', kind, 'US'] })
      qc.invalidateQueries({ queryKey: ['me'] })
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
    ? <LockedSignalCard key={s.lock_token || 'L' + i} s={s} kind={kind} onUnlock={unlockRow} idx={i} isFree={isFree} />
    : <SignalCard key={s.ticker || i} s={s} />
}

// One paginated column for either side. BUY and SELL behave identically: 5 cards
// per page with a pager. Free users get a short locked teaser instead (no paging,
// since the whole feed is upgrade-gated).
function SignalColumn({ kind, isFree }) {
  const { rows, unlockRow, unlockAll, sub, showSeeAll, seeAllPrice } = useSignalSection(kind)
  const [page, setPage] = useState(0)
  const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE))
  const safePage = Math.min(page, pages - 1)
  const emptyMsg = kind === 'BUY' ? 'No buy signals today.' : 'No sell signals today.'
  const slice = isFree
    ? rows.slice(0, FREE_PREVIEW)
    : rows.slice(safePage * PAGE_SIZE, safePage * PAGE_SIZE + PAGE_SIZE)
  return (
    <div className="sig-col">
      <SectionHead kind={kind} sub={sub} showSeeAll={showSeeAll} onSeeAll={unlockAll} seeAllPrice={seeAllPrice} />
      <div className="sig-cards">
        {rows.length === 0
          ? <div className="sig-empty">{emptyMsg}</div>
          : slice.map((s, i) => renderCard(s, isFree ? i : safePage * PAGE_SIZE + i, kind, unlockRow, isFree))}
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
        <SignalColumn kind="BUY" isFree={isFree} />
        <SignalColumn kind="SELL" isFree={isFree} />
      </div>
    </>
  )
}
