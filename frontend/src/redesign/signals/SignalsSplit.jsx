/* Signals page body: two columns — BUY (left) and SELL (right). Every signal is
 * a card with metrics + the AI explanation inline (SignalCard). SELL is paginated
 * so only a page of explanations is fetched at a time. Data: getSignalsToday. */
import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getSignalsToday, unlockSignal } from '../../api'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents } from './gating'
import { SignalCard, LockedSignalCard } from './SignalCard'

const SELL_PAGE = 12

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
  const sub = rows.length ? `${rows.length} ${rows.length === 1 ? 'signal' : 'signals'}` : '—'
  const showSeeAll = me.plan === 'pro' && hasMoreLocked(rows)
  return { me, rows, unlockRow, unlockAll, sub, showSeeAll }
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

function renderCard(s, i, kind, unlockRow) {
  return isLocked(s)
    ? <LockedSignalCard key={i} s={s} kind={kind} onUnlock={unlockRow} idx={i} />
    : <SignalCard key={s.ticker || i} s={s} />
}

function BuyColumn() {
  const { me, rows, unlockRow, unlockAll, sub, showSeeAll } = useSignalSection('BUY')
  return (
    <div className="sig-col">
      <SectionHead kind="BUY" sub={sub} showSeeAll={showSeeAll} onSeeAll={unlockAll} seeAllPrice={me.see_all_price_cents} />
      <div className="sig-cards">
        {rows.length === 0
          ? <div className="sig-empty">No buy signals today.</div>
          : rows.map((s, i) => renderCard(s, i, 'BUY', unlockRow))}
      </div>
    </div>
  )
}

function SellColumn() {
  const { me, rows, unlockRow, unlockAll, sub, showSeeAll } = useSignalSection('SELL')
  const [page, setPage] = useState(0)
  const pages = Math.max(1, Math.ceil(rows.length / SELL_PAGE))
  const safePage = Math.min(page, pages - 1)
  const slice = rows.slice(safePage * SELL_PAGE, safePage * SELL_PAGE + SELL_PAGE)
  return (
    <div className="sig-col">
      <SectionHead kind="SELL" sub={sub} showSeeAll={showSeeAll} onSeeAll={unlockAll} seeAllPrice={me.see_all_price_cents} />
      <div className="sig-cards">
        {rows.length === 0
          ? <div className="sig-empty">No sell signals today.</div>
          : slice.map((s, i) => renderCard(s, safePage * SELL_PAGE + i, 'SELL', unlockRow))}
      </div>
      {pages > 1 && (
        <div className="sig-pager">
          <button disabled={safePage === 0} onClick={() => setPage(safePage - 1)}>‹ Prev</button>
          <span><b>{safePage + 1}</b> / {pages}</span>
          <button disabled={safePage >= pages - 1} onClick={() => setPage(safePage + 1)}>Next ›</button>
        </div>
      )}
    </div>
  )
}

export default function SignalsSplit() {
  return (
    <div className="sig-twocol">
      <BuyColumn />
      <SellColumn />
    </div>
  )
}
