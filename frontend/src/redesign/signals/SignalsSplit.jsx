/* Signals page body: two columns — BUY (left) and SELL (right). Every signal is
 * a card with metrics + the AI explanation inline (SignalCard). Both columns are
 * paginated (5 cards per page) so only a page of explanations is shown at a time.
 * Data: getSignalsToday. */
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { getSignalsToday, getSignalsTodayTiers, unlockSignal } from '../../api'
import { useMe } from '../../context/MeContext'
import { isLocked, hasMoreLocked, fmtCents, seeAllCents, lockedCount, tierOf, tierUnlockCents, allTiersCents, allTiersGrossCents } from './gating'
import { SignalCard, LockedSignalCard } from './SignalCard'
import { TierLegend } from './tierStar'
import { UnlockAllButton, ConfirmUnlockDialog } from './UnlockAll'

const PAGE_SIZE = 5
const FREE_PREVIEW = 4   // free users see a short locked teaser per column (no pager)

// Query one side + the unlock handlers + header counts/prices.
function useSignalSection(kind) {
  const me = useMe()
  const qc = useQueryClient()
  const { data } = useQuery({
    queryKey: ['signals-today', kind, 'US'],
    queryFn: () => getSignalsToday(kind, 'US'),
    refetchInterval: 3_000,
  })
  // BUY-only tier breakdown for the section-head legend/chips.
  const { data: tiers } = useQuery({
    queryKey: ['signals-today-tiers', 'US'],
    queryFn: () => getSignalsTodayTiers('US'),
    enabled: kind === 'BUY',
  })
  const rows = Array.isArray(data) ? data : []
  const isBuy = kind === 'BUY'

  async function unlock(body) {
    try {
      await unlockSignal({ market: 'US', ...body })
      qc.invalidateQueries({ queryKey: ['signals-today', kind, 'US'] })
      qc.invalidateQueries({ queryKey: ['signals-today-tiers', 'US'] })
      qc.invalidateQueries({ queryKey: ['me'] })
    } catch (e) {
      if (String(e.message).startsWith('402')) alert('Not enough wallet balance.')
    }
  }

  // Per-tier locked counts: total (from /tiers) minus the unlocked rows we can see.
  const tierCounts = tiers ? { 1: tiers.tiers.prime.count, 2: tiers.tiers.strong.count, 3: tiers.tiers.promising.count } : null
  const rates = tiers ? { 1: tiers.tiers.prime.rate_cents, 2: tiers.tiers.strong.rate_cents, 3: tiers.tiers.promising.rate_cents } : null
  const lockedByTier = {}
  if (isBuy && tierCounts) {
    for (const t of [1, 2, 3]) {
      const unlockedSeen = rows.filter(r => !isLocked(r) && tierOf(r) === t).length
      lockedByTier[t] = Math.max(0, tierCounts[t] - unlockedSeen)
    }
  }

  const sub = rows.length ? `${rows.length} ${rows.length === 1 ? 'signal' : 'signals'}` : '—'
  const isPro = me.plan === 'pro'
  // BUY "all" price = value-weighted across still-locked tiers; SELL = legacy bulk.
  const seeAllPrice = isBuy
    ? allTiersCents(lockedByTier, rates)
    : seeAllCents(rows.length, me.per_row_price_cents?.sell ?? 10)
  // "was" anchor (undiscounted tier sum) — BUY only, shown struck through.
  const seeAllGross = isBuy ? allTiersGrossCents(lockedByTier, rates) : 0
  const showSeeAll = isPro && hasMoreLocked(rows) && (!isBuy || !!tiers)
  return { me, isBuy, isPro, rows, unlock, sub, showSeeAll, seeAllPrice, seeAllGross, tierCounts, lockedByTier, rates }
}

function SectionHead({ kind, sub, tierCounts, lockedByTier, rates, isPro, onBuyTier, showSeeAll, onSeeAll, seeAllPrice, seeAllGross }) {
  const isBuy = kind === 'BUY'
  // A tier chip is a buy button only for a Pro user with ≥1 still-locked signal.
  const buys = (isBuy && isPro && tierCounts) ? {} : null
  if (buys) {
    for (const t of [1, 2, 3]) {
      const n = lockedByTier?.[t] || 0
      if (n > 0) buys[t] = { price: fmtCents(tierUnlockCents(t, n, rates)), onUnlock: () => onBuyTier(t) }
    }
  }
  return (
    <div className="sig-sec-h">
      <div className="ssh-left">
        <span className={'tag ' + (isBuy ? 'buy' : 'sell')}>
          <svg className="tag-ico" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            {isBuy ? <><path d="M12 19V7" /><path d="M6 13l6-6 6 6" /></> : <><path d="M12 5v12" /><path d="M18 11l-6 6-6-6" /></>}
          </svg>
          {kind}
        </span>
        <span className="sub">{sub}</span>
        {isBuy && <TierLegend counts={tierCounts} buys={buys} />}
      </div>
      <div className="ssh-right">
        {showSeeAll && (
          <UnlockAllButton price={seeAllPrice} anchor={seeAllGross} onClick={onSeeAll} tone={isBuy ? 'buy' : 'sell'} />
        )}
      </div>
    </div>
  )
}

function renderCard(s, i, kind, isFree) {
  return isLocked(s)
    ? <LockedSignalCard key={'L' + i} s={s} kind={kind} idx={i} isFree={isFree} />
    : <SignalCard key={s.ticker || i} s={s} />
}

function SignalColumn({ kind, isFree }) {
  const { rows, unlock, sub, showSeeAll, seeAllPrice, seeAllGross, tierCounts, lockedByTier, rates, isPro } = useSignalSection(kind)
  const [page, setPage] = useState(0)
  const [confirm, setConfirm] = useState(null)   // { scope:'all' } | { scope:'tier', tier }
  const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE))
  const safePage = Math.min(page, pages - 1)
  const emptyMsg = kind === 'BUY' ? 'No buy signals today.' : 'No sell signals today.'
  const slice = isFree
    ? rows.slice(0, FREE_PREVIEW)
    : rows.slice(safePage * PAGE_SIZE, safePage * PAGE_SIZE + PAGE_SIZE)

  const TIER_LABEL = { 1: 'Prime', 2: 'Strong', 3: 'Promising' }
  let dialog = null
  if (confirm?.scope === 'all') {
    const count = lockedCount(rows)
    const saving = seeAllGross > seeAllPrice ? seeAllGross - seeAllPrice : 0
    dialog = {
      title: `Unlock all ${kind} signals?`,
      body: <>This unlocks {count} locked {kind} {count === 1 ? 'signal' : 'signals'} and charges{' '}
        <b>{fmtCents(seeAllPrice)}</b> from your wallet{saving ? <> — you save <b>{fmtCents(saving)}</b></> : null}.</>,
      price: seeAllPrice,
      run: () => unlock({ kind: kind.toLowerCase(), scope: 'all' }),
    }
  } else if (confirm?.scope === 'tier') {
    const t = confirm.tier
    const n = lockedByTier?.[t] || 0
    const price = tierUnlockCents(t, n, rates)
    dialog = {
      title: `Unlock all ${TIER_LABEL[t]} signals?`,
      body: <>This unlocks {n} locked {TIER_LABEL[t]} {n === 1 ? 'signal' : 'signals'} and charges{' '}
        <b>{fmtCents(price)}</b> from your wallet.</>,
      price,
      run: () => unlock({ kind: 'buy', scope: 'tier', tier: t }),
    }
  }

  return (
    <div className="sig-col">
      <SectionHead
        kind={kind} sub={sub} tierCounts={tierCounts} lockedByTier={lockedByTier} rates={rates} isPro={isPro}
        onBuyTier={(t) => setConfirm({ scope: 'tier', tier: t })}
        showSeeAll={showSeeAll} onSeeAll={() => setConfirm({ scope: 'all' })}
        seeAllPrice={seeAllPrice} seeAllGross={seeAllGross}
      />
      {dialog && (
        <ConfirmUnlockDialog
          title={dialog.title} body={dialog.body} price={dialog.price}
          onConfirm={async () => { await dialog.run(); setConfirm(null) }}
          onCancel={() => setConfirm(null)}
        />
      )}
      <div className="sig-cards">
        {rows.length === 0
          ? <div className="sig-empty">{emptyMsg}</div>
          : slice.map((s, i) => renderCard(s, isFree ? i : safePage * PAGE_SIZE + i, kind, isFree))}
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
