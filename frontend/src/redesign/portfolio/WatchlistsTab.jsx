/* Watchlists tab — organize tickers into named lists (owned or watch-only).
 * Lot-editing and aggregate KPIs live on the Holdings tab; this tab only
 * manages lists and their ticker membership. */
import { useState } from 'react'
import { useQuery, useQueries, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getWatchlists, getWatchlistTickers, getHoldings, getSignalsByTickers,
  getPortfolioComparison, createWatchlist,
} from '../../api'
import { useLivePrices } from '../../hooks/useLivePrices'
import { buildCards, filterCards, sortCards } from './watchlistDerive'
import WatchlistCard from './WatchlistCard'

export default function WatchlistsTab() {
  const qc = useQueryClient()
  const [q, setQ] = useState('')
  const [sortDesc, setSortDesc] = useState(true)
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')

  const { data: lists, isLoading: listsLoading } = useQuery({ queryKey: ['watchlists'], queryFn: getWatchlists })
  const listArr = Array.isArray(lists) ? lists : []

  const tickerQueries = useQueries({
    queries: listArr.map((l) => ({ queryKey: ['watchlist-tickers', l.id], queryFn: () => getWatchlistTickers(l.id) })),
  })
  const holdingsQueries = useQueries({
    queries: listArr.map((l) => ({ queryKey: ['watchlist-holdings', l.id], queryFn: () => getHoldings(l.id) })),
  })

  const tickersByList = {}
  const holdingsByList = {}
  listArr.forEach((l, i) => {
    tickersByList[l.id] = tickerQueries[i]?.data || []
    holdingsByList[l.id] = holdingsQueries[i]?.data || []
  })

  // Recomputed every render (cheap — a few dozen tickers at most); not worth
  // memoizing since the query keys below are derived strings, not array
  // identity, so a new array reference each render doesn't cause refetches.
  const allTickers = [...new Set(listArr.flatMap((l) => (tickersByList[l.id] || []).map((t) => t.ticker)))]

  const { data: signals } = useQuery({
    queryKey: ['wl-signals', allTickers.join(',')],
    queryFn: () => getSignalsByTickers(allTickers),
    enabled: allTickers.length > 0,
  })
  const signalsByTicker = Object.fromEntries((signals || []).map((s) => [s.ticker, s]))
  const { prices } = useLivePrices(allTickers)

  const { data: cmp } = useQuery({ queryKey: ['portfolio-comparison'], queryFn: () => getPortfolioComparison('US') })
  const comparisonByName = Object.fromEntries(
    (Array.isArray(cmp) ? cmp : []).filter((c) => c.name !== 'Vesign').map((c) => [c.name, c.yield]),
  )

  const ready = !listsLoading
    && tickerQueries.every((tq) => !tq.isLoading)
    && holdingsQueries.every((hq) => !hq.isLoading)
    && (allTickers.length === 0 || signals != null)

  const cards = buildCards({ lists: listArr, tickersByList, holdingsByList, signalsByTicker, prices, comparisonByName })
  const totalTickers = new Set(cards.flatMap((c) => c.rows.map((r) => r.ticker))).size
  const view = sortCards(filterCards(cards, q), sortDesc ? 'desc' : 'asc')

  const createListMut = useMutation({
    mutationFn: (name) => createWatchlist(name),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['watchlists'] })
      qc.invalidateQueries({ queryKey: ['portfolio-comparison'] })
      setNewName('')
      setCreating(false)
    },
  })
  const submitCreate = () => {
    const name = newName.trim()
    if (name) createListMut.mutate(name)
  }

  return (
    <div id="watchlists" className="tab-pane active">
      <div className="wl-toolbar">
        <div className="lead">
          <strong>{listArr.length}</strong> watchlist{listArr.length === 1 ? '' : 's'} ·{' '}
          <strong>{totalTickers}</strong> ticker{totalTickers === 1 ? '' : 's'} tracked
        </div>
        <div className="spacer"></div>
        {creating ? (
          <>
            <input className="search-input" autoFocus placeholder="List name"
              value={newName} onChange={(e) => setNewName(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') submitCreate(); if (e.key === 'Escape') setCreating(false) }} />
            <div className="btn-new" onClick={submitCreate}>Create</div>
            <div className="sort-pill" onClick={() => setCreating(false)}>Cancel</div>
          </>
        ) : (
          <>
            <input className="search-input" type="text" placeholder="Search watchlists or tickers..."
              value={q} onChange={(e) => setQ(e.target.value)} />
            <div className="sort-pill" onClick={() => setSortDesc((d) => !d)}>
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M3 6h18M6 12h12M10 18h4" /></svg>
              Sort: Yield {sortDesc ? '↓' : '↑'}
            </div>
            <div className="btn-new" onClick={() => setCreating(true)}><span className="plus">+</span> New watchlist</div>
          </>
        )}
      </div>

      {!ready ? (
        <div className="muted" style={{ padding: 24 }}>Loading…</div>
      ) : (
        <>
          {view.length === 0 && listArr.length > 0 && (
            <div className="muted" style={{ marginBottom: 16 }}>No matches.</div>
          )}
          <div className="wl-card-grid">
            {view.map((card) => <WatchlistCard key={card.id} card={card} />)}
            <div className="wl-card add-new" onClick={() => setCreating(true)}>
              <div className="plus-big">+</div>
              <div className="add-label">Create new watchlist</div>
              <div className="add-sub">Group tickers by theme, strategy, or sector</div>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
