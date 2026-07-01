/* One watchlist card: header (name/menu/aggregate yield), ticker rows (P&L
 * pill if owned, analyst upside if watch-only), footer add-ticker. Owns all
 * of its own list/ticker mutations so WatchlistsTab only has to render it. */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { renameWatchlist, deleteWatchlist, addTicker, removeTicker, searchTickers } from '../../api'
import { useTickerModal } from '../TickerModalContext'
import { useCurrency } from '../../context/CurrencyContext'
import { LOGO, pct, dirClass } from '../fmt'
import ConfirmDialog from './ConfirmDialog'

function invalidateAfterMutation(qc, listId) {
  qc.invalidateQueries({ queryKey: ['watchlist-tickers', listId] })
  qc.invalidateQueries({ queryKey: ['watchlist-holdings', listId] })
  qc.invalidateQueries({ queryKey: ['watchlists'] })
  qc.invalidateQueries({ queryKey: ['portfolio-comparison'] })
  qc.invalidateQueries({ queryKey: ['portfolio-holdings'] })
  qc.invalidateQueries({ queryKey: ['portfolio-lots'] })
}

export default function WatchlistCard({ card }) {
  const qc = useQueryClient()
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()

  const [menuOpen, setMenuOpen] = useState(false)
  const [renaming, setRenaming] = useState(false)
  const [nameDraft, setNameDraft] = useState(card.name)
  const [confirmDeleteList, setConfirmDeleteList] = useState(false)
  const [removeTarget, setRemoveTarget] = useState(null)   // Row being removed (owned -> needs confirm)
  const [adding, setAdding] = useState(false)
  const [q, setQ] = useState('')

  const { data: sug } = useQuery({
    queryKey: ['wl-add-search', card.id, q],
    queryFn: () => searchTickers(q, 6),
    enabled: adding && q.trim().length >= 1,
  })

  const renameMut = useMutation({
    mutationFn: (name) => renameWatchlist(card.id, name),
    onSuccess: () => { invalidateAfterMutation(qc, card.id); setRenaming(false) },
  })
  const deleteListMut = useMutation({
    mutationFn: () => deleteWatchlist(card.id),
    onSuccess: () => invalidateAfterMutation(qc, card.id),
  })
  const addMut = useMutation({
    mutationFn: (ticker) => addTicker(card.id, ticker),
    onSuccess: () => { invalidateAfterMutation(qc, card.id); setAdding(false); setQ('') },
  })
  const removeMut = useMutation({
    mutationFn: (ticker) => removeTicker(card.id, ticker),
    onSuccess: () => { invalidateAfterMutation(qc, card.id); setRemoveTarget(null) },
  })

  const commitRename = () => {
    const name = nameDraft.trim()
    if (name && name !== card.name) renameMut.mutate(name)
    else setRenaming(false)
  }

  return (
    <div className="wl-card">
      <div className="wl-card-head">
        <div className="title">
          {renaming ? (
            <input className="ahf-input" autoFocus value={nameDraft}
              onChange={(e) => setNameDraft(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') commitRename(); if (e.key === 'Escape') setRenaming(false) }}
              onBlur={commitRename} />
          ) : (
            <div className="nm">{card.name}</div>
          )}
          <div className="ct">{card.tickerCount} ticker{card.tickerCount === 1 ? '' : 's'}</div>
        </div>
        <div className="yield-block">
          <div className="l">12m yield</div>
          <div className={'y ' + dirClass(card.aggregateYield)}>{pct(card.aggregateYield)}</div>
        </div>
        <div className="wl-menu-wrap">
          <div className="menu" onClick={() => setMenuOpen((v) => !v)}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><circle cx="12" cy="5" r="1.6" /><circle cx="12" cy="12" r="1.6" /><circle cx="12" cy="19" r="1.6" /></svg>
          </div>
          {menuOpen && (
            <div className="wl-menu-pop" onMouseLeave={() => setMenuOpen(false)}>
              <button className="wl-menu-item" onClick={() => { setRenaming(true); setMenuOpen(false) }}>Rename</button>
              <button className="wl-menu-item danger" onClick={() => { setConfirmDeleteList(true); setMenuOpen(false) }}>Delete</button>
            </div>
          )}
        </div>
      </div>

      <div className="wl-card-body">
        {card.rows.length === 0 ? (
          <div className="muted" style={{ padding: '14px 18px', fontSize: 12.5 }}>No tickers yet.</div>
        ) : card.rows.map((r) => (
          <div className="wl-card-row" key={r.ticker} onClick={() => open(r.ticker, r.company || '')}>
            <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
            <div><div className="tk">{r.ticker}</div><div className="co">{r.company || '—'}</div></div>
            <div className="px">{r.price != null ? fmtPrice(r.price) : '—'}</div>
            <div className={'day ' + dirClass(r.dayPct)}>{pct(r.dayPct, { fd: 2 })}</div>
            {r.owned ? (
              <div className={'y-pill ' + (r.yieldPct == null ? 'flat' : dirClass(r.yieldPct))}>{pct(r.yieldPct, { fd: 1 })}</div>
            ) : (
              <div className={'y-pill ' + (r.upsidePct == null ? 'flat' : dirClass(r.upsidePct))}>{pct(r.upsidePct, { fd: 1 })}</div>
            )}
            <button className="wl-row-remove" title="Remove from list"
              onClick={(e) => { e.stopPropagation(); r.owned ? setRemoveTarget(r) : removeMut.mutate(r.ticker) }}>
              ✕
            </button>
          </div>
        ))}
      </div>

      <div className="wl-card-foot">
        {adding ? (
          <div className="ahf-field" style={{ flex: 1 }}>
            <input className="ahf-input" autoFocus placeholder="Ticker symbol" style={{ maxWidth: 160 }}
              value={q} onChange={(e) => setQ(e.target.value.toUpperCase())}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && q.trim()) addMut.mutate(q.trim())
                if (e.key === 'Escape') { setAdding(false); setQ('') }
              }} />
            {q.trim() && Array.isArray(sug) && sug.length > 0 && (
              <div className="ahf-suggest">
                {sug.map((s) => (
                  <div key={s.ticker} className="ahf-suggest-row" onMouseDown={(e) => e.preventDefault()}
                    onClick={() => addMut.mutate(s.ticker)}>
                    <b>{s.ticker}</b> <span>{s.company || ''}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : (
          <span className="add" onClick={() => setAdding(true)}>+ Add ticker</span>
        )}
      </div>

      {confirmDeleteList && (() => {
        const ownedRows = card.rows.filter((r) => r.owned)
        const invested = ownedRows.reduce((s, r) => s + (r.costBasis || 0), 0)
        return (
          <ConfirmDialog
            title={`Delete "${card.name}"?`}
            confirmLabel="Delete" danger
            body={ownedRows.length > 0
              ? <>This list has <b>{card.tickerCount}</b> tickers, including <b>{ownedRows.length}</b> owned
                  (<b>{fmtPrice(invested)}</b> invested). Deleting the list will also delete those holdings.</>
              : <>This will remove <b>{card.name}</b> and its {card.tickerCount} tracked ticker{card.tickerCount === 1 ? '' : 's'}.</>}
            onConfirm={() => deleteListMut.mutateAsync()}
            onCancel={() => setConfirmDeleteList(false)}
          />
        )
      })()}

      {removeTarget && (
        <ConfirmDialog
          title={`Remove ${removeTarget.ticker}?`}
          confirmLabel="Remove" danger
          body={<>{removeTarget.ticker} has <b>{removeTarget.lotCount}</b> lot{removeTarget.lotCount === 1 ? '' : 's'}
                 {' '}(<b>{fmtPrice(removeTarget.costBasis || 0)}</b> invested) in this list — removing it will also
                 delete those holdings.</>}
          onConfirm={() => removeMut.mutateAsync(removeTarget.ticker)}
          onCancel={() => setRemoveTarget(null)}
        />
      )}
    </div>
  )
}
