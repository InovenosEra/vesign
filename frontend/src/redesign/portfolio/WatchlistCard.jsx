/* One watchlist card: header (name/menu/ticker count), a per-list "Vesign's
 * read" analysis panel, a stack of ticker cockpit cards (price/day-change/
 * target-price/analyst-upside/health, verdict badge), footer add-ticker.
 * No ownership concept anywhere — that's the Holdings tab, fully independent.
 * Owns all of its own list/ticker mutations so WatchlistsTab only has to
 * render it. */
import { useState, useEffect, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { renameWatchlist, deleteWatchlist, addTicker, removeTicker, updateTicker, searchTickers } from '../../api'
import { useTickerModal } from '../TickerModalContext'
import { useCurrency } from '../../context/CurrencyContext'
import { useMe } from '../../context/MeContext'
import { LOGO, pct, dirClass } from '../fmt'
import ConfirmDialog from './ConfirmDialog'
import WatchlistRead from './WatchlistRead'

const SIG_CLS = { BUY: 'buy', HOLD: 'hold', SELL: 'sell' }

function invalidateAfterMutation(qc, listId) {
  qc.invalidateQueries({ queryKey: ['watchlist-tickers', listId] })
  qc.invalidateQueries({ queryKey: ['watchlists'] })
}

function TickerCard({ r, listId, onOpen, onRemove }) {
  const me = useMe()
  const modelLocked = me.plan !== 'pro' && me.plan !== 'max'
  const qc = useQueryClient()
  const { fmtPrice } = useCurrency()
  const [draft, setDraft] = useState(r.targetPrice != null ? String(r.targetPrice) : '')
  const inputRef = useRef(null)

  // r.targetPrice can arrive after this component has already mounted (the
  // list loads in two phases: watchlists, then each list's tickers), so the
  // useState initializer above can miss it. Re-sync whenever it changes,
  // unless the user is actively editing the field.
  useEffect(() => {
    if (document.activeElement === inputRef.current) return
    setDraft(r.targetPrice != null ? String(r.targetPrice) : '')
  }, [r.targetPrice])

  const targetMut = useMutation({
    mutationFn: (value) => updateTicker(listId, r.ticker, { target_price: value }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['watchlist-tickers', listId] }),
  })
  const commitTarget = () => {
    const n = Number(draft)
    if (draft.trim() !== '' && n > 0 && n !== r.targetPrice) targetMut.mutate(n)
  }

  return (
    <div className="wl-ticker-card" onClick={() => onOpen(r.ticker, r.company || '')}>
      <button className="wl-row-remove" title="Remove from list"
        onClick={(e) => { e.stopPropagation(); onRemove(r.ticker) }}>✕</button>
      <div className="wl-ticker-head">
        <img className="logo-mini" src={LOGO(r.ticker)} alt={r.ticker} />
        <div>
          <div className="tk">{r.ticker}</div>
          <div className="co">{r.company || '—'}</div>
        </div>
        {r.signal
          ? <span className={'wl-verdict ' + (SIG_CLS[r.signal] || 'hold')}>{r.signal}</span>
          : modelLocked && <span className="wl-verdict hold wl-read-hazed">—</span>}
      </div>
      <div className="wl-cockpit">
        <div className="cell">
          <div className="lbl">Price</div>
          <div className="val">{r.price != null ? fmtPrice(r.price) : '—'}</div>
          <div className={'sub ' + dirClass(r.dayPct)}>{pct(r.dayPct, { fd: 2 })}</div>
        </div>
        <div className="cell" onClick={(e) => e.stopPropagation()}>
          <div className="lbl">Target</div>
          <input ref={inputRef} className="wl-target-input" inputMode="decimal" placeholder="—"
            value={draft} onChange={(e) => setDraft(e.target.value)}
            onBlur={commitTarget}
            onKeyDown={(e) => { if (e.key === 'Enter') e.target.blur() }} />
        </div>
        <div className="cell">
          <div className="lbl">Upside</div>
          <div className={'val ' + dirClass(r.upsidePct)}>{pct(r.upsidePct, { fd: 1 })}</div>
        </div>
        <div className="cell">
          <div className="lbl">Health</div>
          {r.healthScore != null ? (
            <span className="wl-health">
              {[0, 1, 2, 3, 4].map(i => <span key={i} className={'d' + (i < r.healthScore ? '' : ' off')} />)}
            </span>
          ) : modelLocked ? (
            <span className="wl-health wl-read-hazed">●●●●●</span>
          ) : '—'}
        </div>
      </div>
    </div>
  )
}

export default function WatchlistCard({ card }) {
  const qc = useQueryClient()
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()

  const [menuOpen, setMenuOpen] = useState(false)
  const [renaming, setRenaming] = useState(false)
  const [nameDraft, setNameDraft] = useState(card.name)
  const [confirmDeleteList, setConfirmDeleteList] = useState(false)
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
    onSuccess: () => invalidateAfterMutation(qc, card.id),
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
        <WatchlistRead card={card} />

        {card.rows.length === 0 ? (
          <div className="muted" style={{ padding: '14px 0' }}>No tickers yet.</div>
        ) : (
          <div className="wl-ticker-stack">
            {card.rows.map((r) => (
              <TickerCard key={r.ticker} r={r} listId={card.id} onOpen={open} onRemove={(t) => removeMut.mutate(t)} />
            ))}
          </div>
        )}

        {adding ? (
          <div className="ahf-field">
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
          <div className="wl-add-row" onClick={() => setAdding(true)}>+ Add ticker</div>
        )}
      </div>

      {confirmDeleteList && (
        <ConfirmDialog
          title={`Delete "${card.name}"?`}
          confirmLabel="Delete" danger
          body={<>This will remove <b>{card.name}</b> and its {card.tickerCount} tracked ticker{card.tickerCount === 1 ? '' : 's'}.</>}
          onConfirm={() => deleteListMut.mutateAsync()}
          onCancel={() => setConfirmDeleteList(false)}
        />
      )}
    </div>
  )
}
