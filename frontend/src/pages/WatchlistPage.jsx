import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getWatchlists, createWatchlist, deleteWatchlist,
  getWatchlistTickers, addTicker, removeTicker, updateTickerNote,
} from '../api'

function SignalBadge({ signal }) {
  if (!signal) return '—'
  return <span className={`badge badge-${signal}`}>{signal}</span>
}

export default function WatchlistPage() {
  const qc = useQueryClient()
  const [selectedId, setSelectedId] = useState(null)
  const [newListName, setNewListName] = useState('')
  const [newTicker, setNewTicker] = useState('')
  const [newNote, setNewNote] = useState('')
  const [editNotes, setEditNotes] = useState({})

  const { data: lists = [] } = useQuery({
    queryKey: ['watchlists'],
    queryFn: getWatchlists,
  })

  const { data: tickers = [], isLoading: loadingTickers } = useQuery({
    queryKey: ['watchlist-tickers', selectedId],
    queryFn: () => getWatchlistTickers(selectedId),
    enabled: selectedId != null,
  })

  const invalidateLists = () => qc.invalidateQueries({ queryKey: ['watchlists'] })
  const invalidateTickers = () => qc.invalidateQueries({ queryKey: ['watchlist-tickers', selectedId] })

  const createMut = useMutation({
    mutationFn: () => createWatchlist(newListName.trim()),
    onSuccess: (created) => {
      invalidateLists()
      setNewListName('')
      setSelectedId(created.id)
    },
  })

  const deleteMut = useMutation({
    mutationFn: (id) => deleteWatchlist(id),
    onSuccess: () => {
      invalidateLists()
      if (selectedId === deleteMut.variables) setSelectedId(null)
    },
  })

  const addMut = useMutation({
    mutationFn: () => addTicker(selectedId, newTicker, newNote),
    onSuccess: () => {
      invalidateTickers()
      setNewTicker('')
      setNewNote('')
    },
  })

  const removeMut = useMutation({
    mutationFn: (ticker) => removeTicker(selectedId, ticker),
    onSuccess: invalidateTickers,
  })

  const noteMut = useMutation({
    mutationFn: ({ ticker, note }) => updateTickerNote(selectedId, ticker, note),
    onSuccess: invalidateTickers,
  })

  const selectedList = lists.find(l => l.id === selectedId)

  return (
    <div>
      <p className="page-title">Watchlists</p>
      <div className="watchlist-layout">

        {/* ── Sidebar ── */}
        <div className="watchlist-sidebar">
          <h3>My Lists</h3>
          {lists.length === 0 && <p className="empty" style={{ fontSize: 12 }}>No lists yet.</p>}
          {lists.map(l => (
            <div
              key={l.id}
              className={`list-item ${l.id === selectedId ? 'selected' : ''}`}
              onClick={() => setSelectedId(l.id)}
            >
              <span>{l.name}</span>
              <button
                className="del-btn"
                onClick={e => { e.stopPropagation(); deleteMut.mutate(l.id) }}
                title="Delete list"
              >✕</button>
            </div>
          ))}

          <div className="new-list-row">
            <input
              placeholder="New list name…"
              value={newListName}
              onChange={e => setNewListName(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && newListName.trim() && createMut.mutate()}
            />
            <button
              className="primary"
              onClick={() => createMut.mutate()}
              disabled={!newListName.trim() || createMut.isPending}
            >+</button>
          </div>
          {createMut.isError && <p className="error" style={{ fontSize: 12, marginTop: 6 }}>{createMut.error.message}</p>}
        </div>

        {/* ── Main panel ── */}
        <div>
          {!selectedList ? (
            <p className="empty">Select or create a watchlist.</p>
          ) : (
            <>
              <p className="section-title">{selectedList.name}</p>

              {/* Add ticker row */}
              <div className="controls" style={{ marginBottom: 20 }}>
                <input
                  placeholder="Ticker (e.g. AAPL)"
                  value={newTicker}
                  onChange={e => setNewTicker(e.target.value.toUpperCase())}
                  style={{ width: 120 }}
                />
                <input
                  placeholder="Note (optional)"
                  value={newNote}
                  onChange={e => setNewNote(e.target.value)}
                  style={{ width: 220 }}
                />
                <button
                  className="primary"
                  onClick={() => addMut.mutate()}
                  disabled={!newTicker || addMut.isPending}
                >Add</button>
                {addMut.isError && <span className="error">{addMut.error.message}</span>}
              </div>

              {loadingTickers ? (
                <p className="loading">Loading…</p>
              ) : tickers.length === 0 ? (
                <p className="empty">No tickers yet. Add one above.</p>
              ) : (
                <div className="data-table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Ticker</th>
                        <th>Note</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {tickers.map(t => (
                        <tr key={t.ticker}>
                          <td><strong>{t.ticker}</strong></td>
                          <td>
                            <input
                              style={{
                                background: 'transparent',
                                border: '1px solid transparent',
                                color: 'var(--text)',
                                padding: '4px 8px',
                                borderRadius: 4,
                                width: 260,
                                fontFamily: 'inherit',
                                fontSize: 13,
                              }}
                              value={editNotes[t.ticker] ?? t.note ?? ''}
                              onChange={e => setEditNotes(n => ({ ...n, [t.ticker]: e.target.value }))}
                              onBlur={() => {
                                const note = editNotes[t.ticker]
                                if (note !== undefined && note !== t.note) {
                                  noteMut.mutate({ ticker: t.ticker, note })
                                }
                              }}
                              onFocus={e => {
                                e.target.style.borderColor = 'var(--border)'
                              }}
                              placeholder="Add a note…"
                            />
                          </td>
                          <td>
                            <button
                              className="danger"
                              style={{ padding: '4px 10px', fontSize: 12 }}
                              onClick={() => removeMut.mutate(t.ticker)}
                            >Remove</button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}
