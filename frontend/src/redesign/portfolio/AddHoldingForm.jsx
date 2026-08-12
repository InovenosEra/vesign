/* Add-holding / add-lot form. Used both for a brand-new ticker (no preset),
 * for adding a lot to an existing ticker (presetTicker locks the symbol),
 * and for editing an existing lot (editingLot pre-fills the fields and
 * submits to the update endpoint instead of create).
 * Holdings are user-scoped (not filed under any watchlist), so this form only
 * ever asks for ticker/shares/price/date. */
import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { searchTickers, addHolding, updateHolding, getResearch } from '../../api'
import { validateHolding } from './holdingForm'

const today = () => new Date().toISOString().slice(0, 10)

export default function AddHoldingForm({ presetTicker, editingLot, onDone }) {
  const qc = useQueryClient()
  const [ticker, setTicker] = useState(presetTicker || '')
  const [q, setQ] = useState('')
  const [shares, setShares] = useState(editingLot ? String(editingLot.quantity) : '')
  const [price, setPrice] = useState(editingLot ? String(editingLot.buy_price) : '')
  const [date, setDate] = useState(editingLot ? editingLot.buy_date : today())
  const [err, setErr] = useState(null)

  const { data: sug } = useQuery({
    queryKey: ['hold-search', q], queryFn: () => searchTickers(q, 6),
    enabled: !presetTicker && q.trim().length >= 1,
  })

  // Prefill price with the live/current price once a ticker is chosen (new lots only).
  const { data: liveClose } = useQuery({
    queryKey: ['hold-price', ticker], enabled: !!ticker && !editingLot, staleTime: 60_000,
    queryFn: () => getResearch(ticker).then(r => (r?.close ?? null)),
  })
  useEffect(() => {
    if (liveClose != null && price === '') setPrice(liveClose.toFixed(2))
  }, [liveClose])  // only when the fetched price changes; leaves user edits intact

  const save = useMutation({
    mutationFn: () => editingLot
      ? updateHolding(editingLot.id, { quantity: Number(shares), buy_price: Number(price), buy_date: date })
      : addHolding({ ticker: ticker.trim().toUpperCase(), quantity: Number(shares), buy_price: Number(price), buy_date: date }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['portfolio-holdings'] })
      qc.invalidateQueries({ queryKey: ['portfolio-lots'] })
      onDone?.()
    },
    onError: (e) => setErr(e?.message || (editingLot ? 'Could not save lot' : 'Could not add holding')),
  })

  const submit = () => {
    setErr(null)
    const v = validateHolding({ ticker, shares, price, date })
    if (v) { setErr(v); return }
    save.mutate()
  }

  return (
    <div className="add-holding-form">
      {!presetTicker && (
        <div className="ahf-field ahf-ticker">
          <input className="ahf-input" placeholder="Ticker" value={ticker}
            onChange={(e) => { setTicker(e.target.value.toUpperCase()); setQ(e.target.value) }} />
          {q.trim() && Array.isArray(sug) && sug.length > 0 && (
            <div className="ahf-suggest">
              {sug.map(s => (
                <div key={s.ticker} className="ahf-suggest-row" onMouseDown={(e) => e.preventDefault()}
                  onClick={() => { setTicker(s.ticker); setQ('') }}>
                  <b>{s.ticker}</b> <span>{s.company || ''}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
      <input className="ahf-input" type="number" min="0" step="any" placeholder="Shares"
        value={shares} onChange={(e) => setShares(e.target.value)} />
      <input className="ahf-input" type="number" min="0" step="any" placeholder="Buy price"
        value={price} onChange={(e) => setPrice(e.target.value)} />
      <input className="ahf-input" type="date" max={today()} value={date}
        onChange={(e) => setDate(e.target.value)} />
      <button className="ahf-btn" disabled={save.isPending} onClick={submit}>
        {save.isPending ? (editingLot ? 'Saving…' : 'Adding…') : (editingLot ? 'Save' : 'Add')}
      </button>
      <button className="ahf-btn ghost" onClick={() => onDone?.()}>Cancel</button>
      {err && <div className="ahf-err">{err}</div>}
    </div>
  )
}
