/* Currencies strip — user picks a base currency; shows the 5 key world
 * currencies' rate per 1 unit in that base (e.g. base ILS → USD/ILS, EUR/ILS …).
 * Same .cm-cell style as the other strips. Base choice persists. */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getCurrencies } from '../../api'
import { num, pct, dirClass } from '../fmt'

const BASES = ['ILS', 'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CNY', 'AUD', 'CAD']

export default function Currencies() {
  const [base, setBase] = useState(() => {
    try { return localStorage.getItem('rd-fx-base') || 'ILS' } catch { return 'ILS' }
  })
  const pickBase = (b) => { setBase(b); try { localStorage.setItem('rd-fx-base', b) } catch { /* ignore */ } }
  const { data } = useQuery({ queryKey: ['market-currencies', base], queryFn: () => getCurrencies(base), refetchInterval: 60_000 })
  const rows = data?.currencies || []
  return (
    <>
      <div className="section-h">
        <h2>Currencies</h2>
        <span className="sub">Rate per 1 unit · vs{' '}
          <select className="fx-base" value={base} onChange={(e) => pickBase(e.target.value)} aria-label="Base currency">
            {BASES.map(b => <option key={b} value={b}>{b}</option>)}
          </select>
        </span>
      </div>
      <div className="cross-market" style={{ gridTemplateColumns: `repeat(${Math.max(rows.length, 1)}, 1fr)` }}>
        {rows.map((r, i) => (
          <div className="cm-cell" key={i}>
            <div className="lbl">{r.label}</div>
            <div className="val">{r.price == null ? '—' : num(r.price, { fd: r.price < 10 ? 4 : 2 })}</div>
            <div className={'delta ' + dirClass(r.change_pct)}>{pct(r.change_pct)}</div>
          </div>
        ))}
      </div>
    </>
  )
}
