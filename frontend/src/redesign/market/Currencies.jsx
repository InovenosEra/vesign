/* Currencies strip — user picks a base currency; shows the 5 key world
 * currencies' rate per 1 unit in that base. Base picker uses the same styled
 * dropdown (.hs-menu) as the header currency selector. Base choice persists. */
import { useState, useRef, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getCurrencies } from '../../api'
import { num, pct, dirClass } from '../fmt'

const BASES = ['ILS', 'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CNY', 'AUD', 'CAD']
const SYM = { USD: '$', EUR: '€', GBP: '£', JPY: '¥', CHF: 'Fr', CNY: '¥', AUD: 'A$', CAD: 'C$', ILS: '₪' }

const Caret = () => (
  <svg className="hs-caret" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3"><polyline points="6 9 12 15 18 9" /></svg>
)

export default function Currencies() {
  const [base, setBase] = useState(() => {
    try { return localStorage.getItem('rd-fx-base') || 'ILS' } catch { return 'ILS' }
  })
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  useEffect(() => {
    const onDown = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [])
  const pickBase = (b) => {
    setBase(b); setOpen(false)
    try { localStorage.setItem('rd-fx-base', b) } catch { /* ignore */ }
  }
  const { data } = useQuery({ queryKey: ['market-currencies', base], queryFn: () => getCurrencies(base), refetchInterval: 60_000 })
  const rows = data?.currencies || []

  return (
    <>
      <div className="section-h">
        <h2>Currencies</h2>
        <span className="sub">Rate per 1 unit · vs{' '}
          <span className={'hdr-select fx-picker' + (open ? ' open' : '')} ref={ref}>
            <button className="fx-base" type="button" aria-haspopup="true" aria-expanded={open} onClick={() => setOpen(o => !o)}>
              {base} <Caret />
            </button>
            <div className="hs-menu" role="menu">
              {BASES.map(b => (
                <button key={b} type="button" role="menuitem" className={'hs-row' + (b === base ? ' sel' : '')} onClick={() => pickBase(b)}>
                  <span className="hs-sym">{SYM[b]}</span><span className="hs-name">{b}</span>
                </button>
              ))}
            </div>
          </span>
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
