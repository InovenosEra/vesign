/* Commodities strip — Gold / Silver / WTI / Brent / Nat Gas / Copper (yfinance
 * futures). Same .cm-cell style as the cross-market strip, with a section title. */
import { useQuery } from '@tanstack/react-query'
import { getCommodities } from '../../api'
import { num, pct, dirClass } from '../fmt'

export default function Commodities() {
  const { data } = useQuery({ queryKey: ['market-commodities'], queryFn: getCommodities, refetchInterval: 15_000 })
  const rows = data?.commodities || []
  if (!rows.length) return null
  return (
    <>
      <div className="section-h"><h2>Commodities</h2><span className="sub">Futures · last close</span></div>
      <div className="cross-market" style={{ gridTemplateColumns: `repeat(${rows.length}, 1fr)` }}>
        {rows.map((r, i) => (
          <div className="cm-cell" key={i}>
            <div className="lbl">{r.label}{r.stale && <span style={{ opacity: 0.6, fontSize: 9 }}> stale</span>}</div>
            <div className="val">{r.price == null ? '—' : num(r.price, { fd: 2 })}</div>
            <div className={'delta ' + dirClass(r.change_pct)}>{pct(r.change_pct)}</div>
          </div>
        ))}
      </div>
    </>
  )
}
