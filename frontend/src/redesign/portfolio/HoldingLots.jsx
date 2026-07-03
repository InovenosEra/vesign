/* Expanded lot breakdown for one ticker: lists each lot with delete, plus a
 * "+ Add lot" form (DCA). Driven by /api/portfolio/holdings/lots. */
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getHoldingLots, deleteHolding } from '../../api'
import { num } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'
import AddHoldingForm from './AddHoldingForm'

export default function HoldingLots({ ticker, latestClose, colSpan }) {
  const qc = useQueryClient()
  const { fmtPrice, fmtAmount } = useCurrency()
  const [adding, setAdding] = useState(false)
  const { data: lots, isLoading } = useQuery({
    queryKey: ['portfolio-lots', ticker], queryFn: () => getHoldingLots(ticker),
  })
  const del = useMutation({
    mutationFn: (lot) => deleteHolding(lot.id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['portfolio-holdings'] })
      qc.invalidateQueries({ queryKey: ['portfolio-lots', ticker] })
    },
  })

  return (
    <tr className="lots-row">
      <td colSpan={colSpan}>
        <div className="lots-wrap">
          {isLoading ? <div className="lots-empty">Loading…</div>
            : !Array.isArray(lots) || lots.length === 0 ? <div className="lots-empty">No lots.</div>
            : (
              <table className="lots-table">
                <thead><tr>
                  <th>Buy date</th><th className="r">Shares</th><th className="r">Buy price</th>
                  <th className="r">Cost</th><th className="r">P/L</th><th></th>
                </tr></thead>
                <tbody>
                  {lots.map(l => {
                    const cost = l.quantity * l.buy_price
                    const pnl = latestClose != null ? (latestClose - l.buy_price) * l.quantity : null
                    return (
                      <tr key={l.id}>
                        <td>{l.buy_date}</td>
                        <td className="r">{num(l.quantity, { fd: 2 })}</td>
                        <td className="r">{fmtPrice(l.buy_price)}</td>
                        <td className="r">{fmtPrice(cost)}</td>
                        <td className={'r ' + (pnl == null ? '' : pnl >= 0 ? 'up' : 'down')}>{pnl == null ? '—' : fmtAmount(pnl)}</td>
                        <td className="r">
                          <button className="lot-del" title="Delete lot" disabled={del.isPending}
                            onClick={() => del.mutate(l)}>🗑</button>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}
          {adding
            ? <AddHoldingForm presetTicker={ticker} onDone={() => setAdding(false)} />
            : <button className="add-lot-btn" onClick={() => setAdding(true)}>+ Add lot</button>}
        </div>
      </td>
    </tr>
  )
}
