/* Tape — top-20 US stocks by market cap. Price + change come from the server's
 * shared live snapshot (same basis as the movers/breadth panels), so a ticker
 * reads the same % here as everywhere else. Refreshes every 3s. */
import { useQuery } from '@tanstack/react-query'
import { getTape } from '../../api'
import { num, dirClass } from '../fmt'
import { useTickerModal } from '../TickerModalContext'

// Directional change: ▲/▼ arrow (coloured via dirClass) + the magnitude — no +/- sign.
const changeArrow = (v) => {
  if (v == null || !isFinite(v)) return '—'
  const a = v > 0 ? '▲' : v < 0 ? '▼' : ''
  return <>{a && <span className="tape-arrow">{a}</span>}{Math.abs(v).toFixed(2)}%</>
}

export default function Tape() {
  const open = useTickerModal()
  const { data } = useQuery({ queryKey: ['market-tape'], queryFn: getTape, refetchInterval: 3_000 })
  const items = (data?.tape || []).filter((r) => r.close != null)

  const Item = (r, k) => (
    <span className="tape-item" key={k}>
      <span className="tk" data-ticker={r.ticker} onClick={() => open(r.ticker)}>{r.ticker}</span>{num(r.close)}
      <span className={dirClass(r.change_pct)}>{changeArrow(r.change_pct)}</span>
    </span>
  )

  return (
    <div className="tape">
      <div className="tape-track">
        {items.map((r, i) => Item(r, 'a' + i))}
        {items.map((r, i) => Item(r, 'b' + i))}
      </div>
    </div>
  )
}
