/* Tape — top-20 US stocks by market cap. Price + change come from the server's
 * shared live snapshot (same basis as the movers/breadth panels), so a ticker
 * reads the same % here as everywhere else. Refreshes every 3s. */
import { useQuery } from '@tanstack/react-query'
import { getTape } from '../../api'
import { num, pct, dirClass } from '../fmt'
import { useTickerModal } from '../TickerModalContext'

export default function Tape() {
  const open = useTickerModal()
  const { data } = useQuery({ queryKey: ['market-tape'], queryFn: getTape, refetchInterval: 3_000 })
  const items = (data?.tape || []).filter((r) => r.close != null)

  const Item = (r, k) => (
    <span className="tape-item" key={k}>
      <span className="tk" data-ticker={r.ticker} onClick={() => open(r.ticker)}>{r.ticker}</span>{num(r.close)}
      <span className={dirClass(r.change_pct)}>{pct(r.change_pct)}</span>
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
