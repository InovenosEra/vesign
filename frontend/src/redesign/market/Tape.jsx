/* Tape — top-20 by market cap. Live during market hours: /api/prices/live is
 * overlaid (~1s) on the stored close, change recomputed vs derived prev-close. */
import { useQuery } from '@tanstack/react-query'
import { getTape } from '../../api'
import { useLivePrices } from '../../hooks/useLivePrices'
import { num, pct, dirClass, overlayLive } from '../fmt'
import { useTickerModal } from '../TickerModalContext'

export default function Tape() {
  const open = useTickerModal()
  const { data } = useQuery({ queryKey: ['market-tape'], queryFn: getTape, refetchInterval: 60_000 })
  const items = (data?.tape || []).filter(r => r.close != null)
  const { prices } = useLivePrices(items.map(r => r.ticker))

  const Item = (r, k) => {
    const { price, change } = overlayLive(r.close, r.change_pct, prices[r.ticker])
    return (
      <span className="tape-item" key={k}>
        <span className="tk" data-ticker={r.ticker} onClick={() => open(r.ticker)}>{r.ticker}</span>{num(price)}
        <span className={dirClass(change)}>{pct(change)}</span>
      </span>
    )
  }

  return (
    <div className="tape">
      <div className="tape-track">
        {items.map((r, i) => Item(r, 'a' + i))}
        {items.map((r, i) => Item(r, 'b' + i))}
      </div>
    </div>
  )
}
