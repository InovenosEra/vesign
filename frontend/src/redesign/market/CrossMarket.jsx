/* Cross-market strip (FX / rates / commodities / crypto). Ported verbatim. */
import { useQuery } from '@tanstack/react-query'
import { getCrossMarket } from '../../api'
import { num, pct, dirClass } from '../fmt'

export default function CrossMarket() {
  const { data } = useQuery({ queryKey: ['market-cross'], queryFn: getCrossMarket, refetchInterval: 60_000 })
  const cross = data?.cross || []
  if (!cross.length) return null
  return (
    <div className="cross-market" style={{ gridTemplateColumns: `repeat(${cross.length}, 1fr)` }}>
      {cross.map((r, i) => (
        <div className="cm-cell" key={i}>
          <div className="lbl">{r.label}{r.stale && <span style={{ opacity: 0.6, fontSize: 9 }}> stale</span>}</div>
          <div className="val">{r.price == null ? '—' : num(r.price, { fd: r.price < 10 ? 4 : 2 })}</div>
          <div className={'delta ' + dirClass(r.change_pct)}>{pct(r.change_pct)}</div>
        </div>
      ))}
    </div>
  )
}
