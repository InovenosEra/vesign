/* Page-head context strip: date · NYSE · S&P 500 / Breadth / 52w hi-lo / VIX.
 * SPY + VIX chips come from the indices payload; A/D + 52w from breadth. */
import { useQuery } from '@tanstack/react-query'
import { getBreadth, getIndices } from '../../api'
import { num, pct, dirClass } from '../fmt'

export default function PageHead() {
  const { data: breadth } = useQuery({ queryKey: ['market-breadth'], queryFn: getBreadth, refetchInterval: 60_000 })
  const { data: idx } = useQuery({ queryKey: ['market-indices'], queryFn: getIndices, refetchInterval: 60_000 })
  const by = Object.fromEntries((idx?.indices || []).map(r => [r.ticker, r]))
  const spy = by.SPY, vix = by.VIX
  const day = new Date().toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })

  return (
    <div className="page-head">
      <div className="ph-left">
        <span className="day">{day}</span>
        <span className="sep">·</span>
        <span className="status-open">NYSE</span>
      </div>
      <div className="ph-breadth">
        <div className="b-item"><div className="l">S&amp;P 500</div>
          <div className={'v ' + (spy ? dirClass(spy.change_pct) : '')}>{spy ? pct(spy.change_pct) : '—'}</div></div>
        <div className="b-item"><div className="l">Breadth</div>
          <div className="v">{breadth ? <><span className="up">{breadth.advancers}</span> / <span className="down">{breadth.decliners}</span></> : '—'}</div></div>
        <div className="b-item"><div className="l">52w highs</div>
          <div className="v">{breadth?.week52_highs ?? '—'}</div></div>
        <div className="b-item"><div className="l">52w lows</div>
          <div className="v">{breadth?.week52_lows ?? '—'}</div></div>
        <div className="b-item"><div className="l">VIX</div>
          <div className={'v ' + (vix ? dirClass(vix.change_pct) : '')}>{vix ? (vix.close == null ? '—' : num(vix.close, { fd: 2 })) : '—'}</div></div>
      </div>
    </div>
  )
}
