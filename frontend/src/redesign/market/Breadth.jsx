/* Market breadth — how broad today's move is: advancers vs decliners, new
 * 52-week highs vs lows, and % of stocks above their 50-day average. */
import { useQuery } from '@tanstack/react-query'
import { getBreadth } from '../../api'

function Bar({ up, down }) {
  const tot = (up + down) || 1
  return (
    <div className="bd-bar">
      <div className="a" style={{ width: (up / tot * 100).toFixed(1) + '%' }} />
      <div className="d" style={{ width: (down / tot * 100).toFixed(1) + '%' }} />
    </div>
  )
}

const fmtN = (n) => (n ?? 0).toLocaleString()

export default function Breadth() {
  const { data } = useQuery({ queryKey: ['market-breadth'], queryFn: getBreadth, refetchInterval: 60_000 })
  if (!data) return null
  const adv = data.advancers ?? 0, dec = data.decliners ?? 0
  const hi = data.week52_highs ?? 0, lo = data.week52_lows ?? 0
  const ma = data.above_50d_ma_pct != null ? data.above_50d_ma_pct * 100 : null
  const ma200 = data.above_200d_ma_pct != null ? data.above_200d_ma_pct * 100 : null
  return (
    <>
      <div className="section-h"><h2>Market Breadth</h2><span className="sub">US market · last close</span></div>
      <div className="breadth">
        <div className="bd-cell">
          <div className="bd-k">Advancers / Decliners</div>
          <Bar up={adv} down={dec} />
          <div className="bd-nums"><span className="up">{fmtN(adv)} advancing</span><span className="down">{fmtN(dec)} declining</span></div>
        </div>
        <div className="bd-cell">
          <div className="bd-k">New 52-week highs / lows</div>
          <Bar up={hi} down={lo} />
          <div className="bd-nums"><span className="up">{fmtN(hi)} highs</span><span className="down">{fmtN(lo)} lows</span></div>
        </div>
        <div className="bd-cell">
          <div className="bd-k">Above 50-day average</div>
          <div className="bd-big">{ma == null ? '—' : ma.toFixed(0) + '%'}</div>
          {ma != null && (
            <div className="bd-bar"><div className="a" style={{ width: ma.toFixed(1) + '%' }} /><div className="d" style={{ width: (100 - ma).toFixed(1) + '%' }} /></div>
          )}
        </div>
        <div className="bd-cell">
          <div className="bd-k">Above 200-day average</div>
          <div className="bd-big">{ma200 == null ? '—' : ma200.toFixed(0) + '%'}</div>
          {ma200 != null && (
            <div className="bd-bar"><div className="a" style={{ width: ma200.toFixed(1) + '%' }} /><div className="d" style={{ width: (100 - ma200).toFixed(1) + '%' }} /></div>
          )}
        </div>
      </div>
    </>
  )
}
