/* Market breadth — how broad today's move is. Four uniform cells (label →
 * split bar → counts) so the bars line up vertically. */
import { useQuery } from '@tanstack/react-query'
import { getBreadth } from '../../api'

const fmtN = (n) => (n ?? 0).toLocaleString()

function Cell({ label, up, down, leftNum, rightNum }) {
  const tot = (up + down) || 1
  return (
    <div className="bd-cell">
      <div className="bd-k">{label}</div>
      <div className="bd-bar">
        <div className="a" style={{ width: (up / tot * 100).toFixed(1) + '%' }} />
        <div className="d" style={{ width: (down / tot * 100).toFixed(1) + '%' }} />
      </div>
      <div className="bd-nums"><span className="up">{leftNum}</span><span className="down">{rightNum}</span></div>
    </div>
  )
}

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
        <Cell label="Advancers / Decliners"
          up={adv} down={dec} leftNum={`${fmtN(adv)} advancing`} rightNum={`${fmtN(dec)} declining`} />
        <Cell label="New 52-week highs / lows"
          up={hi} down={lo} leftNum={`${fmtN(hi)} highs`} rightNum={`${fmtN(lo)} lows`} />
        <Cell label="Above 50-day average"
          up={ma ?? 0} down={ma == null ? 0 : 100 - ma}
          leftNum={ma == null ? '—' : `${ma.toFixed(0)}% above`} rightNum={ma == null ? '' : `${(100 - ma).toFixed(0)}% below`} />
        <Cell label="Above 200-day average"
          up={ma200 ?? 0} down={ma200 == null ? 0 : 100 - ma200}
          leftNum={ma200 == null ? '—' : `${ma200.toFixed(0)}% above`} rightNum={ma200 == null ? '' : `${(100 - ma200).toFixed(0)}% below`} />
      </div>
    </>
  )
}
