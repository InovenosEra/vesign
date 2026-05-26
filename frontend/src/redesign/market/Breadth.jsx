/* Market breadth — how broad today's move is. Six uniform cells (label →
 * split bar → counts) so the bars line up vertically. */
import { useQuery } from '@tanstack/react-query'
import { getBreadth, getSectors } from '../../api'

const fmtN = (n) => (n ?? 0).toLocaleString()
const volB = (v) => v == null ? '—'
  : v >= 1e9 ? (v / 1e9).toFixed(1) + 'B'
  : v >= 1e6 ? (v / 1e6).toFixed(0) + 'M'
  : (v / 1e3).toFixed(0) + 'K'

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
  const { data } = useQuery({ queryKey: ['market-breadth'], queryFn: getBreadth, refetchInterval: 3_000 })
  const { data: sec } = useQuery({ queryKey: ['market-sectors'], queryFn: getSectors, refetchInterval: 3_000 })
  if (!data) return null
  const adv = data.advancers ?? 0, dec = data.decliners ?? 0
  const upv = data.up_volume ?? 0, dnv = data.down_volume ?? 0
  const hi = data.week52_highs ?? 0, lo = data.week52_lows ?? 0
  const ma = data.above_50d_ma_pct != null ? data.above_50d_ma_pct * 100 : null
  const ma200 = data.above_200d_ma_pct != null ? data.above_200d_ma_pct * 100 : null

  const sectors = (sec?.sectors || []).filter(s => s.sector !== 'ETF')
  const secUp = sectors.filter(s => (s.change_pct ?? 0) > 0).length
  const secDn = sectors.filter(s => (s.change_pct ?? 0) < 0).length
  const secTot = sectors.length

  return (
    <>
      <div className="section-h"><h2>Market Breadth</h2><span className="sub">US market · live</span></div>
      <div className="breadth">
        <Cell label="Advancers / Decliners"
          up={adv} down={dec} leftNum={`${fmtN(adv)} advancing`} rightNum={`${fmtN(dec)} declining`} />
        <Cell label="Up / Down volume"
          up={upv} down={dnv} leftNum={`${volB(upv)} up`} rightNum={`${volB(dnv)} down`} />
        <Cell label={`Sectors advancing${secTot ? ` (of ${secTot})` : ''}`}
          up={secUp} down={secDn} leftNum={`${secUp} advancing`} rightNum={`${secDn} declining`} />
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
