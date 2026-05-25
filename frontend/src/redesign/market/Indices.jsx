/* Indices row — idx-cards with faded area sparkline. Ported from market-v1.html. */
import { useQuery } from '@tanstack/react-query'
import { getIndices } from '../../api'
import { useLivePrices } from '../../hooks/useLivePrices'
import { num, pct, spark, overlayLive } from '../fmt'

const LABELS = { '^GSPC': 'S&P 500', '^NDX': 'Nasdaq 100', '^DJI': 'Dow Jones', '^RUT': 'Russell 2000', VIX: 'VIX' }

export default function Indices() {
  const { data } = useQuery({ queryKey: ['market-indices'], queryFn: getIndices, refetchInterval: 60_000 })
  const rows = data?.indices || []
  const { prices } = useLivePrices(rows.map(r => r.ticker))

  return (
    <div className="indices">
      {rows.map((row, i) => {
        const { price, change } = overlayLive(row.close, row.change_pct, prices[row.ticker])
        const cls = change == null ? '' : change >= 0 ? 'up' : 'down'
        const color = row.ticker === 'VIX'
          ? (change >= 0 ? '#ff4d5c' : '#00d97e')   // VIX inverted
          : (change >= 0 ? '#00d97e' : '#ff4d5c')
        const d = spark(row.sparkline || [], { width: 220, height: 60 })
        const gid = `g_${i}`
        const abs = price != null && change != null
          ? num(price * change / 100, { fd: 2 }) : '—'
        const name = LABELS[row.ticker] || row.ticker
        return (
          <div className="idx-card" key={row.ticker} data-name={name} style={{ cursor: 'default' }}>
            <div className="name">{name}</div>
            <div className="price">{price == null ? '—' : num(price, { fd: 2 })}</div>
            <div className={'change ' + cls}>
              <span className="pct">{pct(change)}</span>
              <span className="abs">{abs}</span>
            </div>
            <div className="spark">
              <svg viewBox="0 0 220 60" preserveAspectRatio="none">
                <defs><linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={color} stopOpacity="0.30" />
                  <stop offset="100%" stopColor={color} stopOpacity="0" />
                </linearGradient></defs>
                <path d={d ? d + ' L220,60 L0,60 Z' : ''} fill={`url(#${gid})`} />
                <path d={d} fill="none" stroke={color} strokeWidth="1.5" />
              </svg>
            </div>
          </div>
        )
      })}
    </div>
  )
}
