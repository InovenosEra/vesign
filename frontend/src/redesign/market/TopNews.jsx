/* Top news panel. Ported from market-v1.html. Ticker chip → signal modal. */
import { useQuery } from '@tanstack/react-query'
import { getTopNews } from '../../api'
import { useTickerModal } from '../TickerModalContext'

function ageLabel(m) {
  if (m == null) return ''
  if (m < 60) return `${m} min ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h} h ago`
  return `${Math.floor(h / 24)} d ago`
}

export default function TopNews() {
  const open = useTickerModal()
  const { data } = useQuery({ queryKey: ['market-news-top'], queryFn: () => getTopNews(5), refetchInterval: 300_000 })
  const news = data?.news || []
  return (
    <div className="news-panel">
      <div className="news-head"><h3>Top news</h3><span className="day">Live</span></div>
      <div className="cal-list">
        {news.map((n, i) => (
          <div className="news-row" key={i} data-ticker={n.ticker || ''} onClick={() => n.ticker && open(n.ticker)}>
            <div>
              <div className="headline">{n.title}</div>
              <div className="meta">{n.ticker && <span className="tk">{n.ticker}</span>}<span className="src">{n.source || ''}</span></div>
            </div>
            <div className="time">{ageLabel(n.age_minutes)}</div>
          </div>
        ))}
      </div>
    </div>
  )
}
