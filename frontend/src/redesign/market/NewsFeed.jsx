/* News feed — a hero article + a horizontal carousel of cards (image + headline),
 * with prev/next arrows and a "Show more" that loads more. Articles open the
 * source in a new tab. Images/summaries come from FMP via /api/market/news/top. */
import { useState, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getTopNews } from '../../api'

const ago = (m) => m == null ? '' : m < 60 ? `${m}m ago` : m < 1440 ? `${Math.floor(m / 60)}h ago` : `${Math.floor(m / 1440)}d ago`
const openUrl = (url) => { if (url) window.open(url, '_blank', 'noopener') }
const bg = (img) => (img ? { backgroundImage: `url(${img})` } : undefined)
const src = (n) => `${n.source || ''}${n.age_minutes != null ? ` · ${ago(n.age_minutes)}` : ''}`

export default function NewsFeed() {
  const [limit, setLimit] = useState(17)
  const { data } = useQuery({ queryKey: ['market-news-feed', limit], queryFn: () => getTopNews(limit), refetchInterval: 300_000 })
  const news = data?.news || []
  const trackRef = useRef(null)
  if (!news.length) return null
  const [hero, ...rest] = news
  const scroll = (dir) => {
    const track = trackRef.current
    if (!track) return
    const card = track.querySelector('.news-card')
    const step = card ? card.offsetWidth + 16 : track.clientWidth  // card width + gap
    track.scrollBy({ left: dir * step * 4, behavior: 'smooth' })   // advance a full page of 4
  }

  return (
    <div className="news-feed">
      <div className="section-h"><h2>Latest News</h2><span className="sub">Market headlines · live</span></div>

      <div className="news-hero" onClick={() => openUrl(hero.url)}>
        <div className="img" style={bg(hero.image)} />
        <div className="content">
          <div className="h-title">{hero.title}</div>
          {hero.summary && <div className="h-summary">{hero.summary}</div>}
          <div className="h-src">{src(hero)}</div>
        </div>
      </div>

      <div className="news-carousel">
        <button className="news-arrow left" onClick={() => scroll(-1)} aria-label="Previous">‹</button>
        <div className="news-cards" ref={trackRef}>
          {rest.map((n, i) => (
            <div className="news-card" key={i} onClick={() => openUrl(n.url)}>
              <div className="img" style={bg(n.image)} />
              <div className="c-title">{n.title}</div>
              <div className="c-src">{src(n)}</div>
            </div>
          ))}
        </div>
        <button className="news-arrow right" onClick={() => scroll(1)} aria-label="Next">›</button>
      </div>

      <div className="news-more"><a onClick={() => setLimit(l => l + 12)}>Show more news →</a></div>
    </div>
  )
}
