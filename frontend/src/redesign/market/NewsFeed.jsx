/* News feed. Default ("All") shows an editorial hero + a 6-card carousel; any
 * active filter/search switches to a flat card grid. Filters (category, trending
 * ticker, search) are all client-side over the loaded feed. "My Stocks" = items
 * whose ticker is in the user's holdings. A "N new" pill flags fresher stories
 * arriving from the background refresh. Data: /api/market/news/top + holdings. */
import { useState, useRef, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getTopNews, getPortfolioHoldings } from '../../api'
import NewsControls from './NewsControls'
import NewsCard from './NewsCard'
import { newsSrc, openUrl, bgImg } from './newsUtil'

export default function NewsFeed() {
  const [limit, setLimit] = useState(31)
  const [cat, setCat] = useState('all')       // all | macro | stocks | mine
  const [ticker, setTicker] = useState(null)  // trending-chip filter (overrides cat)
  const [q, setQ] = useState('')              // search text
  const [seenTs, setSeenTs] = useState(null)  // newest published_at the user has "caught up" to

  const { data } = useQuery({ queryKey: ['market-news-feed', limit], queryFn: () => getTopNews(limit), refetchInterval: 300_000 })
  const { data: holdings } = useQuery({ queryKey: ['portfolio-holdings'], queryFn: () => getPortfolioHoldings('US') })
  const news = data?.news || []
  const rootRef = useRef(null)
  const trackRef = useRef(null)

  const mySet = useMemo(() => new Set((holdings || []).map((h) => h.ticker)), [holdings])

  // Trending = most-mentioned tickers in the loaded feed (top 8).
  const trending = useMemo(() => {
    const counts = {}
    for (const n of news) if (n.ticker) counts[n.ticker] = (counts[n.ticker] || 0) + 1
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 8)
      .map(([t, c]) => ({ ticker: t, count: c }))
  }, [news])

  // Seed the "seen" mark to the newest story on first load (so the pill starts at 0).
  const newestTs = news[0]?.published_at || null
  useEffect(() => {
    if (seenTs === null && newestTs) setSeenTs(newestTs)
  }, [newestTs, seenTs])
  // published_at is FMP's "YYYY-MM-DD HH:MM:SS" — same format across items, so
  // a lexicographic compare is chronological.
  const newCount = useMemo(
    () => (seenTs ? news.filter((n) => n.published_at && n.published_at > seenTs).length : 0),
    [news, seenTs],
  )

  const filtered = useMemo(() => {
    let arr = news
    if (ticker) arr = arr.filter((n) => n.ticker === ticker)
    else if (cat === 'macro') arr = arr.filter((n) => !n.ticker)
    else if (cat === 'stocks') arr = arr.filter((n) => n.ticker)
    else if (cat === 'mine') arr = arr.filter((n) => n.ticker && mySet.has(n.ticker))
    const s = q.trim().toLowerCase()
    if (s) arr = arr.filter((n) =>
      (n.title || '').toLowerCase().includes(s) ||
      (n.source || '').toLowerCase().includes(s) ||
      (n.ticker || '').toLowerCase().includes(s))
    return arr
  }, [news, cat, ticker, q, mySet])

  const isFiltered = Boolean(ticker) || cat !== 'all' || Boolean(q.trim())

  const catchUp = () => {
    setSeenTs(newestTs)
    rootRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }
  const clearFilters = () => { setCat('all'); setTicker(null); setQ('') }
  const scroll = (dir) => {
    const t = trackRef.current
    if (t) t.scrollBy({ left: dir * t.clientWidth, behavior: 'smooth' })
  }

  if (!news.length) return null

  const [hero, ...rest] = filtered

  return (
    <div className="news-feed" ref={rootRef}>
      <div className="section-h"><h2>Latest News</h2><span className="sub">Market headlines · live</span></div>

      <NewsControls
        cat={cat} setCat={setCat} q={q} setQ={setQ}
        ticker={ticker} setTicker={setTicker} trending={trending}
        newCount={newCount} onCatchUp={catchUp} hasMine={mySet.size > 0}
      />

      {filtered.length === 0 ? (
        <div className="news-empty">No stories match. <a onClick={clearFilters}>Clear filters</a></div>
      ) : isFiltered ? (
        <div className="news-grid">
          {filtered.map((n) => <NewsCard key={n.url || n.title} n={n} />)}
        </div>
      ) : (
        <>
          <div className="news-hero" onClick={() => openUrl(hero.url)}>
            <div className="img" style={bgImg(hero.image)} />
            <div className="content">
              <div className="h-title">{hero.title}</div>
              {hero.summary && <div className="h-summary">{hero.summary}</div>}
              <div className="h-src">{newsSrc(hero)}</div>
            </div>
          </div>
          <div className="news-carousel">
            <button className="news-arrow left" onClick={() => scroll(-1)} aria-label="Previous">‹</button>
            <div className="news-cards" ref={trackRef}>
              {rest.map((n) => <NewsCard key={n.url || n.title} n={n} />)}
            </div>
            <button className="news-arrow right" onClick={() => scroll(1)} aria-label="Next">›</button>
          </div>
        </>
      )}

      <div className="news-more"><a onClick={() => setLimit((l) => l + 20)}>Show more news →</a></div>
    </div>
  )
}
