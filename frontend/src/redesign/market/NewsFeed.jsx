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
import NewsRow from './NewsRow'
import NewsReader from './NewsReader'
import { newsSrc, bgImg } from './newsUtil'

function NewsSkeleton() {
  return (
    <div className="news-feed" aria-busy="true">
      <div className="rd-skel ov-skel-h" style={{ marginBottom: 24 }} />
      <div className="news-hero" style={{ cursor: 'default' }}>
        <div className="rd-skel" style={{ height: 270 }} />
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div className="rd-skel" style={{ height: 13, width: 90 }} />
          <div className="rd-skel" style={{ height: 26, width: '95%' }} />
          <div className="rd-skel" style={{ height: 26, width: '70%' }} />
          <div className="rd-skel" style={{ height: 13, width: '100%', marginTop: 8 }} />
          <div className="rd-skel" style={{ height: 13, width: '100%' }} />
          <div className="rd-skel" style={{ height: 13, width: '85%' }} />
        </div>
      </div>
      <div className="news-cards" style={{ overflow: 'hidden' }}>
        {Array.from({ length: 7 }).map((_, i) => (
          <div key={i} className="news-card" style={{ cursor: 'default' }}>
            <div className="rd-skel" style={{ height: 190 }} />
            <div className="rd-skel" style={{ height: 13, width: '100%', marginTop: 10 }} />
            <div className="rd-skel" style={{ height: 13, width: '60%', marginTop: 6 }} />
          </div>
        ))}
      </div>
    </div>
  )
}

export default function NewsFeed() {
  const [limit, setLimit] = useState(100)
  const [listCount, setListCount] = useState(20)  // More Headlines rows (10 per column)
  const [cat, setCat] = useState('all')       // all | macro | stocks | mine
  const [ticker, setTicker] = useState(null)  // trending-chip filter (overrides cat)
  const [q, setQ] = useState('')              // search text
  const [seenTs, setSeenTs] = useState(null)  // newest published_at the user has "caught up" to
  const [reader, setReader] = useState(null)  // story shown in the reading drawer (null = closed)

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
  const loading = data === undefined

  const catchUp = () => {
    setSeenTs(newestTs)
    rootRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }
  const clearFilters = () => { setCat('all'); setTicker(null); setQ('') }
  const scroll = (dir) => {
    const t = trackRef.current
    if (t) t.scrollBy({ left: dir * t.clientWidth, behavior: 'smooth' })
  }
  const showMore = () => {
    if (isFiltered) { setLimit((l) => l + 30); return }   // grid view: fetch more
    const next = listCount + 20                            // All view: reveal 20 more rows
    setListCount(next)
    if (13 + next > limit) setLimit(13 + next + 13)        // keep enough fetched ahead
  }

  // While the feed is loading, paint a skeleton — NOT null. An empty .body lets
  // the header's pulsing GPU layers (status dot/icon) leave a Chrome compositing
  // ghost-smear on the bare page background; the skeleton covers it (matches the
  // Overview tab, which never renders a bare body during load).
  if (loading) return <NewsSkeleton />
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
          {filtered.map((n) => <NewsCard key={n.url || n.title} n={n} onOpen={setReader} />)}
        </div>
      ) : (
        <>
          <div className="news-hero" onClick={() => setReader(hero)}>
            <div className="img" style={bgImg(hero.image)} />
            <div className="content">
              <div className="h-eyebrow">Top Story</div>
              <div className="h-title">{hero.title}</div>
              {hero.ticker && (
                <div className="h-ticker">
                  <img src={`/logos/${hero.ticker}.png`} alt="" onError={(e) => { e.currentTarget.style.display = 'none' }} />
                  <span>{hero.ticker}</span>
                </div>
              )}
              {hero.summary && <div className="h-summary">{hero.summary}</div>}
              <div className="h-foot">
                <span className="h-src">{newsSrc(hero)}</span>
                <span className="h-read">Read full article →</span>
              </div>
            </div>
          </div>
          <div className="news-carousel">
            <button className="news-arrow left" onClick={() => scroll(-1)} aria-label="Previous">‹</button>
            <div className="news-cards" ref={trackRef}>
              {rest.slice(0, 13).map((n) => <NewsCard key={n.url || n.title} n={n} onOpen={setReader} />)}
            </div>
            <button className="news-arrow right" onClick={() => scroll(1)} aria-label="Next">›</button>
          </div>
          {rest.length > 13 && (
            <>
              <div className="nh-h">More Headlines</div>
              <div className="nh-list">
                {rest.slice(13, 13 + listCount).map((n) => <NewsRow key={n.url || n.title} n={n} onOpen={setReader} />)}
              </div>
            </>
          )}
        </>
      )}

      <div className="news-more"><a onClick={showMore}>Show more news →</a></div>

      {reader && (
        <NewsReader story={reader} allNews={news} onOpen={setReader} onClose={() => setReader(null)} />
      )}
    </div>
  )
}
