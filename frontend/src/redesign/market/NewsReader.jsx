/* Right-side reading drawer for a single news story. We only have FMP's short
 * snippet (no full body), so this is a rich detail view — big image, ticker,
 * headline, summary — with "Read full article" linking out for the full text.
 * Related = other loaded stories sharing the same ticker; clicking one swaps
 * the drawer content in place. Esc / backdrop-click / × all close. */
import { useEffect, useMemo, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import NewsRow from './NewsRow'
import { newsSrc, openUrl, bgImg } from './newsUtil'

export default function NewsReader({ story, allNews, onOpen, onClose }) {
  const navigate = useNavigate()
  const panelRef = useRef(null)

  // Esc to close + lock background scroll while open.
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', onKey)
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => { document.removeEventListener('keydown', onKey); document.body.style.overflow = prev }
  }, [onClose])

  // Scroll back to top whenever the shown story changes (e.g. related-swap).
  useEffect(() => { panelRef.current?.scrollTo({ top: 0 }) }, [story.url])

  const related = useMemo(() => {
    if (!story.ticker) return []
    return (allNews || [])
      .filter((n) => n.ticker === story.ticker && (n.url || n.title) !== (story.url || story.title))
      .slice(0, 5)
  }, [allNews, story])

  const goResearch = () => { onClose(); navigate(`/research/${story.ticker}`) }

  return (
    <div className="news-reader-backdrop" onClick={onClose}>
      <aside className="news-reader" ref={panelRef} onClick={(e) => e.stopPropagation()}>
        <button className="nr-close" onClick={onClose} aria-label="Close">×</button>

        {story.image && <div className="nr-img" style={bgImg(story.image)} />}

        {story.ticker && (
          <button className="nr-ticker" onClick={goResearch} title={`Open ${story.ticker} research`}>
            <img src={`/logos/${story.ticker}.png`} alt="" onError={(e) => { e.currentTarget.style.display = 'none' }} />
            <span>{story.ticker}</span>
          </button>
        )}

        <h2 className="nr-title">{story.title}</h2>
        <div className="nr-meta">{newsSrc(story)}</div>

        {story.summary && <p className="nr-summary">{story.summary}</p>}

        {story.url && (
          <button className="nr-read" onClick={() => openUrl(story.url)}>Read full article →</button>
        )}

        {related.length > 0 && (
          <div className="nr-related">
            <div className="nr-related-h">More on {story.ticker}</div>
            <div className="nh-list nr-related-list">
              {related.map((n) => <NewsRow key={n.url || n.title} n={n} onOpen={onOpen} />)}
            </div>
          </div>
        )}
      </aside>
    </div>
  )
}
