/* One news card — image, optional ticker tag, headline, source · age.
 * Used both in the carousel (All view) and the flat grid (filtered view). */
import { newsSrc, openUrl, bgImg } from './newsUtil'

export default function NewsCard({ n }) {
  return (
    <div className="news-card" onClick={() => openUrl(n.url)}>
      <div className="img" style={bgImg(n.image)} />
      <div className="c-title">{n.title}</div>
      <div className="c-src">
        {n.ticker && <span className="news-card-tk">{n.ticker}</span>}
        {newsSrc(n)}
      </div>
    </div>
  )
}
