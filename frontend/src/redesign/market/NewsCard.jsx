/* One news card — image, optional ticker tag, headline, source · age.
 * Used both in the carousel (All view) and the flat grid (filtered view).
 * Clicking opens the in-app reading drawer via onOpen(n). */
import { newsSrc, bgImg } from './newsUtil'

export default function NewsCard({ n, onOpen }) {
  return (
    <div className="news-card" onClick={() => onOpen(n)}>
      <div className="img" style={bgImg(n.image)} />
      <div className="c-title">{n.title}</div>
      <div className="c-src">
        {n.ticker && <span className="news-card-tk">{n.ticker}</span>}
        {newsSrc(n)}
      </div>
    </div>
  )
}
