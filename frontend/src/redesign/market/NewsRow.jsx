/* Compact news row for the "More Headlines" list below the carousel:
 * thumbnail + headline + ticker tag + source · age. (nh-* classes avoid a
 * collision with the legacy .news-row used by the analyst/news block.)
 * Clicking opens the in-app reading drawer via onOpen(n). */
import { newsSrc, bgImg } from './newsUtil'

export default function NewsRow({ n, onOpen }) {
  return (
    <div className="nh-row" onClick={() => onOpen(n)}>
      <div className="nh-thumb" style={bgImg(n.image)} />
      <div className="nh-body">
        <div className="nh-title">{n.title}</div>
        <div className="nh-meta">
          {n.ticker && <span className="news-card-tk">{n.ticker}</span>}
          {newsSrc(n)}
        </div>
      </div>
    </div>
  )
}
