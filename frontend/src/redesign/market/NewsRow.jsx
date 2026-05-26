/* Compact news row for the "More Headlines" list below the carousel:
 * thumbnail + headline + ticker tag + source · age. */
import { newsSrc, openUrl, bgImg } from './newsUtil'

export default function NewsRow({ n }) {
  return (
    <div className="news-row" onClick={() => openUrl(n.url)}>
      <div className="thumb" style={bgImg(n.image)} />
      <div className="news-row-body">
        <div className="nr-title">{n.title}</div>
        <div className="nr-meta">
          {n.ticker && <span className="news-card-tk">{n.ticker}</span>}
          {newsSrc(n)}
        </div>
      </div>
    </div>
  )
}
