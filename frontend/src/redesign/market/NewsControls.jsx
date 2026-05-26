/* News control bar: category chips (All/Macro/Stocks/My Stocks) + search box +
 * a "N new" pill (when a refresh brings newer stories) + a row of trending
 * ticker chips (most-mentioned in the live feed). All filtering is client-side;
 * this component is purely presentational — state lives in NewsFeed. */

const CATS = [['all', 'All'], ['macro', 'Macro'], ['stocks', 'Stocks'], ['mine', 'My Stocks']]

export default function NewsControls({
  cat, setCat, q, setQ, ticker, setTicker, trending, newCount, onCatchUp, hasMine,
}) {
  const pickCat = (k) => { setTicker(null); setCat(k) }
  const pickTicker = (t) => { setCat('all'); setTicker(ticker === t ? null : t) }

  return (
    <div className="news-controls">
      <div className="news-chips">
        {CATS.map(([k, label]) => (
          (k !== 'mine' || hasMine) && (
            <button
              key={k}
              className={'news-chip' + (!ticker && cat === k ? ' active' : '')}
              onClick={() => pickCat(k)}
            >{label}</button>
          )
        ))}
      </div>

      <div className="news-controls-right">
        {newCount > 0 && (
          <button className="news-new-pill" onClick={onCatchUp}>↑ {newCount} new</button>
        )}
        <input
          className="news-search"
          type="search"
          placeholder="Search headlines…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
      </div>

      {ticker && (
        <div className="news-active-tk">
          Showing <span>{ticker}</span>
          <button onClick={() => setTicker(null)} aria-label="Clear ticker filter">✕</button>
        </div>
      )}

      {trending.length > 0 && (
        <div className="news-trending">
          <span className="nt-label">Trending</span>
          {trending.map((t) => (
            <button
              key={t.ticker}
              className={'news-tk-chip' + (ticker === t.ticker ? ' active' : '')}
              onClick={() => pickTicker(t.ticker)}
            >{t.ticker}<em>{t.count}</em></button>
          ))}
        </div>
      )}
    </div>
  )
}
