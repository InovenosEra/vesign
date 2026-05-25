/* Screener tab — filter rail + ranked results table.
 * Ported from research-v1.html's renderScreener()/initSlider() inline JS.
 * Data: signals/today (US). Rows open the shared SignalModal (matching the
 * mockup's body click → openSignalModal). Filters/sliders are React state. */
import { useState, useRef, useCallback, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSignalsToday, WHITE_BG_LOGOS } from '../../api'
import { num, pct, dirClass, LOGO } from '../fmt'
import { useCurrency } from '../../context/CurrencyContext'
import { useTickerModal } from '../TickerModalContext'

const SECTOR_ABBR = {
  'Information Technology': 'Tech', 'Communication Services': 'Comm.',
  'Health Care': 'Health', 'Consumer Discretionary': 'Cons.', 'Consumer Staples': 'Staples',
  'Financials': 'Fin.', 'Industrials': 'Indu.', 'Energy': 'Energy', 'Materials': 'Mat.',
  'Real Estate': 'RE', 'Utilities': 'Util.',
}
const KNOWN_SECTORS = new Set(['Information Technology', 'Communication Services', 'Financials',
  'Health Care', 'Industrials', 'Consumer Discretionary', 'Energy', 'Materials'])
const SECTOR_PILLS = [
  ['Information Technology', 'Technology'], ['Communication Services', 'Communication'],
  ['Financials', 'Financials'], ['Health Care', 'Health Care'], ['Industrials', 'Industrials'],
  ['Consumer Discretionary', 'Consumer Disc.'], ['Energy', 'Energy'], ['Materials', 'Materials'],
  ['__other', 'Other'],
]
const CAP_PILLS = [['mega', 'Mega >$200B'], ['large', 'Large $10–200B'], ['mid', 'Mid $2–10B'], ['small', 'Small <$2B']]

const sigCls = (s) => ({ BUY: 'buy', SELL: 'sell' }[s] || 'hold')
const capB = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'
const capBucket = (mc) => mc == null ? null : mc >= 200e9 ? 'mega' : mc >= 10e9 ? 'large' : mc >= 2e9 ? 'mid' : 'small'
const healthDots = (n) => [0, 1, 2, 3, 4].map(i => <span key={i} className={'s' + (i < (n || 0) ? '' : ' off')} />)

/* Dual-handle range slider — pointer-drag, same math as the mockup's initSlider. */
function RangeSlider({ min, max, lo, hi, ticks, fmt, onChange }) {
  const barRef = useRef(null)
  const toPct = (v) => ((v - min) / (max - min)) * 100
  const drag = useCallback((which) => (ev) => {
    ev.preventDefault()
    const rect = barRef.current.getBoundingClientRect()
    const move = (e) => {
      const cx = e.touches ? e.touches[0].clientX : e.clientX
      const frac = Math.max(0, Math.min(1, (cx - rect.left) / rect.width))
      let val = Math.round(min + frac * (max - min))
      if (which === 'lo') val = Math.min(val, hi)
      else val = Math.max(val, lo)
      onChange(which, val)
    }
    const up = () => {
      window.removeEventListener('pointermove', move)
      window.removeEventListener('pointerup', up)
    }
    window.addEventListener('pointermove', move)
    window.addEventListener('pointerup', up)
  }, [min, max, lo, hi, onChange])

  return (
    <>
      <div className="range-vals">
        <span>Min <span className="v">{fmt(lo)}</span></span>
        <span>Max <span className="v">{fmt(hi)}</span></span>
      </div>
      <div className="range-bar" ref={barRef} data-lo={lo} data-hi={hi} data-min={min} data-max={max}>
        <div className="range-fill" style={{ left: toPct(lo) + '%', right: (100 - toPct(hi)) + '%' }} />
        <div className="range-handle" data-h="lo" style={{ left: toPct(lo) + '%' }} onPointerDown={drag('lo')} />
        <div className="range-handle" data-h="hi" style={{ left: toPct(hi) + '%' }} onPointerDown={drag('hi')} />
      </div>
      <div className="range-ticks">{ticks.map((t, i) => <span key={i}>{t}</span>)}</div>
    </>
  )
}

const DEFAULTS = {
  signals: new Set(['BUY', 'SELL', 'HOLD']),
  sectors: new Set(SECTOR_PILLS.map(([v]) => v)),
  caps: new Set(CAP_PILLS.map(([v]) => v)),
  health: 1,
  dir: 'any',
  vqs: { lo: 0, hi: 10 }, upside: { lo: -50, hi: 100 }, pe: { lo: 0, hi: 100 },
  search: '',
}

export default function Screener({ onCount }) {
  const openTicker = useTickerModal()
  const { fmtPrice } = useCurrency()
  const { data: rows } = useQuery({ queryKey: ['signals-today', 'US'], queryFn: () => getSignalsToday(null, 'US') })
  const all = Array.isArray(rows) ? rows : []

  const [signals, setSignals] = useState(new Set(DEFAULTS.signals))
  const [sectors, setSectors] = useState(new Set(DEFAULTS.sectors))
  const [caps, setCaps] = useState(new Set(DEFAULTS.caps))
  const [health, setHealth] = useState(DEFAULTS.health)
  const [dir, setDir] = useState(DEFAULTS.dir)
  const [vqs, setVqs] = useState({ ...DEFAULTS.vqs })
  const [upside, setUpside] = useState({ ...DEFAULTS.upside })
  const [pe, setPe] = useState({ ...DEFAULTS.pe })
  const [search, setSearch] = useState(DEFAULTS.search)

  const toggle = (setFn) => (key) => setFn(prev => {
    const next = new Set(prev)
    next.has(key) ? next.delete(key) : next.add(key)
    return next
  })
  const togSignal = toggle(setSignals), togSector = toggle(setSectors), togCap = toggle(setCaps)

  const reset = () => {
    setSignals(new Set(DEFAULTS.signals)); setSectors(new Set(DEFAULTS.sectors))
    setCaps(new Set(DEFAULTS.caps)); setHealth(DEFAULTS.health); setDir(DEFAULTS.dir)
    setVqs({ ...DEFAULTS.vqs }); setUpside({ ...DEFAULTS.upside }); setPe({ ...DEFAULTS.pe })
    setSearch('')
  }

  // Treat slider extremes as unbounded (matches mockup's -Infinity/Infinity).
  const vqsLo = vqs.lo <= 0 ? -Infinity : vqs.lo
  const vqsHi = vqs.hi >= 10 ? Infinity : vqs.hi
  const upLo = upside.lo <= -50 ? -Infinity : upside.lo
  const upHi = upside.hi >= 100 ? Infinity : upside.hi
  const peLo = pe.lo <= 0 ? -Infinity : pe.lo
  const peHi = pe.hi >= 100 ? Infinity : pe.hi
  const q = search.trim().toUpperCase()

  const filtered = all.filter(r => {
    if (signals.size && !signals.has(r.signal)) return false
    if (sectors.size) {
      const sec = KNOWN_SECTORS.has(r.sector) ? r.sector : '__other'
      if (!sectors.has(sec)) return false
    }
    if (caps.size && caps.size < CAP_PILLS.length) {
      const b = capBucket(r.market_cap)
      if (!b || !caps.has(b)) return false
    }
    if (health > 1 && (r.health_score || 0) < health) return false
    const v = r.vqs == null ? 0 : r.vqs
    if (v < vqsLo || v > vqsHi) return false
    if (r.fair_value_upside != null) {
      const up = r.fair_value_upside * 100
      if (up < upLo || up > upHi) return false
    }
    if (r.pe_ttm != null && (r.pe_ttm < peLo || r.pe_ttm > peHi)) return false
    const ml = r.prediction_score
    if (dir === 'up' && !(ml > 0)) return false
    if (dir === 'down' && !(ml < 0)) return false
    if (q && !(`${r.ticker} ${r.company || ''}`.toUpperCase().includes(q))) return false
    return true
  })

  useEffect(() => { onCount && onCount(filtered.length) }, [filtered.length, onCount])

  const ranked = filtered.slice()
    .filter(r => r.fair_value_upside != null)
    .sort((a, b) => b.fair_value_upside - a.fair_value_upside)
    .slice(0, 60)
  const maxUp = Math.max(...ranked.map(r => r.fair_value_upside * 100), 1)

  return (
    <div className="research-layout">
      {/* FILTER RAIL */}
      <aside className="filter-rail">
        <div className="filter-rail-head">
          <h3>Filters</h3>
          <span className="reset" onClick={reset}>Reset all</span>
        </div>

        <div className="fg">
          <div className="fg-label">Search</div>
          <input className="fg-search" type="text" placeholder="Ticker or company..."
            value={search} onChange={(e) => setSearch(e.target.value)} />
        </div>

        <div className="fg">
          <div className="fg-label">Signal</div>
          <div className="pills">
            {['BUY', 'SELL', 'HOLD'].map(s => (
              <span key={s} className={'pill' + (signals.has(s) ? ' active' : '') + (s === 'BUY' ? ' buy' : s === 'SELL' ? ' sell' : '')}
                onClick={() => togSignal(s)}>{s}</span>
            ))}
          </div>
        </div>

        <div className="fg">
          <div className="fg-label">Market cap</div>
          <div className="pills">
            {CAP_PILLS.map(([v, lbl]) => (
              <span key={v} className={'pill' + (caps.has(v) ? ' active' : '')} onClick={() => togCap(v)}>{lbl}</span>
            ))}
          </div>
        </div>

        <div className="fg">
          <div className="fg-label">Sector</div>
          <div className="pills">
            {SECTOR_PILLS.map(([v, lbl]) => (
              <span key={v} className={'pill' + (sectors.has(v) ? ' active' : '')} onClick={() => togSector(v)}>{lbl}</span>
            ))}
          </div>
        </div>

        <div className="fg">
          <div className="fg-label">VQS score</div>
          <RangeSlider min={0} max={10} lo={vqs.lo} hi={vqs.hi}
            ticks={['1', '3', '5', '7', '10']} fmt={(v) => String(v)}
            onChange={(w, val) => setVqs(p => ({ ...p, [w]: val }))} />
        </div>

        <div className="fg">
          <div className="fg-label">Predicted upside</div>
          <RangeSlider min={-50} max={100} lo={upside.lo} hi={upside.hi}
            ticks={['−50%', '0%', '+50%', '+100%']} fmt={(v) => (v >= 0 ? '+' : '') + v + '%'}
            onChange={(w, val) => setUpside(p => ({ ...p, [w]: val }))} />
        </div>

        <div className="fg">
          <div className="fg-label">Health <span className="ct">≥ {health}</span></div>
          <div className="stars">
            {[1, 2, 3, 4, 5].map(n => (
              <span key={n} className={'star' + (n >= health ? ' active' : '') + (health === 1 && n === 1 ? ' bad' : health === 2 && n === 2 ? ' weak' : '')}
                onClick={() => setHealth(n)}>{n}</span>
            ))}
          </div>
        </div>

        <div className="fg">
          <div className="fg-label">P/E ratio <span className="ct" style={{ fontWeight: 400, textTransform: 'none' }}>TTM</span></div>
          <RangeSlider min={0} max={100} lo={pe.lo} hi={pe.hi}
            ticks={['0', '25', '50', '100+']} fmt={(v) => v >= 100 ? '100+' : String(v)}
            onChange={(w, val) => setPe(p => ({ ...p, [w]: val }))} />
        </div>

        <div className="fg">
          <div className="fg-label">Day change <span className="ct" style={{ fontWeight: 400, textTransform: 'none' }}>ML direction</span></div>
          <div className="pills">
            {[['any', 'Any'], ['up', 'Up only'], ['down', 'Down only']].map(([v, lbl]) => (
              <span key={v} className={'pill' + (dir === v ? ' active' : '')} onClick={() => setDir(v)}>{lbl}</span>
            ))}
          </div>
        </div>

        <div className="fg">
          <button className="save-btn">Save filter set</button>
        </div>
      </aside>

      {/* MAIN RESULTS */}
      <div className="results-area">
        <div className="results-toolbar">
          <div className="count"><strong>{filtered.length.toLocaleString()}</strong> tickers · sorted by predicted upside</div>
          <div className="spacer" />
          <div className="pill-btn">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 6h18M6 12h12M10 18h4" /></svg>
            Predicted upside <span className="arr">↓</span>
          </div>
          <div className="pill-btn">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 3h18v18H3z M3 9h18 M9 21V9" /></svg>
            Columns
          </div>
          <div className="pill-btn">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 3v12 M7 10l5 5 5-5 M5 21h14" /></svg>
            Export CSV
          </div>
        </div>

        <table className="data-table">
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Sector</th>
              <th className="r sortable">Price</th>
              <th className="r sortable">Day</th>
              <th className="r sortable">Mkt cap</th>
              <th>Signal</th>
              <th className="r sortable">VQS</th>
              <th className="r sortable">Pred. upside <span className="arr">↓</span></th>
              <th className="r">Health</th>
              <th className="r sortable">ML 5d</th>
              <th className="r sortable">P/E</th>
              <th className="r" style={{ paddingRight: 18 }}>52w high</th>
            </tr>
          </thead>
          <tbody>
            {ranked.length ? ranked.map(r => {
              const up = r.fair_value_upside * 100
              const ml = r.prediction_score == null ? null : r.prediction_score * 100
              const vqsKls = r.vqs >= 8 ? 'high' : r.vqs >= 6 ? 'mid' : ''
              const wb = WHITE_BG_LOGOS.has(r.ticker) ? ' white-bg' : ''
              return (
                <tr key={r.ticker} data-ticker={r.ticker} data-company={r.company || ''}
                  onClick={() => openTicker(r.ticker, r.company)}>
                  <td>
                    <div className="ticker-cell">
                      <img className={'logo-mini' + wb} src={LOGO(r.ticker)} alt={r.ticker} />
                      <div className="tc-text"><div className="tk">{r.ticker}</div><div className="co">{r.company || ''}</div></div>
                    </div>
                  </td>
                  <td><span className="sector-pill">{SECTOR_ABBR[r.sector] || (r.industry || '').slice(0, 8) || '—'}</span></td>
                  <td className="r">{r.close == null ? '—' : fmtPrice(r.close)}</td>
                  <td className={'r ' + dirClass(ml)}>{ml == null ? '—' : pct(ml)}</td>
                  <td className="r">{capB(r.market_cap)}</td>
                  <td><span className={'sig-tag ' + sigCls(r.signal)}>{r.signal || ''}</span></td>
                  <td className="r"><span className={'vqs-pill ' + vqsKls}>{r.vqs ?? '—'}</span></td>
                  <td className="r">
                    <span className="upside-bar"><span className={'fill' + (up < 0 ? ' down' : '')} style={{ width: Math.max(0, Math.min(100, up / maxUp * 100)).toFixed(0) + '%' }} /></span>
                    <span className={dirClass(up)}>{pct(up)}</span>
                  </td>
                  <td className="r"><span className="health">{healthDots(r.health_score)}</span></td>
                  <td className={'r ' + dirClass(ml)}>{ml == null ? '—' : pct(ml)}</td>
                  <td className={'r ' + (r.pe_ttm == null ? 'muted' : '')}>{r.pe_ttm == null ? '—' : num(r.pe_ttm, { fd: 1 })}</td>
                  <td className="r muted" style={{ paddingRight: 18 }}>—</td>
                </tr>
              )
            }) : (
              <tr><td colSpan="12" className="muted" style={{ textAlign: 'center', padding: 24 }}>No tickers match these filters.</td></tr>
            )}
          </tbody>
        </table>

        <div className="pager">
          <span className="p">‹</span>
          <span className="p active">1</span>
          <span className="p">2</span>
          <span className="p">3</span>
          <span className="p">4</span>
          <span className="p gap">…</span>
          <span className="p">21</span>
          <span className="p">›</span>
          <span className="meta">Page 1 of 21 · {Math.min(12, ranked.length)} of {filtered.length}</span>
        </div>
      </div>
    </div>
  )
}
