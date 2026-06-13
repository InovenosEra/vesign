/* Screener tab — filter rail + ranked results table.
 * Data: signals/today (US), filtered/sorted/paginated client-side. Rows open the
 * shared ticker modal. Filters persist via "Save filter set"; column visibility
 * and the chosen sort persist too. */
import { useState, useRef, useCallback, useEffect, useMemo } from 'react'
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
// All 11 GICS sectors are real, filterable pills; anything else (blank/ETF) → "Other".
const SECTOR_PILLS = [
  ['Information Technology', 'Technology'], ['Communication Services', 'Communication'],
  ['Financials', 'Financials'], ['Health Care', 'Health Care'], ['Industrials', 'Industrials'],
  ['Consumer Discretionary', 'Consumer Disc.'], ['Consumer Staples', 'Staples'], ['Energy', 'Energy'],
  ['Materials', 'Materials'], ['Real Estate', 'Real Estate'], ['Utilities', 'Utilities'],
  ['__other', 'Other'],
]
const KNOWN_SECTORS = new Set(SECTOR_PILLS.map(([v]) => v).filter(v => v !== '__other'))
const CAP_PILLS = [['mega', 'Mega >$200B'], ['large', 'Large $10–200B'], ['mid', 'Mid $2–10B'], ['small', 'Small <$2B']]
const PAGE_SIZE = 25

const sigCls = (s) => ({ BUY: 'buy', SELL: 'sell' }[s] || 'hold')
const capB = (mc) => mc == null ? '—'
  : mc >= 1e12 ? '$' + (mc / 1e12).toFixed(2) + 'T'
  : mc >= 1e9 ? '$' + (mc / 1e9).toFixed(1) + 'B'
  : '$' + (mc / 1e6).toFixed(0) + 'M'
const capBucket = (mc) => mc == null ? null : mc >= 200e9 ? 'mega' : mc >= 10e9 ? 'large' : mc >= 2e9 ? 'mid' : 'small'
const HEALTH_COLOR = { 1: '#ff4d5c', 2: '#c2660c', 3: '#ff9500', 4: '#00d97e', 5: '#0a8f54' }
const healthDots = (score) => {
  const n = score == null ? 0 : Math.max(0, Math.min(5, score))
  const c = HEALTH_COLOR[n] || '#6b7280'
  return [0, 1, 2, 3, 4].map(i => (
    <span key={i} className={'s' + (i < n ? '' : ' off')} style={i < n ? { background: c } : undefined} />
  ))
}

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
  upside: { lo: -50, hi: 100 }, pe: { lo: 0, hi: 100 },
  search: '',
}

// Column registry — single source of truth for the table, sorting, the Columns
// picker, and CSV export. `cell` renders the table cell; `csv` returns the raw
// value for export (defaults to the row's `key`). `sortable`/`hideable` gate the
// header click + the picker.
const COLUMNS = [
  { key: 'ticker', label: 'Ticker', hideable: false, sortable: true,
    cell: (r) => (
      <div className="ticker-cell">
        <img className={'logo-mini' + (WHITE_BG_LOGOS.has(r.ticker) ? ' white-bg' : '')} src={LOGO(r.ticker)} alt={r.ticker} />
        <div className="tc-text"><div className="tk">{r.ticker}</div><div className="co">{r.company || ''}</div></div>
      </div>),
    csv: (r) => r.ticker },
  { key: 'sector', label: 'Sector',
    cell: (r) => <span className="sector-pill">{SECTOR_ABBR[r.sector] || (r.industry || '').slice(0, 8) || '—'}</span>,
    csv: (r) => r.sector || '' },
  { key: 'close', label: 'Price', align: 'r', sortable: true,
    cell: (r, c) => r.close == null ? '—' : c.fmtPrice(r.close) },
  { key: 'day_change_pct', label: 'Day', align: 'r', sortable: true,
    cell: (r) => <span className={dirClass(r.day_change_pct)}>{r.day_change_pct == null ? '—' : pct(r.day_change_pct)}</span> },
  { key: 'market_cap', label: 'Mkt cap', align: 'r', sortable: true, cell: (r) => capB(r.market_cap) },
  { key: 'signal', label: 'Signal', sortable: true,
    cell: (r) => <span className={'sig-tag ' + sigCls(r.signal)}>{r.signal || ''}</span> },
  { key: 'fair_value_upside', label: 'Pred. upside', align: 'r', sortable: true,
    cell: (r, c) => {
      if (r.fair_value_upside == null) return <span className="muted">—</span>
      const up = r.fair_value_upside * 100
      return (<>
        <span className="upside-bar"><span className={'fill' + (up < 0 ? ' down' : '')} style={{ width: Math.max(0, Math.min(100, up / c.maxUp * 100)).toFixed(0) + '%' }} /></span>
        <span className={dirClass(up)}>{pct(up)}</span>
      </>)
    },
    csv: (r) => r.fair_value_upside == null ? '' : (r.fair_value_upside * 100).toFixed(2) },
  { key: 'health_score', label: 'Health', align: 'r', sortable: true,
    cell: (r) => <span className="health">{healthDots(r.health_score)}</span> },
  { key: 'prediction_score', label: 'ML 5d', align: 'r', sortable: true,
    cell: (r) => {
      const ml = r.prediction_score == null ? null : r.prediction_score * 100
      return <span className={dirClass(ml)}>{ml == null ? '—' : pct(ml)}</span>
    },
    csv: (r) => r.prediction_score == null ? '' : (r.prediction_score * 100).toFixed(2) },
  { key: 'pe_ttm', label: 'P/E', align: 'r', sortable: true,
    cell: (r) => r.pe_ttm == null ? <span className="muted">—</span> : num(r.pe_ttm, { fd: 1 }) },
  { key: 'week52_high', label: '52w high', align: 'r', sortable: true,
    cell: (r, c) => r.week52_high == null ? <span className="muted">—</span> : c.fmtPrice(r.week52_high) },
]
const COL_BY_KEY = Object.fromEntries(COLUMNS.map(c => [c.key, c]))

const FILTER_LS = 'rd-screener-filters'
const COLS_LS = 'rd-screener-hidden-cols'
const loadJSON = (k) => { try { return JSON.parse(localStorage.getItem(k)) } catch { return null } }

export default function Screener({ onCount }) {
  const openTicker = useTickerModal()
  const { fmtPrice } = useCurrency()
  const { data: rows } = useQuery({ queryKey: ['signals-today', 'US'], queryFn: () => getSignalsToday(null, 'US') })
  const all = Array.isArray(rows) ? rows : []

  // Restore a previously-saved filter set (arrays → Sets), else defaults.
  const saved = useMemo(() => loadJSON(FILTER_LS), [])
  const initSet = (key) => saved && saved[key] ? new Set(saved[key]) : new Set(DEFAULTS[key])
  const initVal = (key) => saved && saved[key] != null ? saved[key] : DEFAULTS[key]

  const [signals, setSignals] = useState(() => initSet('signals'))
  const [sectors, setSectors] = useState(() => initSet('sectors'))
  const [caps, setCaps] = useState(() => initSet('caps'))
  const [health, setHealth] = useState(() => initVal('health'))
  const [dir, setDir] = useState(() => initVal('dir'))
  const [upside, setUpside] = useState(() => ({ ...(saved?.upside || DEFAULTS.upside) }))
  const [pe, setPe] = useState(() => ({ ...(saved?.pe || DEFAULTS.pe) }))
  const [search, setSearch] = useState(() => initVal('search'))

  const [sort, setSort] = useState({ key: 'fair_value_upside', dir: 'desc' })
  const [page, setPage] = useState(1)
  const [hidden, setHidden] = useState(() => new Set(loadJSON(COLS_LS) || []))
  const [colsOpen, setColsOpen] = useState(false)
  const [savedFlag, setSavedFlag] = useState(false)
  const colsRef = useRef(null)

  useEffect(() => {
    const onDown = (e) => { if (colsRef.current && !colsRef.current.contains(e.target)) setColsOpen(false) }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [])

  const toggle = (setFn) => (key) => setFn(prev => {
    const next = new Set(prev)
    next.has(key) ? next.delete(key) : next.add(key)
    return next
  })
  const togSignal = toggle(setSignals), togSector = toggle(setSectors), togCap = toggle(setCaps)

  const reset = () => {
    setSignals(new Set(DEFAULTS.signals)); setSectors(new Set(DEFAULTS.sectors))
    setCaps(new Set(DEFAULTS.caps)); setHealth(DEFAULTS.health); setDir(DEFAULTS.dir)
    setUpside({ ...DEFAULTS.upside }); setPe({ ...DEFAULTS.pe })
    setSearch('')
  }
  const saveFilters = () => {
    const payload = {
      signals: [...signals], sectors: [...sectors], caps: [...caps],
      health, dir, upside, pe, search,
    }
    try { localStorage.setItem(FILTER_LS, JSON.stringify(payload)) } catch { /* ignore */ }
    setSavedFlag(true); setTimeout(() => setSavedFlag(false), 1600)
  }
  const toggleCol = (key) => setHidden(prev => {
    const next = new Set(prev)
    next.has(key) ? next.delete(key) : next.add(key)
    try { localStorage.setItem(COLS_LS, JSON.stringify([...next])) } catch { /* ignore */ }
    return next
  })
  const toggleSort = (key) => setSort(s => s.key === key
    ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
    : { key, dir: key === 'ticker' || key === 'signal' || key === 'sector' ? 'asc' : 'desc' })

  // Treat slider extremes as unbounded (matches mockup's -Infinity/Infinity).
  const upLo = upside.lo <= -50 ? -Infinity : upside.lo
  const upHi = upside.hi >= 100 ? Infinity : upside.hi
  const peLo = pe.lo <= 0 ? -Infinity : pe.lo
  const peHi = pe.hi >= 100 ? Infinity : pe.hi
  const q = search.trim().toUpperCase()

  const filtered = useMemo(() => all.filter(r => {
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
  }), [all, signals, sectors, caps, health, dir, upLo, upHi, peLo, peHi, q])

  useEffect(() => { onCount && onCount(filtered.length) }, [filtered.length, onCount])

  const sorted = useMemo(() => {
    const mul = sort.dir === 'asc' ? 1 : -1
    return filtered.slice().sort((a, b) => {
      const av = a[sort.key], bv = b[sort.key]
      const an = av == null, bn = bv == null
      if (an && bn) return 0
      if (an) return 1   // nulls always last
      if (bn) return -1
      if (typeof av === 'string') return mul * String(av).localeCompare(String(bv))
      return mul * (av - bv)
    })
  }, [filtered, sort])

  const pageCount = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE))
  // Keep page in range when results shrink / sort changes.
  useEffect(() => { if (page > pageCount) setPage(1) }, [pageCount, page])
  const curPage = Math.min(page, pageCount)
  const pageRows = sorted.slice((curPage - 1) * PAGE_SIZE, curPage * PAGE_SIZE)
  const maxUp = Math.max(...filtered.filter(r => r.fair_value_upside != null).map(r => r.fair_value_upside * 100), 1)

  const visibleCols = COLUMNS.filter(c => !hidden.has(c.key))
  const ctx = { fmtPrice, maxUp }

  const exportCsv = () => {
    const header = visibleCols.map(c => c.label)
    const lines = [header.join(',')]
    for (const r of sorted) {
      const cells = visibleCols.map(c => {
        const raw = c.csv ? c.csv(r) : r[c.key]
        const s = raw == null ? '' : String(raw)
        return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s
      })
      lines.push(cells.join(','))
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `vesign-screener-${sorted.length}.csv`
    document.body.appendChild(a); a.click(); a.remove()
    URL.revokeObjectURL(url)
  }

  const sortLabel = COL_BY_KEY[sort.key]?.label || 'Predicted upside'
  const pages = pagerPages(curPage, pageCount)
  const from = sorted.length ? (curPage - 1) * PAGE_SIZE + 1 : 0
  const to = Math.min(curPage * PAGE_SIZE, sorted.length)

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
          <button className="save-btn" onClick={saveFilters}>{savedFlag ? 'Saved ✓' : 'Save filter set'}</button>
        </div>
      </aside>

      {/* MAIN RESULTS */}
      <div className="results-area">
        <div className="results-toolbar">
          <div className="count"><strong>{filtered.length.toLocaleString()}</strong> tickers · sorted by {sortLabel.toLowerCase()}</div>
          <div className="spacer" />
          <div className="pill-btn" onClick={() => toggleSort(sort.key)} title="Toggle sort direction">
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 6h18M6 12h12M10 18h4" /></svg>
            {sortLabel} <span className="arr">{sort.dir === 'desc' ? '↓' : '↑'}</span>
          </div>
          <span className="hdr-select" ref={colsRef} style={{ position: 'relative' }}>
            <div className="pill-btn" onClick={() => setColsOpen(o => !o)}>
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 3h18v18H3z M3 9h18 M9 21V9" /></svg>
              Columns
            </div>
            {colsOpen && (
              <div className="hs-menu" role="menu" style={{ display: 'block', right: 0, minWidth: 160 }}>
                {COLUMNS.filter(c => c.hideable !== false).map(c => (
                  <button key={c.key} type="button" role="menuitemcheckbox" aria-checked={!hidden.has(c.key)}
                    className={'hs-row' + (!hidden.has(c.key) ? ' sel' : '')} onClick={() => toggleCol(c.key)}>
                    <span className="hs-sym">{hidden.has(c.key) ? '' : '✓'}</span><span className="hs-name">{c.label}</span>
                  </button>
                ))}
              </div>
            )}
          </span>
          <div className="pill-btn" onClick={exportCsv}>
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 3v12 M7 10l5 5 5-5 M5 21h14" /></svg>
            Export CSV
          </div>
        </div>

        <table className="data-table">
          <thead>
            <tr>
              {visibleCols.map(c => (
                <th key={c.key} className={(c.align === 'r' ? 'r ' : '') + (c.sortable ? 'sortable' : '')}
                  onClick={c.sortable ? () => toggleSort(c.key) : undefined}>
                  {c.label}{sort.key === c.key && <span className="arr">{sort.dir === 'desc' ? '↓' : '↑'}</span>}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageRows.length ? pageRows.map(r => (
              <tr key={r.ticker} data-ticker={r.ticker} data-company={r.company || ''}
                onClick={() => openTicker(r.ticker, r.company)}>
                {visibleCols.map(c => (
                  <td key={c.key} className={c.align === 'r' ? 'r' : ''}
                    style={c.key === 'week52_high' ? { paddingRight: 18 } : undefined}>
                    {c.cell(r, ctx)}
                  </td>
                ))}
              </tr>
            )) : (
              <tr><td colSpan={visibleCols.length} className="muted" style={{ textAlign: 'center', padding: 24 }}>No tickers match these filters.</td></tr>
            )}
          </tbody>
        </table>

        <div className="pager">
          <span className={'p' + (curPage === 1 ? ' disabled' : '')} onClick={() => curPage > 1 && setPage(curPage - 1)}>‹</span>
          {pages.map((p, i) => p === '…'
            ? <span key={'g' + i} className="p gap">…</span>
            : <span key={p} className={'p' + (p === curPage ? ' active' : '')} onClick={() => setPage(p)}>{p}</span>)}
          <span className={'p' + (curPage === pageCount ? ' disabled' : '')} onClick={() => curPage < pageCount && setPage(curPage + 1)}>›</span>
          <span className="meta">Page {curPage} of {pageCount} · {from.toLocaleString()}–{to.toLocaleString()} of {sorted.length.toLocaleString()}</span>
        </div>
      </div>
    </div>
  )
}

// Page numbers with ellipses: always show first/last + a window around current.
function pagerPages(cur, count) {
  if (count <= 7) return Array.from({ length: count }, (_, i) => i + 1)
  const out = [1]
  const lo = Math.max(2, cur - 1), hi = Math.min(count - 1, cur + 1)
  if (lo > 2) out.push('…')
  for (let p = lo; p <= hi; p++) out.push(p)
  if (hi < count - 1) out.push('…')
  out.push(count)
  return out
}
