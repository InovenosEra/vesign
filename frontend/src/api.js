const BASE = '/api'

export const WHITE_BG_LOGOS = new Set(['CTVA', 'SNX', 'ZBH', 'PCTY', 'PENG', 'SM', 'SSTK', 'HWKN', 'CTS', 'CGNX', 'UE', 'MS', 'H', 'EVR', 'PNFP', 'WH', 'APO', 'SPG', 'AKR', 'ST', 'KKR', 'TCBI', 'BGC', 'CATY', 'XEL', 'XRAY', 'OUST', 'CHKP', 'COTY'])

export const COMPANY_NAME_OVERRIDES = {
  PM: 'Philip Morris',
  SPY: 'SPDR S&P 500 ETF',
  GOOGL: 'Alphabet A',
}

const NGROK_HEADERS = { 'ngrok-skip-browser-warning': 'true' }

// Set by TokenSync component in App.jsx once Clerk is ready
let _getToken = null
export function setTokenGetter(fn) { _getToken = fn }

async function authHeaders() {
  try {
    const token = _getToken
      ? await _getToken()
      : await window.Clerk?.session?.getToken()
    console.log('[api] token:', token ? token.slice(0, 20) + '...' : 'NULL')
    return token ? { Authorization: `Bearer ${token}` } : {}
  } catch (e) {
    console.error('[api] authHeaders error:', e)
    return {}
  }
}

async function handleResponse(res) {
  if (res.status === 403) {
    // Backend returns {detail: {code: "ACCOUNT_DISABLED", reason: "..."}} when
    // the user is on the admin denylist. Surface a single "account-disabled"
    // event so the app can render a clear screen instead of a generic 403.
    let payload = null
    try { payload = await res.json() } catch {}
    const detail = payload?.detail
    if (detail?.code === 'ACCOUNT_DISABLED') {
      try {
        window.dispatchEvent(new CustomEvent('vesign:account-disabled', {
          detail: { reason: detail.reason || 'Your account has been disabled.' }
        }))
      } catch {}
      const e = new Error('ACCOUNT_DISABLED')
      e.code = 'ACCOUNT_DISABLED'
      e.reason = detail.reason
      throw e
    }
  }
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  if (res.status === 204) return null
  return res.json()
}

async function get(path) {
  const res = await fetch(BASE + path, { headers: { ...NGROK_HEADERS, ...await authHeaders() }, cache: 'no-store' })
  return handleResponse(res)
}

async function post(path, body) {
  const res = await fetch(BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...NGROK_HEADERS, ...await authHeaders() },
    body: JSON.stringify(body),
  })
  return handleResponse(res)
}

async function patch(path, body) {
  const res = await fetch(BASE + path, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', ...NGROK_HEADERS, ...await authHeaders() },
    body: JSON.stringify(body),
  })
  return handleResponse(res)
}

async function del(path) {
  const res = await fetch(BASE + path, {
    method: 'DELETE',
    headers: { ...NGROK_HEADERS, ...await authHeaders() },
  })
  return handleResponse(res)
}

// --- Access request (public — no auth) ------------------------------------
export async function requestAccess(email, message = '', agreementName = '', agreedAt = '') {
  const res = await fetch('/api/auth/request-access', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, message, agreement_name: agreementName, agreed_at: agreedAt }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Request failed')
  }
  return res.json()
}

export async function submitContact(name, email, subject, message) {
  const res = await fetch('/api/contact', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, email, subject, message }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(err.detail || 'Failed to send message')
  }
  return res.json()
}

// --- Market ----------------------------------------------------------------
export const getMarketStatus = (market = 'US') =>
  get(`/market/status?market=${market}`)

export const getDataStatus = () => get('/data/status')
export const getFxRates    = () => get('/fx/rates')

// Redesign Market page — sector/movers/valuation/tape/calendar widgets.
export const getIndices      = ()            => get('/market/indices')
export const getCrossMarket  = ()            => get('/market/cross')
export const getCommodities  = ()            => get('/market/commodities')
export const getCurrencies   = (base = 'ILS') => get(`/market/currencies?base=${encodeURIComponent(base)}`)
export const getMovers       = (type, n = 5) => get(`/market/movers?type=${encodeURIComponent(type)}&limit=${n}`)
export const getHighsLows    = (type, n = 5) => get(`/market/highs-lows?type=${encodeURIComponent(type)}&limit=${n}`)
export const getBreadth      = ()            => get('/market/breadth')
export const getValuation    = (n = 6)       => get(`/market/valuation?limit=${n}`)
export const getSectors      = ()            => get('/market/sectors')
export const getSectorDetail = (name)        => get(`/market/sector/${encodeURIComponent(name)}`)
export const getTape         = ()            => get('/market/tape')
export const getTopNews      = (n = 5)       => get(`/market/news/top?limit=${n}`)
export const getTopAnalyst   = (days = 1, n = 5) => get(`/market/analyst-changes/top?days=${days}&limit=${n}`)
export const getTopGrades    = (n = 5)       => get(`/market/grades/top?limit=${n}`)
export const getAnalystActivity = (n = 50)   => get(`/market/analyst-activity?limit=${n}`)
export const getAnalystConsensus = (tickers = []) => get(`/market/analyst-consensus?tickers=${tickers.join(',')}`)
export const getEarningsWeek = ()            => get('/market/earnings/week')
export const getEconomicCal  = (days = 7)    => get(`/market/economic-calendar?days=${days}`)
export const getStats        = ()            => get('/stats')

// --- Signals ---------------------------------------------------------------
export const getSignalsToday = (signal, market = 'US') => {
  const params = new URLSearchParams({ market })
  if (signal) params.set('signal', signal)
  return get(`/signals/today?${params}`)
}

export const getSignals = ({ signal, search, months = 12, start, end, page = 1, page_size = 100, sort_by = 'date', sort_dir = 'desc', market = 'US' } = {}) => {
  const params = new URLSearchParams({ months, page, page_size, sort_by, sort_dir, market })
  if (signal) params.set('signal', signal)
  if (search) params.set('search', search)
  if (start)  params.set('start', start)
  if (end)    params.set('end', end)
  return get(`/signals?${params}`)
}

export const getSignalMarkers = (ticker, months = 13) =>
  get(`/signals/markers?ticker=${encodeURIComponent(ticker)}&months=${months}`)

export const getSignalsByTickers = (tickers) =>
  get(`/signals/by-tickers?tickers=${tickers.join(',')}`)

export const getSuccessRate = (months = 12) =>
  get(`/signals/success-rate?months=${months}`)

// --- Live prices -----------------------------------------------------------
export const getLivePrices = (tickers) =>
  get(`/prices/live?tickers=${tickers.join(',')}`)

export const getPriceHistory = (ticker, { start, end } = {}) => {
  const params = new URLSearchParams({ ticker })
  if (start && end) { params.set('start', start); params.set('end', end) }
  return get(`/prices/history?${params}`)
}

export const getAnalystHistory = (ticker, { start, end } = {}) => {
  const params = new URLSearchParams({ ticker })
  if (start) params.set('start', start)
  if (end)   params.set('end', end)
  return get(`/analyst-history?${params}`)
}

// --- Watchlists ------------------------------------------------------------
export const getWatchlists = () => get('/watchlists')
export const createWatchlist = (name) => post('/watchlists', { name })
export const renameWatchlist = (id, name) => patch(`/watchlists/${id}`, { name })
export const deleteWatchlist = (id) => del(`/watchlists/${id}`)
export const getWatchlistTickers = (id) => get(`/watchlists/${id}/tickers`)
export const addTicker = (id, ticker, note = '') =>
  post(`/watchlists/${id}/tickers`, { ticker, note })
export const updateTickerNote = (id, ticker, note) =>
  patch(`/watchlists/${id}/tickers/${ticker}`, { note })
export const removeTicker = (id, ticker) =>
  del(`/watchlists/${id}/tickers/${ticker}`)
export const getHoldings = (id) => get(`/watchlists/${id}/holdings`)
export const addHolding = (id, body) => post(`/watchlists/${id}/holdings`, body)
export const deleteHolding = (id, holdingId) => del(`/watchlists/${id}/holdings/${holdingId}`)
export const getHoldingLots = (ticker, market = 'US') =>
  get(`/portfolio/holdings/lots?ticker=${encodeURIComponent(ticker)}&market=${market}`)

// --- Search ----------------------------------------------------------------
export const searchTickers = (q, limit = 10) =>
  get(`/search?q=${encodeURIComponent(q)}&limit=${limit}`)

// --- Trades ----------------------------------------------------------------
export const getTrades = ({ start, end, market = 'US', includeLots = false } = {}) => {
  const params = new URLSearchParams({ market })
  if (start) params.set('start', start)
  if (end) params.set('end', end)
  if (includeLots) params.set('include_lots', '1')
  return get('/trades?' + params.toString())
}

export const getOpenTrades = (market = 'US', includeLots = false) => {
  const params = new URLSearchParams({ market })
  if (includeLots) params.set('include_lots', '1')
  return get('/trades/open?' + params.toString())
}

// --- News & analyst changes ------------------------------------------------
export const getNews = (ticker, limit = 5, lang) =>
  get(`/news?ticker=${encodeURIComponent(ticker)}&limit=${limit}${lang ? `&lang=${lang}` : ''}`)

export const getEarnings = (ticker) =>
  get(`/earnings?ticker=${encodeURIComponent(ticker)}`)

export const getAnalystChanges = (ticker, limit = 8) =>
  get(`/analyst-changes?ticker=${encodeURIComponent(ticker)}&limit=${limit}`)

// --- Research --------------------------------------------------------------
export const getResearch = (ticker, lang) =>
  get(`/research/${encodeURIComponent(ticker)}${lang ? `?lang=${lang}` : ''}`)

export const generateAIReport = (ticker, entryPrice, lang) =>
  post(`/research/${encodeURIComponent(ticker)}/ai-report`, {
    entry_price: entryPrice || null,
    lang: lang || 'en',
  })

// --- Portfolio -------------------------------------------------------------
export const getPortfolioHoldings = (market = 'US') =>
  get(`/portfolio/holdings?market=${market}`)

export const getPortfolioPerformance = (market = 'US', months = 12) =>
  get(`/portfolio/performance?market=${market}&months=${months}`)

export const getPortfolioComparison = (market = 'US') =>
  get(`/portfolio/comparison?market=${market}`)

// --- Entitlements (plan + wallet) ------------------------------------------
export const getMe = () => get('/me')
export const unlockSignal = (body) => post('/signals/unlock', body)
