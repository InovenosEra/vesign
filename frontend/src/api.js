const BASE = '/api'

export const WHITE_BG_LOGOS = new Set(['CTVA', 'SNX', 'ZBH', 'PCTY', 'PENG', 'SM', 'SSTK'])

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

// --- Signals ---------------------------------------------------------------
export const getSignalsToday = (signal, market = 'US') => {
  const params = new URLSearchParams({ market })
  if (signal) params.set('signal', signal)
  return get(`/signals/today?${params}`)
}

export const getSignals = ({ signal, search, months = 12, page = 1, page_size = 100, sort_by = 'date', sort_dir = 'desc', market = 'US' } = {}) => {
  const params = new URLSearchParams({ months, page, page_size, sort_by, sort_dir, market })
  if (signal) params.set('signal', signal)
  if (search) params.set('search', search)
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

// --- Search ----------------------------------------------------------------
export const searchTickers = (q, limit = 10) =>
  get(`/search?q=${encodeURIComponent(q)}&limit=${limit}`)

// --- Trades ----------------------------------------------------------------
export const getTrades = ({ start, end, market = 'US' } = {}) => {
  const params = new URLSearchParams({ market })
  if (start) params.set('start', start)
  if (end) params.set('end', end)
  return get('/trades?' + params.toString())
}

export const getOpenTrades = (market = 'US') =>
  get(`/trades/open?market=${market}`)

// --- News & analyst changes ------------------------------------------------
export const getNews = (ticker, limit = 5) =>
  get(`/news?ticker=${encodeURIComponent(ticker)}&limit=${limit}`)

export const getEarnings = (ticker) =>
  get(`/earnings?ticker=${encodeURIComponent(ticker)}`)

export const getAnalystChanges = (ticker, limit = 8) =>
  get(`/analyst-changes?ticker=${encodeURIComponent(ticker)}&limit=${limit}`)

// --- Portfolio -------------------------------------------------------------
export const getPortfolioHoldings = () =>
  get('/portfolio/holdings')

export const getPortfolioPerformance = () =>
  get('/portfolio/performance')

export const getPortfolioComparison = () =>
  get('/portfolio/comparison')
