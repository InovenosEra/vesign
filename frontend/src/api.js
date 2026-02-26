const BASE = '/api'

async function get(path) {
  const res = await fetch(BASE + path)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

async function post(path, body) {
  const res = await fetch(BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  if (res.status === 204) return null
  return res.json()
}

async function patch(path, body) {
  const res = await fetch(BASE + path, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json()
}

async function del(path) {
  const res = await fetch(BASE + path, { method: 'DELETE' })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
}

// --- Market ----------------------------------------------------------------
export const getMarketStatus = () => get('/market/status')

// --- Signals ---------------------------------------------------------------
export const getSignalsToday = (signal) =>
  get('/signals/today' + (signal ? `?signal=${signal}` : ''))

export const getSignals = ({ signal, search, months = 12, page = 1, page_size = 100, sort_by = 'date', sort_dir = 'desc' } = {}) => {
  const params = new URLSearchParams({ months, page, page_size, sort_by, sort_dir })
  if (signal) params.set('signal', signal)
  if (search) params.set('search', search)
  return get(`/signals?${params}`)
}

export const getSignalsByTickers = (tickers) =>
  get(`/signals/by-tickers?tickers=${tickers.join(',')}`)

// --- Live prices -----------------------------------------------------------
export const getLivePrices = (tickers) =>
  get(`/prices/live?tickers=${tickers.join(',')}`)

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

// --- Trades ----------------------------------------------------------------
export const getTrades = ({ start, end } = {}) => {
  const params = new URLSearchParams()
  if (start) params.set('start', start)
  if (end) params.set('end', end)
  const qs = params.toString()
  return get('/trades' + (qs ? `?${qs}` : ''))
}

// --- Pipeline --------------------------------------------------------------
export const runPipeline = () => post('/pipeline/run')
export const getPipelineStatus = () => get('/pipeline/status')
