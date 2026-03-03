import { useState, useEffect } from 'react'
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { getMarketStatus } from './api'
import SignalsPage from './pages/SignalsPage'
import WatchlistPage from './pages/WatchlistPage'
import TradesPage from './pages/TradesPage'
import './App.css'

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30_000 } },
})

// ---------------------------------------------------------------------------
// Returns the UTC timestamp (ms) of the next NYSE open (9:30 AM ET),
// correctly accounting for DST via the browser's Intl API.
// ---------------------------------------------------------------------------
function fmtCountdown(ms) {
  if (ms <= 0) return '00:00:00'
  const s = Math.floor(ms / 1000)
  const h = Math.floor(s / 3600)
  const min = Math.floor((s % 3600) / 60)
  const sec = s % 60
  return `${String(h).padStart(2, '0')}:${String(min).padStart(2, '0')}:${String(sec).padStart(2, '0')}`
}

// Returns UTC ms for next NYSE close (4:00 PM ET today, since we're open)
function getMarketCloseUTC() {
  const now = new Date()
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour12: false,
  }).formatToParts(now)
  const et = {}
  parts.forEach(p => { if (p.type !== 'literal') et[p.type] = parseInt(p.value) })

  const y = et.year
  const m = String(et.month).padStart(2, '0')
  const d = String(et.day).padStart(2, '0')

  const guess = new Date(`${y}-${m}-${d}T21:00:00Z`) // rough 4 PM ET guess
  const gParts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour: 'numeric', minute: 'numeric', hour12: false,
  }).formatToParts(guess)
  const gp = {}
  gParts.forEach(p => { if (p.type !== 'literal') gp[p.type] = parseInt(p.value) })
  gp.hour = (gp.hour || 0) % 24

  const diffMs = ((16 * 60) - (gp.hour * 60 + (gp.minute || 0))) * 60_000
  return guess.getTime() + diffMs
}

function getNextMarketOpenUTC() {
  const now = new Date()

  // Current ET date + time components
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour: 'numeric', minute: 'numeric',
    hour12: false,
  }).formatToParts(now)

  const et = {}
  parts.forEach(p => { if (p.type !== 'literal') et[p.type] = parseInt(p.value) })
  et.hour = (et.hour || 0) % 24 // guard against midnight = 24

  const dow = new Date(et.year, et.month - 1, et.day).getDay() // 0=Sun, 6=Sat
  const isWeekday  = dow >= 1 && dow <= 5
  const beforeOpen = et.hour < 9 || (et.hour === 9 && et.minute < 30)

  // Start from today in ET; advance if market already opened today or it's a weekend
  const target = new Date(et.year, et.month - 1, et.day)
  if (!isWeekday || !beforeOpen) {
    target.setDate(target.getDate() + 1)
    while (target.getDay() === 0 || target.getDay() === 6)
      target.setDate(target.getDate() + 1)
  }

  const y = target.getFullYear()
  const m = String(target.getMonth() + 1).padStart(2, '0')
  const d = String(target.getDate()).padStart(2, '0')

  // Use a UTC guess around 14:30 (midpoint between ET-4 and ET-5 offsets)
  // then correct by checking what ET time that UTC moment actually maps to.
  const guess = new Date(`${y}-${m}-${d}T14:30:00Z`)
  const gParts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour: 'numeric', minute: 'numeric', hour12: false,
  }).formatToParts(guess)
  const gp = {}
  gParts.forEach(p => { if (p.type !== 'literal') gp[p.type] = parseInt(p.value) })
  gp.hour = (gp.hour || 0) % 24

  const diffMs = ((9 * 60 + 30) - (gp.hour * 60 + (gp.minute || 0))) * 60_000
  return guess.getTime() + diffMs
}

function useCountdown(isOpen) {
  const [countdown, setCountdown] = useState('')

  useEffect(() => {
    function tick() {
      const remaining = isOpen
        ? getMarketCloseUTC() - Date.now()
        : getNextMarketOpenUTC() - Date.now()
      setCountdown(fmtCountdown(remaining))
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [isOpen])

  return countdown
}

function MarketStatus() {
  const { data } = useQuery({
    queryKey: ['market-status'],
    queryFn: getMarketStatus,
    refetchInterval: 60_000,
  })
  const countdown = useCountdown(data?.is_open ?? false)

  if (!data) return null
  return data.is_open
    ? (
      <span className="market-open" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
        <span>● Market Open</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#cccccc', fontWeight: 'normal' }}>Closes in {countdown}</span>
      </span>
    )
    : (
      <span className="market-closed" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
        <span>● Market Closed</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#cccccc', fontWeight: 'normal' }}>Opens in {countdown}</span>
      </span>
    )
}

function Header() {
  return (
    <header className="app-header">
      <h1 style={{ display: 'flex', alignItems: 'flex-end', gap: 2, fontWeight: 900, fontSize: '2.7rem', letterSpacing: '0.08em', fontFamily: "'Segoe UI', system-ui, sans-serif" }}>
        <img src="/favicon.png" alt="V" style={{ height: '3.2rem', objectFit: 'contain', flexShrink: 0, filter: 'drop-shadow(0 2px 4px rgba(0, 210, 255, 0.6))' }} />
        <span className="title-shimmer" style={{ letterSpacing: '0.08em' }}>esign</span>
      </h1>
      <nav>
        <NavLink to="/">Signals</NavLink>
        <NavLink to="/watchlist">Watchlist</NavLink>
        <NavLink to="/trades">Trades</NavLink>
      </nav>
      <MarketStatus />
    </header>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Header />
        <main className="app-main">
          <Routes>
            <Route path="/" element={<SignalsPage />} />
            <Route path="/watchlist" element={<WatchlistPage />} />
            <Route path="/trades" element={<TradesPage />} />
          </Routes>
        </main>
      </BrowserRouter>
    </QueryClientProvider>
  )
}
