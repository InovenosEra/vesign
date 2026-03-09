import { useState, useEffect, useContext, useRef } from 'react'
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { ClerkProvider, RedirectToSignIn, useAuth, useClerk, useUser } from '@clerk/react'
import { setTokenGetter } from './api'
import { getMarketStatus } from './api'
import { MarketContext, MarketProvider } from './context/MarketContext'
import SignalsPage from './pages/SignalsPage'
import WatchlistPage from './pages/WatchlistPage'
import TradesPage from './pages/TradesPage'
import GlobalSearch from './components/GlobalSearch'
import LoginPage from './pages/LoginPage'
import CompleteProfilePage from './pages/CompleteProfilePage'
import './App.css'

const PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30_000 } },
})

// ---------------------------------------------------------------------------
// Shared countdown formatter
// ---------------------------------------------------------------------------
function fmtCountdown(ms) {
  if (ms <= 0) return '00:00:00'
  const s = Math.floor(ms / 1000)
  const h = Math.floor(s / 3600)
  const min = Math.floor((s % 3600) / 60)
  const sec = s % 60
  return `${String(h).padStart(2, '0')}:${String(min).padStart(2, '0')}:${String(sec).padStart(2, '0')}`
}

// ---------------------------------------------------------------------------
// NYSE helpers (America/New_York)
// ---------------------------------------------------------------------------
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

  const guess = new Date(`${y}-${m}-${d}T21:00:00Z`)
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
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour: 'numeric', minute: 'numeric',
    hour12: false,
  }).formatToParts(now)
  const et = {}
  parts.forEach(p => { if (p.type !== 'literal') et[p.type] = parseInt(p.value) })
  et.hour = (et.hour || 0) % 24

  const dow = new Date(et.year, et.month - 1, et.day).getDay()
  const isWeekday  = dow >= 1 && dow <= 5
  const beforeOpen = et.hour < 9 || (et.hour === 9 && et.minute < 30)

  const target = new Date(et.year, et.month - 1, et.day)
  if (!isWeekday || !beforeOpen) {
    target.setDate(target.getDate() + 1)
    while (target.getDay() === 0 || target.getDay() === 6)
      target.setDate(target.getDate() + 1)
  }

  const y = target.getFullYear()
  const m = String(target.getMonth() + 1).padStart(2, '0')
  const d = String(target.getDate()).padStart(2, '0')

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

// ---------------------------------------------------------------------------
// TASE helpers (Asia/Jerusalem, open Sun–Thu 09:59–17:29)
// ---------------------------------------------------------------------------
function getTaseCloseUTC() {
  const now = new Date()
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Jerusalem',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour12: false,
  }).formatToParts(now)
  const il = {}
  parts.forEach(p => { if (p.type !== 'literal') il[p.type] = parseInt(p.value) })

  const y = il.year
  const m = String(il.month).padStart(2, '0')
  const d = String(il.day).padStart(2, '0')

  const guess = new Date(`${y}-${m}-${d}T15:29:00Z`)
  const gParts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Jerusalem',
    hour: 'numeric', minute: 'numeric', hour12: false,
  }).formatToParts(guess)
  const gp = {}
  gParts.forEach(p => { if (p.type !== 'literal') gp[p.type] = parseInt(p.value) })
  gp.hour = (gp.hour || 0) % 24

  const diffMs = ((17 * 60 + 29) - (gp.hour * 60 + (gp.minute || 0))) * 60_000
  return guess.getTime() + diffMs
}

function getNextTaseOpenUTC() {
  const now = new Date()
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Jerusalem',
    year: 'numeric', month: 'numeric', day: 'numeric',
    hour: 'numeric', minute: 'numeric',
    hour12: false,
  }).formatToParts(now)
  const il = {}
  parts.forEach(p => { if (p.type !== 'literal') il[p.type] = parseInt(p.value) })
  il.hour = (il.hour || 0) % 24

  const dow = new Date(il.year, il.month - 1, il.day).getDay()
  const isTaseDay  = dow >= 0 && dow <= 4
  const beforeOpen = il.hour < 9 || (il.hour === 9 && il.minute < 59)

  const target = new Date(il.year, il.month - 1, il.day)
  if (!isTaseDay || !beforeOpen) {
    target.setDate(target.getDate() + 1)
    while (target.getDay() === 5 || target.getDay() === 6)
      target.setDate(target.getDate() + 1)
  }

  const y = target.getFullYear()
  const m = String(target.getMonth() + 1).padStart(2, '0')
  const d = String(target.getDate()).padStart(2, '0')

  const guess = new Date(`${y}-${m}-${d}T07:59:00Z`)
  const gParts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Jerusalem',
    hour: 'numeric', minute: 'numeric', hour12: false,
  }).formatToParts(guess)
  const gp = {}
  gParts.forEach(p => { if (p.type !== 'literal') gp[p.type] = parseInt(p.value) })
  gp.hour = (gp.hour || 0) % 24

  const diffMs = ((9 * 60 + 59) - (gp.hour * 60 + (gp.minute || 0))) * 60_000
  return guess.getTime() + diffMs
}

// ---------------------------------------------------------------------------
// Countdown hook — market-aware
// ---------------------------------------------------------------------------
function useCountdown(isOpen, market = 'US') {
  const [countdown, setCountdown] = useState('')

  useEffect(() => {
    function tick() {
      const remaining = market === 'IL'
        ? (isOpen ? getTaseCloseUTC() - Date.now() : getNextTaseOpenUTC() - Date.now())
        : (isOpen ? getMarketCloseUTC() - Date.now() : getNextMarketOpenUTC() - Date.now())
      setCountdown(fmtCountdown(remaining))
    }
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [isOpen, market])

  return countdown
}

// ---------------------------------------------------------------------------
// Flag selector — switches global market context
// ---------------------------------------------------------------------------
const MARKETS = [
  { code: 'US', flag: '🇺🇸', label: 'US' },
  { code: 'IL', flag: '🇮🇱', label: 'IL' },
]

function FlagSelector() {
  const { market, setMarket } = useContext(MarketContext)
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    function onMouseDown(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  const current = MARKETS.find(m => m.code === market) ?? MARKETS[0]

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        onClick={() => setOpen(o => !o)}
        title="Switch market"
        style={{
          background: 'transparent',
          border: '1px solid var(--border)',
          borderRadius: 6,
          padding: '4px 10px',
          cursor: 'pointer',
          fontSize: 20,
          lineHeight: 1,
          color: 'var(--text)',
        }}
      >
        {current.flag}
      </button>
      {open && (
        <div style={{
          position: 'absolute',
          right: 0,
          top: 'calc(100% + 6px)',
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          borderRadius: 8,
          zIndex: 100,
          minWidth: 'fit-content',
          boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
          overflow: 'hidden',
        }}>
          {MARKETS.map(opt => (
            <button
              key={opt.code}
              onClick={() => { setMarket(opt.code); setOpen(false) }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                width: '100%',
                padding: '8px 12px',
                background: market === opt.code ? 'rgba(0,210,255,0.12)' : 'transparent',
                border: 'none',
                cursor: 'pointer',
                color: 'var(--text)',
                fontSize: 11,
                textAlign: 'left',
              }}
            >
              <span style={{ fontSize: 20 }}>{opt.flag}</span>
              <span>{opt.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Market status badge
// ---------------------------------------------------------------------------
function MarketStatus() {
  const { market } = useContext(MarketContext)
  const { data } = useQuery({
    queryKey: ['market-status', market],
    queryFn: () => getMarketStatus(market),
    refetchInterval: 60_000,
  })
  const countdown = useCountdown(data?.is_open ?? false, market)

  if (!data) return null
  return data.is_open
    ? (
      <span className="market-open" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
        <span><span className="dot-blink">●</span> Market Open</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#cccccc', fontWeight: 'normal' }}>Closes in {countdown}</span>
      </span>
    )
    : (
      <span className="market-closed" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
        <span>● Market Close</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#cccccc', fontWeight: 'normal' }}>Opens in {countdown}</span>
      </span>
    )
}

// ---------------------------------------------------------------------------
// User menu (Hello [First name] + dropdown)
// ---------------------------------------------------------------------------
function UserMenu() {
  const { signOut, openUserProfile } = useClerk()
  const { user } = useUser()
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    function onMouseDown(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          background: 'transparent',
          border: '1px solid var(--border)',
          borderRadius: 6,
          padding: '4px 12px',
          cursor: 'pointer',
          fontSize: 13,
          color: 'var(--text)',
          whiteSpace: 'nowrap',
        }}
      >
        Hello, {user?.firstName} ▾
      </button>
      {open && (
        <div style={{
          position: 'absolute', right: 0, top: 'calc(100% + 6px)',
          background: 'var(--surface)', border: '1px solid var(--border)',
          borderRadius: 8, zIndex: 200, minWidth: 160,
          boxShadow: '0 4px 16px rgba(0,0,0,0.4)', overflow: 'hidden',
        }}>
          {[
            { label: 'Change Password', action: () => { openUserProfile(); setOpen(false) } },
            { label: 'Sign Out', action: () => signOut({ redirectUrl: '/sign-in' }) },
          ].map(item => (
            <button
              key={item.label}
              onClick={item.action}
              style={{
                display: 'block', width: '100%', padding: '10px 16px',
                background: 'transparent', border: 'none', cursor: 'pointer',
                color: 'var(--text)', fontSize: 13, textAlign: 'left',
              }}
            >
              {item.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------
function Header() {
  const { market } = useContext(MarketContext)
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
      <div style={{ flex: 1, display: 'flex', justifyContent: 'center' }}>
        <GlobalSearch />
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <MarketStatus />
        <FlagSelector />
        <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', minWidth: 14, textAlign: 'center' }}>
          {market === 'IL' ? '₪' : '$'}
        </span>
        <UserMenu />
      </div>
    </header>
  )
}

// ---------------------------------------------------------------------------
// Keeps api.js token getter in sync with Clerk session
// ---------------------------------------------------------------------------
function TokenSync({ onReady }) {
  const { getToken, isSignedIn } = useAuth()
  useEffect(() => {
    console.log('[TokenSync] isSignedIn:', isSignedIn)
    if (isSignedIn) {
      setTokenGetter(getToken)
      onReady()
    } else {
      setTokenGetter(null)
    }
  }, [isSignedIn, getToken])
  return null
}

// ---------------------------------------------------------------------------
// Protected app layout
// ---------------------------------------------------------------------------
function AppLayout() {
  const { isLoaded, userId } = useAuth()
  const { user } = useUser()
  const [tokenReady, setTokenReady] = useState(false)

  if (!isLoaded) return null
  if (!userId) return <RedirectToSignIn />
  if (user && (!user.firstName || !user.lastName)) return <CompleteProfilePage />

  return (
    <MarketProvider>
      <QueryClientProvider client={queryClient}>
        <TokenSync onReady={() => setTokenReady(true)} />
        {tokenReady && <Header />}
        {tokenReady && (
          <main className="app-main">
            <Routes>
              <Route path="/" element={<SignalsPage />} />
              <Route path="/watchlist" element={<WatchlistPage />} />
              <Route path="/trades" element={<TradesPage />} />
            </Routes>
          </main>
        )}
      </QueryClientProvider>
    </MarketProvider>
  )
}

// ---------------------------------------------------------------------------
// App root
// ---------------------------------------------------------------------------
export default function App() {
  return (
    <ClerkProvider publishableKey={PUBLISHABLE_KEY} signInUrl="/sign-in">
      <BrowserRouter>
        <Routes>
          <Route path="/sign-in" element={<LoginPage />} />
          <Route path="/*" element={<AppLayout />} />
        </Routes>
      </BrowserRouter>
    </ClerkProvider>
  )
}
