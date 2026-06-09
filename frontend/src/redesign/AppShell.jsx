/* Redesign shared shell — the new top nav for redesigned routes.
 * Renders inside a <div className="rd"> so the scoped redesign.css applies only
 * here, leaving un-ported pages on the old design. Wired to the REAL contexts:
 * market-status is live, search is the real GlobalSearch. (Currency + language
 * live in Account → Profile, not the header.) */
import { useState, useEffect } from 'react'
import { NavLink } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useUser } from '@clerk/react'
import { getMarketStatus } from '../api'
import { MeProvider, useMe } from '../context/MeContext'
import { fmtCents } from './signals/gating'
import GlobalSearch from '../components/GlobalSearch'
import SignalModal from './SignalModalRd'
import { TickerModalContext } from './TickerModalContext'
import Tape from './market/Tape'
import './redesign.css'

function fmtCountdown(ms) {
  if (ms <= 0) return '00:00:00'
  const s = Math.floor(ms / 1000)
  const h = String(Math.floor(s / 3600)).padStart(2, '0')
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0')
  const sec = String(s % 60).padStart(2, '0')
  return `${h}:${m}:${sec}`
}
function useCountdown(iso) {
  const [cd, setCd] = useState('')
  useEffect(() => {
    if (!iso) { setCd(''); return }
    const target = new Date(iso).getTime()
    const tick = () => setCd(fmtCountdown(target - Date.now()))
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [iso])
  return cd
}

function MarketChip() {
  const { data } = useQuery({
    queryKey: ['market-status', 'US'],
    queryFn: () => getMarketStatus('US'),
    refetchInterval: 60_000,
  })
  // Backend returns `phase` (regular | pre | post | idle), not is_open.
  // Countdown always tracks the next regular open/close boundary.
  const phase = data?.phase
  const cd = useCountdown(data?.next_regular_event_utc || data?.next_event_utc)
  // Map phase → { className, label }. Pre/post are "extended" (amber).
  const view = {
    regular: { cls: '', label: 'Market open' },
    pre: { cls: ' extended', label: 'Pre-market' },
    post: { cls: ' extended', label: 'Post-market' },
    idle: { cls: ' closed', label: 'Market closed' },
  }[phase] ?? { cls: '', label: 'Market…' }
  return (
    <div className={'status' + view.cls}>
      <span className="dot" />{' '}
      <span>{view.label}</span>
      {cd && <span className="ct">{cd}</span>}
    </div>
  )
}

const PLAN_LABELS = { free: 'Free', pro: 'Pro', pro_plus: 'Pro+', max: 'Max' }

/* Plan-tier status chip (★ Free/Pro/Max) — read-only indicator next to the avatar. */
function PlanChip() {
  const me = useMe()
  const plan = me.plan || 'free'
  const label = PLAN_LABELS[plan] || (plan[0].toUpperCase() + plan.slice(1))
  return (
    <span className={'plan-chip ' + plan} title={`${label} plan`}>
      <span className="star">★</span>{label}
    </span>
  )
}

function Avatar() {
  const { user } = useUser()
  const initials = ((user?.firstName?.[0] || '') + (user?.lastName?.[0] || '')) || 'IL'
  return (
    <NavLink to="/account" className="avatar" title={user?.firstName || 'Account'}
      aria-label="Account" style={{ padding: 0, overflow: 'hidden', textDecoration: 'none' }}>
      {user?.imageUrl
        ? <img src={user.imageUrl} alt={initials}
            style={{ width: '100%', height: '100%', objectFit: 'cover', borderRadius: '50%' }} />
        : initials}
    </NavLink>
  )
}

function WalletChip() {
  const me = useMe()
  if (me.plan === 'free') return null
  return (
    <span className="wallet-chip" title="Wallet balance">{fmtCents(me.balance_cents)}</span>
  )
}

export default function AppShell({ children }) {
  const [modalRow, setModalRow] = useState(null)
  const openTicker = (ticker, company) => { if (ticker) setModalRow({ ticker, company }) }
  return (
    <TickerModalContext.Provider value={openTicker}>
    <MeProvider>
    <div className="rd">
      <Tape />
      <div className="top">
        <div className="top-left">
          <NavLink to="/market" className="logo" style={{ textDecoration: 'none', color: 'inherit' }}>
            <svg className="logo-mark" viewBox="0 0 100 100" fill="none" aria-label="VeSign">
              <defs>
                <linearGradient id="rd-grad-nav" x1="50" y1="0" x2="50" y2="100" gradientUnits="userSpaceOnUse">
                  <stop offset="0%" stopColor="#a5f3fc" />
                  <stop offset="35%" stopColor="#22d3ee" />
                  <stop offset="75%" stopColor="#3b82f6" />
                  <stop offset="100%" stopColor="#1d4ed8" />
                </linearGradient>
              </defs>
              <path d="M4 22 L50 96 L96 22 L78 32 L50 70 L22 32 Z" fill="url(#rd-grad-nav)" />
              <path d="M40 32 Q50 22 60 32" stroke="url(#rd-grad-nav)" strokeWidth="2.2" fill="none" />
              <path d="M34 28 Q50 14 66 28" stroke="url(#rd-grad-nav)" strokeWidth="2" fill="none" opacity="0.85" />
              <path d="M28 24 Q50 6 72 24" stroke="url(#rd-grad-nav)" strokeWidth="1.8" fill="none" opacity="0.7" />
            </svg>
            <div className="logo-text">VeSign</div>
          </NavLink>
          <nav className="nav">
            <NavLink to="/market">Market</NavLink>
            <NavLink to="/signals">Signals</NavLink>
            <NavLink to="/portfolio">Portfolio</NavLink>
            <NavLink to="/research">Research</NavLink>
          </nav>
        </div>
        <div className="header-search-slot" style={{ width: 380, maxWidth: '100%' }}>
          <GlobalSearch />
        </div>
        <div className="topright">
          <WalletChip />
          <MarketChip />
          <PlanChip />
          <Avatar />
        </div>
      </div>
      {children}
      {modalRow && <SignalModal row={modalRow} onClose={() => setModalRow(null)} />}
    </div>
    </MeProvider>
    </TickerModalContext.Provider>
  )
}
