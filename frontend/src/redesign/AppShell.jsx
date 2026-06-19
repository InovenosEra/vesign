/* Redesign shared shell — the new top nav for redesigned routes.
 * Renders inside a <div className="rd"> so the scoped redesign.css applies only
 * here, leaving un-ported pages on the old design. Wired to the REAL contexts:
 * market-status is live, search is the real GlobalSearch. (Currency + language
 * live in Account → Profile, not the header.) */
import { useState, useEffect, useRef } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useUser, useClerk } from '@clerk/react'
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

/* Phase glyphs for the market-status chip: sun = pre-market, moon = post-market.
   Stroke icons in the app's existing inline-SVG style; stroke="currentColor" lets
   them inherit the amber `.status.extended` color, and CSS adds a matching glow. */
function SunIcon(props) {
  return (
    <svg {...props} width="13" height="13" viewBox="0 0 24 24" fill="none"
         stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <circle cx="12" cy="12" r="5" />
      <line x1="12" y1="1" x2="12" y2="3" />
      <line x1="12" y1="21" x2="12" y2="23" />
      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
      <line x1="1" y1="12" x2="3" y2="12" />
      <line x1="21" y1="12" x2="23" y2="12" />
      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
      <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
    </svg>
  )
}
function MoonIcon(props) {
  return (
    <svg {...props} width="13" height="13" viewBox="0 0 24 24" fill="none"
         stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  )
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
      {phase === 'pre' ? <SunIcon className="status-icon" />
        : phase === 'post' ? <MoonIcon className="status-icon" />
        : <span className="dot" />}{' '}
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

const MenuCaret = () => (
  <svg className="menu-caret" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><polyline points="6 9 12 15 18 9" /></svg>
)

function AccountMenu() {
  const { user } = useUser()
  const me = useMe()
  const plan = me.plan || 'free'
  const { signOut } = useClerk()
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  useEffect(() => {
    const onDown = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [])
  const initials = ((user?.firstName?.[0] || '') + (user?.lastName?.[0] || '')) || 'IL'
  const firstName = user?.firstName || 'there'
  // Password lives in the Security pane; picture editor opens on Profile.
  const go = (m) => { setOpen(false); navigate(m === 'password' ? '/account/security?m=password' : '/account/profile?m=picture') }
  return (
    <div className={'account-menu' + (open ? ' open' : '')} ref={ref}>
      <div className="account-trigger">
        <NavLink to="/account" className="account-id" title={user?.firstName || 'Account'} aria-label="Account">
          <span className={'avatar tier-' + plan}>
            {user?.imageUrl
              ? <img src={user.imageUrl} alt={initials}
                  style={{ width: '100%', height: '100%', objectFit: 'cover', borderRadius: '50%' }} />
              : initials}
          </span>
          <span className="greeting">Hello, {firstName}</span>
        </NavLink>
        <button type="button" className="account-caret" aria-haspopup="true" aria-expanded={open}
          aria-label="Account menu" onClick={() => setOpen(o => !o)}>
          <MenuCaret />
        </button>
      </div>
      <div className="hs-menu" role="menu">
        <button type="button" role="menuitem" className="hs-row" onClick={() => go('picture')}>Edit Profile Picture</button>
        <button type="button" role="menuitem" className="hs-row" onClick={() => go('password')}>Change Password</button>
        <button type="button" role="menuitem" className="hs-row signout"
          onClick={() => { setOpen(false); signOut({ redirectUrl: '/sign-in' }) }}>Sign Out</button>
      </div>
    </div>
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
  // Make the document canvas match the redesign --bg (#0a0e15) while the app is
  // mounted, so overscroll / lazy-load / partial-paint never reveal the legacy
  // html background (#0b0e18). Restored on unmount so marketing pages keep theirs.
  useEffect(() => {
    const html = document.documentElement
    const prev = html.style.background
    html.style.background = '#0a0e15'
    return () => { html.style.background = prev }
  }, [])
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
          <AccountMenu />
        </div>
      </div>
      {children}
      {modalRow && <SignalModal row={modalRow} onClose={() => setModalRow(null)} />}
    </div>
    </MeProvider>
    </TickerModalContext.Provider>
  )
}
