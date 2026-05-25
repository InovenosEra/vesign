/* Redesign shared shell — the new top nav for redesigned routes.
 * Renders inside a <div className="rd"> so the scoped redesign.css applies only
 * here, leaving un-ported pages on the old design. Wired to the REAL contexts:
 * currency actually converts (useCurrency), language actually switches (i18n),
 * market-status is live, search is the real GlobalSearch. */
import { useState, useRef, useEffect } from 'react'
import { NavLink } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useUser } from '@clerk/react'
import { getMarketStatus } from '../api'
import { useCurrency } from '../context/CurrencyContext'
import GlobalSearch from '../components/GlobalSearch'
import SignalModal from './SignalModalRd'
import { TickerModalContext } from './TickerModalContext'
import Tape from './market/Tape'
import './redesign.css'

const LANGS = [
  { code: 'en', label: 'EN', flag: '🇬🇧', name: 'English' },
  { code: 'he', label: 'HE', flag: '🇮🇱', name: 'עברית' },
  { code: 'es', label: 'ES', flag: '🇪🇸', name: 'Español' },
  { code: 'fr', label: 'FR', flag: '🇫🇷', name: 'Français' },
  { code: 'de', label: 'DE', flag: '🇩🇪', name: 'Deutsch' },
  { code: 'it', label: 'IT', flag: '🇮🇹', name: 'Italiano' },
]
const CCYS = [
  { code: 'USD', sym: '$', name: 'US Dollar' },
  { code: 'EUR', sym: '€', name: 'Euro' },
  { code: 'ILS', sym: '₪', name: 'Israeli Shekel' },
]

const Caret = () => (
  <svg className="hs-caret" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3"><polyline points="6 9 12 15 18 9" /></svg>
)

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
  const open = data?.phase ? data.phase === 'regular' : null
  const cd = useCountdown(data?.next_regular_event_utc || data?.next_event_utc)
  return (
    <div className={'status' + (open === false ? ' closed' : '')}>
      <span className="dot" />{' '}
      <span>{open == null ? 'Market…' : open ? 'Market open' : 'Market closed'}</span>
      {cd && <span className="ct">{cd}</span>}
    </div>
  )
}

/* Generic .hdr-select dropdown used by language + currency. */
function HdrSelect({ items, current, onPick, renderBtn, renderRow }) {
  const [open, setOpen] = useState(false)
  const ref = useRef(null)
  useEffect(() => {
    const onDown = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', onDown)
    return () => document.removeEventListener('mousedown', onDown)
  }, [])
  const cur = items.find(i => i.code === current) || items[0]
  return (
    <div className={'hdr-select' + (open ? ' open' : '')} ref={ref}>
      <button className="hs-btn" type="button" aria-haspopup="true" aria-expanded={open}
        onClick={() => setOpen(o => !o)}>
        {renderBtn(cur)}<Caret />
      </button>
      <div className="hs-menu" role="menu">
        {items.map(i => (
          <button key={i.code} type="button" role="menuitem"
            className={'hs-row' + (i.code === current ? ' sel' : '')}
            onClick={() => { onPick(i.code); setOpen(false) }}>
            {renderRow(i)}
          </button>
        ))}
      </div>
    </div>
  )
}

function LangSelect() {
  const { i18n } = useTranslation()
  const cur = (i18n.language || 'en').slice(0, 2)
  const pick = (code) => {
    i18n.changeLanguage(code)
    try { localStorage.setItem('lang', code) } catch { /* ignore */ }
    document.documentElement.dir = code === 'he' ? 'rtl' : 'ltr'
    document.documentElement.lang = code
  }
  return (
    <HdrSelect items={LANGS} current={cur} onPick={pick}
      renderBtn={c => <><span className="hs-flag">{c.flag}</span><span className="hs-lbl">{c.label}</span></>}
      renderRow={i => <><span className="hs-flag">{i.flag}</span><span className="hs-name">{i.name}</span></>} />
  )
}

function CcySelect() {
  const { currency, setCurrency } = useCurrency()
  return (
    <HdrSelect items={CCYS} current={currency} onPick={setCurrency}
      renderBtn={c => <><span className="hs-sym">{c.sym}</span><span className="hs-lbl">{c.code}</span></>}
      renderRow={i => <><span className="hs-sym">{i.sym}</span><span className="hs-name">{i.code}</span></>} />
  )
}

function Avatar() {
  const { user } = useUser()
  const initials = ((user?.firstName?.[0] || '') + (user?.lastName?.[0] || '')) || 'IL'
  if (user?.imageUrl) {
    return <img className="avatar" src={user.imageUrl} alt={initials}
      style={{ objectFit: 'cover' }} title={user.firstName || ''} />
  }
  return <div className="avatar" title={user?.firstName || ''}>{initials}</div>
}

export default function AppShell({ children }) {
  const [modalRow, setModalRow] = useState(null)
  const openTicker = (ticker, company) => { if (ticker) setModalRow({ ticker, company }) }
  return (
    <TickerModalContext.Provider value={openTicker}>
    <div className="rd">
      <Tape />
      <div className="top">
        <div className="top-left">
          <div className="logo">
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
          </div>
          <nav className="nav">
            <NavLink to="/market">Market</NavLink>
            <NavLink to="/" end>Signals</NavLink>
            <NavLink to="/portfolio">Portfolio</NavLink>
            <NavLink to="/research">Research</NavLink>
          </nav>
        </div>
        <div className="header-search-slot" style={{ width: 380, maxWidth: '100%' }}>
          <GlobalSearch />
        </div>
        <div className="topright">
          <MarketChip />
          <LangSelect />
          <CcySelect />
          <Avatar />
        </div>
      </div>
      {children}
      {modalRow && <SignalModal row={modalRow} onClose={() => setModalRow(null)} />}
    </div>
    </TickerModalContext.Provider>
  )
}
