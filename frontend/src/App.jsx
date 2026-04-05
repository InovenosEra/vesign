import { useState, useEffect, useContext, useRef } from 'react'
import { BrowserRouter, Routes, Route, NavLink, Link, useLocation } from 'react-router-dom'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { ClerkProvider, RedirectToSignIn, useAuth, useClerk, useUser } from '@clerk/react'
import { useTranslation } from 'react-i18next'
import i18n from './i18n'
import { setTokenGetter } from './api'
import { getMarketStatus } from './api'
import { MarketContext, MarketProvider } from './context/MarketContext'
import SignalsPage from './pages/SignalsPage'
import WatchlistPage from './pages/WatchlistPage'
import TradesPage from './pages/TradesPage'
import GlobalSearch from './components/GlobalSearch'
import ProfilePictureModal from './components/ProfilePictureModal'
import LoginPage from './pages/LoginPage'
import CompleteProfilePage from './pages/CompleteProfilePage'
import AboutPage from './pages/AboutPage'
import ContactPage from './pages/ContactPage'
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
// Countdown hook — counts down to next_event_utc from backend
// ---------------------------------------------------------------------------
function useCountdown(nextEventUtc) {
  const [countdown, setCountdown] = useState('')

  useEffect(() => {
    if (!nextEventUtc) { setCountdown(''); return }
    const targetMs = new Date(nextEventUtc).getTime()
    function tick() { setCountdown(fmtCountdown(targetMs - Date.now())) }
    tick()
    const id = setInterval(tick, 1000)
    return () => clearInterval(id)
  }, [nextEventUtc])

  return countdown
}

// ---------------------------------------------------------------------------
// Language switcher
// ---------------------------------------------------------------------------
const LANGUAGES = [
  { code: 'en', label: 'EN', flag: '🇬🇧' },
  { code: 'he', label: 'HE', flag: '🇮🇱' },
  { code: 'es', label: 'ES', flag: '🇪🇸' },
  { code: 'fr', label: 'FR', flag: '🇫🇷' },
  { code: 'de', label: 'DE', flag: '🇩🇪' },
  { code: 'it', label: 'IT', flag: '🇮🇹' },
]

export function LanguageSwitcher() {
  const { i18n: i18nInstance } = useTranslation()
  const currentLang = i18nInstance.language

  function switchLang(code) {
    localStorage.setItem('lang', code)
    document.documentElement.dir = code === 'he' ? 'rtl' : 'ltr'
    document.documentElement.lang = code
    i18nInstance.changeLanguage(code)
  }

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
      {LANGUAGES.map(lang => (
        <button
          key={lang.code}
          onClick={() => switchLang(lang.code)}
          title={lang.label}
          style={{
            background: 'none', border: 'none', cursor: 'pointer',
            padding: '2px 3px', borderRadius: 4, lineHeight: 1,
            fontSize: 20,
            opacity: currentLang === lang.code ? 1 : 0.35,
            filter: currentLang === lang.code
              ? 'drop-shadow(0 0 5px rgba(83,229,239,0.7))'
              : 'none',
            transition: 'opacity 0.2s, filter 0.2s',
          }}
        >
          {lang.flag}
        </button>
      ))}
    </div>
  )
}

function LanguageSwitcherDropdown() {
  const { i18n: i18nInstance } = useTranslation()
  const currentLang = i18nInstance.language
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    function onMouseDown(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  function switchLang(code) {
    localStorage.setItem('lang', code)
    document.documentElement.dir = code === 'he' ? 'rtl' : 'ltr'
    document.documentElement.lang = code
    i18nInstance.changeLanguage(code)
    setOpen(false)
  }

  const current = LANGUAGES.find(l => l.code === currentLang) ?? LANGUAGES[0]

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          background: 'transparent', border: '1px solid var(--border)',
          borderRadius: 6, padding: '4px 8px', cursor: 'pointer',
          display: 'flex', alignItems: 'center', gap: 5,
          color: 'var(--text)', fontSize: 13, fontWeight: 600,
        }}
      >
        <span style={{ fontSize: 18, lineHeight: 1 }}>{current.flag}</span>
        <span>{current.label}</span>
        <span style={{ fontSize: 10, opacity: 0.6 }}>▾</span>
      </button>
      {open && (
        <div style={{
          position: 'absolute', right: 0, top: 'calc(100% + 6px)',
          background: 'var(--surface)', border: '1px solid var(--border)',
          borderRadius: 8, zIndex: 200, minWidth: 110,
          boxShadow: '0 4px 16px rgba(0,0,0,0.4)', overflow: 'hidden',
        }}>
          {LANGUAGES.map(lang => (
            <button
              key={lang.code}
              onClick={() => switchLang(lang.code)}
              style={{
                display: 'flex', alignItems: 'center', gap: 10,
                width: '100%', padding: '8px 12px',
                background: currentLang === lang.code ? 'rgba(83,229,239,0.1)' : 'transparent',
                border: 'none', cursor: 'pointer',
                color: currentLang === lang.code ? 'var(--accent)' : 'var(--text)',
                fontSize: 13, fontWeight: currentLang === lang.code ? 700 : 400,
                textAlign: 'left',
              }}
            >
              <span style={{ fontSize: 18 }}>{lang.flag}</span>
              <span>{lang.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
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
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const { data } = useQuery({
    queryKey: ['market-status', market],
    queryFn: () => getMarketStatus(market),
    refetchInterval: 60_000,
  })
  const countdown = useCountdown(data?.next_event_utc)

  if (!data) return null
  return data.is_open
    ? (
      <span className="market-open" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
        <span><span className="dot-blink">●</span> {t('market.open')}</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#cccccc', fontWeight: 'normal' }}>{t('market.closesIn', { countdown })}</span>
      </span>
    )
    : (
      <span className="market-closed" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 2 }}>
        <span>● {t('market.closed')}</span>
        <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#cccccc', fontWeight: 'normal' }}>{t('market.opensIn', { countdown })}</span>
      </span>
    )
}

// ---------------------------------------------------------------------------
// User menu (Hello [First name] + dropdown)
// ---------------------------------------------------------------------------
function UserMenu() {
  const { t } = useTranslation()
  const { signOut, openUserProfile } = useClerk()
  const { user } = useUser()
  const [open, setOpen] = useState(false)
  const [showPicModal, setShowPicModal] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    function onMouseDown(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  return (
    <>
      <div ref={ref} style={{ position: 'relative' }}>
        <button
          className="user-menu-btn"
          onClick={() => setOpen(o => !o)}
          style={{
            background: 'transparent',
            border: '1px solid var(--border)',
            borderRadius: 20,
            padding: '3px 12px 3px 4px',
            cursor: 'pointer',
            fontSize: 13,
            color: 'var(--text)',
            whiteSpace: 'nowrap',
            display: 'flex',
            alignItems: 'center',
            gap: 8,
          }}
        >
          {user?.imageUrl
            ? <img src={user.imageUrl} alt="" style={{ width: 28, height: 28, borderRadius: '50%', objectFit: 'cover' }} />
            : <div style={{ width: 28, height: 28, borderRadius: '50%', background: 'var(--accent)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, fontWeight: 700, color: '#fff' }}>
                {user?.firstName?.[0]}
              </div>
          }
          <span className="user-hello">{t('header.hello')} </span><span className="user-name">{user?.firstName}</span> ▾
        </button>
        {open && (
          <div style={{
            position: 'absolute', right: 0, top: 'calc(100% + 6px)',
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 8, zIndex: 200, minWidth: 180,
            boxShadow: '0 4px 16px rgba(0,0,0,0.4)', overflow: 'hidden',
          }}>
            {[
              { labelKey: 'header.editPicture', action: () => { setShowPicModal(true); setOpen(false) } },
              { labelKey: 'header.changePassword', action: () => { openUserProfile(); setOpen(false) } },
              { labelKey: 'header.signOut', action: () => signOut({ redirectUrl: '/sign-in' }) },
            ].map(item => (
              <button
                key={item.labelKey}
                onClick={item.action}
                style={{
                  display: 'block', width: '100%', padding: '10px 16px',
                  background: 'transparent', border: 'none', cursor: 'pointer',
                  color: 'var(--text)', fontSize: 13, textAlign: 'left',
                }}
              >
                {t(item.labelKey)}
              </button>
            ))}
          </div>
        )}
      </div>
      {showPicModal && <ProfilePictureModal onClose={() => setShowPicModal(false)} />}
    </>
  )
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------
function Header() {
  const { t } = useTranslation()
  const { market } = useContext(MarketContext)
  const [mobileNavOpen, setMobileNavOpen] = useState(false)
  const closeNav = () => setMobileNavOpen(false)

  return (
    <>
      <header className="app-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 24, flexShrink: 0 }}>
          <NavLink to="/" onClick={closeNav} style={{ textDecoration: 'none' }}>
            <h1 style={{ display: 'flex', alignItems: 'center', gap: 2, fontWeight: 900, fontSize: '2.7rem', letterSpacing: '0.08em', fontFamily: "'Segoe UI', system-ui, sans-serif", margin: 0, marginTop: '-6px', cursor: 'pointer', direction: 'ltr' }}>
              <img src="/favicon.png" alt="V" style={{ height: '3.2rem', objectFit: 'contain', flexShrink: 0, filter: 'drop-shadow(0 2px 4px rgba(0, 210, 255, 0.6))' }} />
              <span className="title-shimmer" style={{ letterSpacing: '0.08em' }}>esign</span>
            </h1>
          </NavLink>
          <nav className="desktop-nav">
            <NavLink to="/">{t('nav.signals')}</NavLink>
            <NavLink to="/trades">{t('nav.trades')}</NavLink>
            <NavLink to="/portfolio">{t('nav.portfolio')}</NavLink>
            <NavLink to="/about">{t('nav.about')}</NavLink>
          </nav>
        </div>
        <div className="header-search-wrap" style={{ flex: 1, display: 'flex', justifyContent: 'center' }}>
          <GlobalSearch />
        </div>
        <div className="header-right" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span className="header-market-status-wrap"><MarketStatus /></span>
          <FlagSelector />
          <span className="lang-switcher-header"><LanguageSwitcherDropdown /></span>
          <span className="header-currency-wrap" style={{ fontSize: 13, fontWeight: 700, color: 'var(--muted)', minWidth: 14, textAlign: 'center' }}>
            {market === 'IL' ? '₪' : '$'}
          </span>
          <UserMenu />
          <button
            className="hamburger"
            onClick={() => setMobileNavOpen(o => !o)}
            aria-label={t('header.menu')}
          >
            {mobileNavOpen ? '✕' : '☰'}
          </button>
        </div>
      </header>
      {mobileNavOpen && (
        <div className="mobile-menu">
          <NavLink to="/" onClick={closeNav}>{t('nav.signals')}</NavLink>
          <NavLink to="/trades" onClick={closeNav}>{t('nav.trades')}</NavLink>
          <NavLink to="/portfolio" onClick={closeNav}>{t('nav.portfolio')}</NavLink>
          <NavLink to="/about" onClick={closeNav}>{t('nav.about')}</NavLink>
          <div className="mobile-menu-divider" />
          <div className="mobile-menu-market"><MarketStatus /></div>
          <div className="mobile-menu-divider" />
          <div className="mobile-menu-langs">
            {LANGUAGES.map(lang => (
              <button
                key={lang.code}
                className={`mobile-lang-btn${i18n.language === lang.code ? ' active' : ''}`}
                onClick={() => {
                  i18n.changeLanguage(lang.code)
                  localStorage.setItem('lang', lang.code)
                  document.documentElement.dir = lang.code === 'he' ? 'rtl' : 'ltr'
                  document.documentElement.lang = lang.code
                  closeNav()
                }}
              >{lang.flag}</button>
            ))}
          </div>
        </div>
      )}
    </>
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
  const location = useLocation()
  const [tokenReady, setTokenReady] = useState(false)

  if (!isLoaded) return null
  const PUBLIC_PATHS = ['/about', '/contact']
  if (!userId && !PUBLIC_PATHS.includes(location.pathname)) return <RedirectToSignIn />
  if (!userId && location.pathname === '/about') return <AboutPage standalone />
  if (!userId) return <ContactPage standalone />
  if (user && (!user.firstName || !user.lastName)) return <CompleteProfilePage />

  return (
    <MarketProvider>
      <QueryClientProvider client={queryClient}>
        <TokenSync onReady={() => setTokenReady(true)} />
        {tokenReady && <Header />}
        {tokenReady && (
          <>
            <main className="app-main">
              <Routes>
                <Route path="/" element={<SignalsPage />} />
                <Route path="/portfolio" element={<WatchlistPage />} />
                <Route path="/trades" element={<TradesPage />} />
                <Route path="/about" element={<AboutPage />} />
              <Route path="/contact" element={<ContactPage />} />
              </Routes>
            </main>
            <Footer />
          </>
        )}
      </QueryClientProvider>
    </MarketProvider>
  )
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------
export function PublicHeader() {
  const { t } = useTranslation()
  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100,
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      padding: '12px 24px', borderBottom: '1px solid var(--border)',
      background: 'radial-gradient(ellipse 100% 80% at 0% 0%, rgba(45,147,204,0.16) 0%, transparent 100%), radial-gradient(ellipse 80% 80% at 100% 100%, rgba(83,229,239,0.09) 0%, transparent 100%), #0b0e18',
      backgroundAttachment: 'fixed',
      direction: 'ltr',
    }}>
      <Link to="/" style={{ textDecoration: 'none' }}>
        <h1 style={{
          display: 'flex', alignItems: 'center', gap: 2,
          fontWeight: 900, fontSize: '2rem', letterSpacing: '0.08em',
          fontFamily: "'Segoe UI', system-ui, sans-serif", margin: 0, direction: 'ltr',
        }}>
          <img src="/favicon.png" alt="V" style={{ height: '2.4rem', objectFit: 'contain', filter: 'drop-shadow(0 2px 6px rgba(0,210,255,0.7))' }} />
          <span className="title-shimmer" style={{ letterSpacing: '0.08em' }}>esign</span>
        </h1>
      </Link>
      <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
        <Link to="/contact" className="header-contact-link">{t('nav.contact')}</Link>
        <LanguageSwitcher />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
export function Footer() {
  return (
    <footer style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      flexWrap: 'wrap', gap: 8,
      padding: '18px 28px', borderTop: '1px solid var(--border)',
      fontSize: 12, color: 'var(--muted, #666)',
    }}>
      <span>© {new Date().getFullYear()} Vesign. All rights reserved.</span>
      <a href="/contact" style={{ color: 'var(--accent)', textDecoration: 'none', fontWeight: 600 }}>
        Contact Us
      </a>
    </footer>
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
