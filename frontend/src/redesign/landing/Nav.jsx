import { useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Link } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import { track } from './analytics'
import { useScrollProgress } from './hooks'

/* Same V-mark used across landing/auth/app shells; each mounted instance
 * parameterizes the gradient id so multiple copies on one page never
 * collide in the SVG defs namespace. */
export function LogoMark({ id = 'nav', className }) {
  const gid = `rd-grad-${id}`
  return (
    <svg className={className} viewBox="0 0 100 100" fill="none" aria-hidden="true">
      <defs>
        <linearGradient id={gid} x1="50" y1="0" x2="50" y2="100" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#a5f3fc" />
          <stop offset="35%" stopColor="#22d3ee" />
          <stop offset="75%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#1d4ed8" />
        </linearGradient>
      </defs>
      <path d="M4 22 L50 96 L96 22 L78 32 L50 70 L22 32 Z" fill={`url(#${gid})`} />
      <path d="M40 32 Q50 22 60 32" stroke={`url(#${gid})`} strokeWidth="2.2" fill="none" />
      <path d="M34 28 Q50 14 66 28" stroke={`url(#${gid})`} strokeWidth="2" fill="none" opacity="0.85" />
      <path d="M28 24 Q50 6 72 24" stroke={`url(#${gid})`} strokeWidth="1.8" fill="none" opacity="0.7" />
    </svg>
  )
}

const NAV_LINKS = [
  { href: '#how', key: 'ld.nav.how' },
  { href: '#proof', key: 'ld.nav.proof' },
  { href: '#platform', key: 'ld.nav.platform' },
  { href: '#pricing', key: 'ld.nav.pricing' },
  { href: '#faq', key: 'ld.nav.faq' },
]

const LANGS = [
  { code: 'en', flag: '🇺🇸' }, { code: 'he', flag: '🇮🇱' }, { code: 'es', flag: '🇪🇸' },
  { code: 'fr', flag: '🇫🇷' }, { code: 'de', flag: '🇩🇪' }, { code: 'it', flag: '🇮🇹' },
]

function applyLanguage(i18n, code) {
  i18n.changeLanguage(code)
  localStorage.setItem('lang', code)
  document.documentElement.dir = code === 'he' ? 'rtl' : 'ltr'
  document.documentElement.lang = code
}

function LangSwitcher({ inMenu = false }) {
  const { t, i18n } = useTranslation()
  const [open, setOpen] = useState(false)
  const ref = useRef(null)

  useEffect(() => {
    if (!open) return
    const onDocClick = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    const onEsc = (e) => { if (e.key === 'Escape') setOpen(false) }
    document.addEventListener('mousedown', onDocClick)
    document.addEventListener('keydown', onEsc)
    return () => {
      document.removeEventListener('mousedown', onDocClick)
      document.removeEventListener('keydown', onEsc)
    }
  }, [open])

  const current = LANGS.find((l) => l.code === i18n.language) || LANGS[0]

  return (
    <div className={'ld-lang-switcher' + (open ? ' open' : '') + (inMenu ? ' in-menu' : '')} ref={ref}>
      <button
        type="button"
        className="ld-lang-btn"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={t('ld.nav.language')}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="ld-lang-flag" aria-hidden="true">{current.flag}</span>
        <span className="ld-lang-code">{t(`lang.${current.code}`)}</span>
      </button>
      <div className="ld-lang-menu" role="listbox">
        {LANGS.map((l) => (
          <button
            key={l.code}
            type="button"
            role="option"
            aria-selected={l.code === current.code}
            className={'ld-lang-row' + (l.code === current.code ? ' sel' : '')}
            onClick={() => { applyLanguage(i18n, l.code); setOpen(false) }}
          >
            <span className="ld-lang-flag" aria-hidden="true">{l.flag}</span>
            <span>{t(`lang.${l.code}`)}</span>
          </button>
        ))}
      </div>
    </div>
  )
}

// Focus trap: Tab/Shift+Tab wrap within the panel's focusable elements,
// Esc closes and returns focus to the toggle button that opened it.
function useMobileMenuA11y(open, onClose, panelRef, toggleRef) {
  useEffect(() => {
    if (!open) return
    const panel = panelRef.current
    if (!panel) return
    const focusables = () => Array.from(
      panel.querySelectorAll('a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])')
    )
    const first = focusables()[0]
    first?.focus()

    const onKeyDown = (e) => {
      if (e.key === 'Escape') {
        e.preventDefault()
        onClose()
        toggleRef.current?.focus()
        return
      }
      if (e.key !== 'Tab') return
      const items = focusables()
      if (!items.length) return
      const firstEl = items[0]
      const lastEl = items[items.length - 1]
      if (e.shiftKey && document.activeElement === firstEl) {
        e.preventDefault(); lastEl.focus()
      } else if (!e.shiftKey && document.activeElement === lastEl) {
        e.preventDefault(); firstEl.focus()
      }
    }
    document.addEventListener('keydown', onKeyDown)
    return () => document.removeEventListener('keydown', onKeyDown)
  }, [open, onClose, panelRef, toggleRef])
}

// Nav sits permanently elevated (blur + shadow) so it never looks bare, but
// gains a touch more opacity/depth once the page has actually scrolled —
// rAF-throttled so the scroll listener never does layout work per-frame.
function useScrolled(threshold = 8) {
  const [scrolled, setScrolled] = useState(false)
  useEffect(() => {
    let ticking = false
    const check = () => { setScrolled(window.scrollY > threshold); ticking = false }
    const onScroll = () => {
      if (ticking) return
      ticking = true
      requestAnimationFrame(check)
    }
    onScroll()
    window.addEventListener('scroll', onScroll, { passive: true })
    return () => window.removeEventListener('scroll', onScroll)
  }, [threshold])
  return scrolled
}

export function LandingNav() {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)
  const panelRef = useRef(null)
  const toggleRef = useRef(null)
  const menuId = useId()
  const scrolled = useScrolled()
  const progress = useScrollProgress()
  useMobileMenuA11y(open, () => setOpen(false), panelRef, toggleRef)

  // Body scroll lock while the mobile panel is open.
  useEffect(() => {
    if (!open) return
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => { document.body.style.overflow = prev }
  }, [open])

  const cta = (location, label, extra) => () => track('landing_cta_click', { location, label, ...extra })

  return (
    <header className={'ld-nav' + (scrolled ? ' scrolled' : '')}>
      <a className="ld-skip-link" href="#main">{t('ld.nav.skipToContent')}</a>
      <a href="#top" className="ld-nav-logo">
        <LogoMark id="nav" className="ld-logo-mark" />
        <span className="ld-logo-text">VeSign</span>
      </a>
      <nav className="ld-nav-links" aria-label={t('ld.nav.primaryLabel')}>
        {NAV_LINKS.map((l) => (
          <a key={l.href} href={l.href}>{t(l.key)}</a>
        ))}
      </nav>
      <div className="ld-nav-ctas">
        <LangSwitcher />
        <span className="ld-nav-divider" aria-hidden="true" />
        <Link to="/sign-in" className="ld-btn ghost" onClick={cta('nav', 'log_in')}>{t('ld.nav.logIn')}</Link>
        <Link to="/sign-up" className="ld-btn primary" onClick={cta('nav', 'sign_up')}>{t('ld.nav.signUp')}</Link>
      </div>
      <button
        ref={toggleRef}
        type="button"
        className="ld-nav-burger"
        aria-expanded={open}
        aria-controls={menuId}
        aria-label={t(open ? 'ld.nav.closeMenu' : 'ld.nav.openMenu')}
        onClick={() => setOpen((v) => !v)}
      >
        <span /><span /><span />
      </button>
      {/* Scroll progress — width tracks page scroll fraction (useScrollProgress,
       * same rAF-throttled pattern as useScrolled above). transform: scaleX,
       * not width, so this never triggers layout on scroll; set inline since
       * it changes every frame and doesn't belong in a stylesheet rule. */}
      <div
        className="ld-nav-progress"
        style={{ transform: `scaleX(${progress})` }}
        aria-hidden="true"
      />
      {/* Portaled to document.body — .ld-nav has backdrop-filter (the sticky
       * header's frosted-glass look), which establishes a new containing
       * block for any position:fixed descendant. Left in place, this panel's
       * `inset:64px 0 0 0` would resolve against .ld-nav's own 64px-tall box
       * instead of the viewport, collapsing it to a sliver instead of a
       * full-screen overlay. Portaling escapes that containing block.
       * Wrapped in its own `.rd` — every rule here is a `.rd .ld-mobile-menu`
       * etc. DESCENDANT selector, which stops matching once this subtree is
       * outside the app's one `.rd.ld` wrapper; this re-establishes that
       * ancestor (and the --bg/--ink/etc. token scope it defines) locally,
       * as a real ancestor rather than compounded onto the same node.
       * display:contents — the wrapper must stay in the DOM ancestor chain
       * for selector matching but must NOT generate its own box: .rd sets
       * min-height:100vh + a background, which would otherwise render as a
       * second, empty full-height colored panel after <body>'s real content
       * even while the menu is closed. */}
      {createPortal(
        <div className="rd ld" style={{ display: 'contents' }}>
          <div
            id={menuId}
            ref={panelRef}
            className={'ld-mobile-menu' + (open ? ' open' : '')}
            aria-hidden={!open}
          >
            <nav className="ld-mobile-links" aria-label={t('ld.nav.primaryLabel')}>
              {NAV_LINKS.map((l) => (
                <a key={l.href} href={l.href} onClick={() => setOpen(false)}>{t(l.key)}</a>
              ))}
            </nav>
            <div className="ld-mobile-ctas">
              <Link to="/sign-in" className="ld-btn ghost lg" onClick={cta('nav', 'log_in', { device: 'mobile' })}>{t('ld.nav.logIn')}</Link>
              <Link to="/sign-up" className="ld-btn primary lg" onClick={cta('nav', 'sign_up', { device: 'mobile' })}>{t('ld.nav.signUp')}</Link>
            </div>
            <LangSwitcher inMenu />
          </div>
        </div>,
        document.body
      )}
    </header>
  )
}
