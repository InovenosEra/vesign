/* Shared shell for the public auth pages (/sign-in, /sign-up): marketing nav
 * with a cross-link, centered main column, research-only legal line. Also the
 * single source of the dark Clerk `appearance` theme so both prebuilt
 * components stay visually identical. */
import { useEffect } from 'react'
import { Link } from 'react-router-dom'
import './redesign.css'
import './landing.css'

/* Dark .rd theme for Clerk's prebuilt components. Clerk's appearance variables
 * need literal values (they feed Clerk's style engine, not our cascade) —
 * these mirror the redesign tokens in redesign.css. clerk-js renamed its theme
 * variables at some point, so BOTH generations are shipped. */
export const CLERK_APPEARANCE = {
  layout: {
    /* Silences the "Development mode" ribbon shown for pk_test_ instances.
     * Cosmetic only — the real fix is switching to a pk_live_ key via
     * Clerk's "Deploy to Production" flow. */
    unsafe_disableDevelopmentModeWarnings: true,
  },
  variables: {
    colorPrimary: '#3b82f6',           /* --blue   */
    colorBackground: '#0e131c',        /* --panel  */
    colorText: '#e8eaf0',              /* --ink    (legacy name) */
    colorForeground: '#e8eaf0',        /* --ink    (current name) */
    colorTextSecondary: '#a8acb8',     /* --ink-2  (legacy) */
    colorMutedForeground: '#a8acb8',   /* --ink-2  (current) */
    colorInputBackground: '#11161f',   /* --bg-2   (legacy) */
    colorInput: '#11161f',             /* --bg-2   (current) */
    colorInputText: '#e8eaf0',         /* legacy */
    colorInputForeground: '#e8eaf0',   /* current */
    colorNeutral: '#e8eaf0',
    colorDanger: '#ff4d5c',            /* --red    */
    colorSuccess: '#00d97e',           /* --green  */
    borderRadius: '8px',
    fontFamily: "'Inter Variable', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
  },
  elements: {
    card: { border: '1px solid rgba(255,255,255,0.10)', boxShadow: '0 24px 70px -28px rgba(0,0,0,0.8)' },
    headerTitle: { letterSpacing: '-0.02em' },
    footerActionLink: { color: '#60a5fa' },
  },
}

/* Same V-mark as the landing/app shells; gradient id is parameterized so two
 * mounted instances never collide in the defs namespace. */
export function LogoMark({ id = 'auth', className = 'ld-logo-mark' }) {
  const gid = `rd-grad-${id}`
  return (
    <svg className={className} viewBox="0 0 100 100" fill="none" aria-label="VeSign">
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

export default function AuthLayout({ hint, ctaLabel, ctaTo, children }) {
  // Keep the document canvas on the redesign --bg while mounted (AppShell pattern).
  useEffect(() => {
    const html = document.documentElement
    const prev = html.style.background
    html.style.background = '#0a0e15'
    return () => { html.style.background = prev }
  }, [])

  return (
    <div className="rd ld ld-auth">
      <header className="ld-nav">
        <Link to="/" className="ld-nav-logo">
          <LogoMark />
          <span className="ld-logo-text">VeSign</span>
        </Link>
        <div className="ld-nav-ctas">
          {hint && <span className="ld-auth-hint">{hint}</span>}
          <Link to={ctaTo} className="ld-btn ghost">{ctaLabel}</Link>
        </div>
      </header>
      <main className="ld-auth-main">
        {children}
        <p className="ld-auth-legal">
          By continuing you agree that VeSign provides research and information
          only — not investment advice.
        </p>
      </main>
    </div>
  )
}
