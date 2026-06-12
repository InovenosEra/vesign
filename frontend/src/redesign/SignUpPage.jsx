/* VeSign self-serve sign-up — Clerk's prebuilt <SignUp /> (handles email
 * verification) themed to the dark .rd design system. Public route /sign-up.
 * New users carry NO entitlement record, so backend/entitlements.get_plan()
 * resolves them to the FREE tier — paid features stay behind the paywall. */
import { SignUp } from '@clerk/react'
import { useEffect } from 'react'
import { Link } from 'react-router-dom'
import './redesign.css'
import './landing.css'

/* Dark .rd theme for the prebuilt component. Clerk's appearance variables need
 * literal values (they feed Clerk's own style engine, not our cascade) — these
 * mirror the redesign tokens in redesign.css. */
const APPEARANCE = {
  variables: {
    colorPrimary: '#3b82f6',           /* --blue   */
    colorBackground: '#0e131c',        /* --panel  */
    /* clerk-js renamed the theme variables; ship BOTH generations so the
       theme holds across clerk-js minor upgrades. */
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

/* Same V-mark as the landing/app shells, own gradient id to avoid collisions. */
function LogoMark() {
  return (
    <svg className="ld-logo-mark" viewBox="0 0 100 100" fill="none" aria-label="VeSign">
      <defs>
        <linearGradient id="rd-grad-signup" x1="50" y1="0" x2="50" y2="100" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#a5f3fc" />
          <stop offset="35%" stopColor="#22d3ee" />
          <stop offset="75%" stopColor="#3b82f6" />
          <stop offset="100%" stopColor="#1d4ed8" />
        </linearGradient>
      </defs>
      <path d="M4 22 L50 96 L96 22 L78 32 L50 70 L22 32 Z" fill="url(#rd-grad-signup)" />
      <path d="M40 32 Q50 22 60 32" stroke="url(#rd-grad-signup)" strokeWidth="2.2" fill="none" />
      <path d="M34 28 Q50 14 66 28" stroke="url(#rd-grad-signup)" strokeWidth="2" fill="none" opacity="0.85" />
      <path d="M28 24 Q50 6 72 24" stroke="url(#rd-grad-signup)" strokeWidth="1.8" fill="none" opacity="0.7" />
    </svg>
  )
}

export default function SignUpPage() {
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
          <span className="ld-auth-hint">Already have an account?</span>
          <Link to="/sign-in" className="ld-btn ghost">Log in</Link>
        </div>
      </header>
      <main className="ld-auth-main">
        <div className="ld-auth-head">
          <h1>Create your free account</h1>
          <p>Free tier, no credit card. Upgrade only if the signals earn it.</p>
        </div>
        {/* forceRedirectUrl (Clerk v6): always land on /market after the full
            sign-up flow, including the built-in email verification step. */}
        <SignUp forceRedirectUrl="/market" signInUrl="/sign-in" appearance={APPEARANCE} />
        <p className="ld-auth-legal">
          By signing up you agree that VeSign provides research and information
          only — not investment advice.
        </p>
      </main>
    </div>
  )
}
