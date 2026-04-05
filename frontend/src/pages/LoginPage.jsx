import { useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { useClerk } from '@clerk/react'
import { useTranslation } from 'react-i18next'
import { requestAccess } from '../api'
import { Footer, LanguageSwitcher } from '../App'

// ── Shared ────────────────────────────────────────────────────────────────────
const inputStyle = {
  padding: '12px 14px',
  background: 'var(--bg)',
  border: '1px solid var(--border)',
  borderRadius: 6,
  color: 'var(--text)',
  fontSize: 16,
  outline: 'none',
  width: '100%',
  boxSizing: 'border-box',
}

const FEATURES = [
  { icon: '📊', key: 'login.feature1' },
  { icon: '📈', key: 'login.feature2' },
  { icon: '💼', key: 'login.feature3' },
]

const STATS = [
  { display: '4,200+', labelKey: 'landing.statSignals' },
  { display: '67%',    labelKey: 'landing.statWinRate'  },
  { display: '+12.4%', labelKey: 'landing.statAvgYield' },
  { display: '5,000+', labelKey: 'landing.statStocks'   },
]

const AGREEMENT_SECTION_KEYS = [
  { titleKey: 'agreement.s1title', bodyKey: 'agreement.s1body' },
  { titleKey: 'agreement.s2title', bodyKey: 'agreement.s2body' },
  { titleKey: 'agreement.s3title', bodyKey: 'agreement.s3body' },
  { titleKey: 'agreement.s4title', bodyKey: 'agreement.s4body' },
  { titleKey: 'agreement.s5title', bodyKey: 'agreement.s5body' },
]

function ErrorBox({ msg }) {
  if (!msg) return null
  return (
    <div style={{ color: '#ff6b6b', fontSize: 13, padding: '8px 12px', background: 'rgba(255,107,107,0.1)', borderRadius: 6, border: '1px solid rgba(255,107,107,0.3)' }}>
      {msg}
    </div>
  )
}

// ── Slim header ───────────────────────────────────────────────────────────────
function SlimHeader() {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      padding: '12px 24px', borderBottom: '1px solid rgba(255,255,255,0.08)',
      background: 'rgba(11,14,24,0.85)', backdropFilter: 'blur(10px)',
      position: 'sticky', top: 0, zIndex: 10,
    }}>
      <h1 style={{
        display: 'flex', alignItems: 'center', gap: 2,
        fontWeight: 900, fontSize: '2rem', letterSpacing: '0.08em',
        fontFamily: "'Segoe UI', system-ui, sans-serif", margin: 0, direction: 'ltr',
      }}>
        <img src="/favicon.png" alt="V" style={{ height: '2.4rem', objectFit: 'contain', filter: 'drop-shadow(0 2px 6px rgba(0,210,255,0.7))' }} />
        <span className="title-shimmer" style={{ letterSpacing: '0.08em' }}>esign</span>
      </h1>
      <LanguageSwitcher />
    </div>
  )
}

// ── Modal overlay ─────────────────────────────────────────────────────────────
function Modal({ onClose, children }) {
  return (
    <div
      style={{
        position: 'fixed', inset: 0, zIndex: 100,
        background: 'rgba(0,0,0,0.75)', backdropFilter: 'blur(6px)',
        display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px',
      }}
      onClick={e => { if (e.target === e.currentTarget) onClose() }}
    >
      <div style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 14, padding: '44px 48px', width: '100%', maxWidth: 440,
        boxShadow: '0 24px 80px rgba(0,0,0,0.7)', maxHeight: '90vh', overflowY: 'auto',
      }}>
        {children}
      </div>
    </div>
  )
}

// ── Sign-in form ──────────────────────────────────────────────────────────────
function SignInForm({ onRequestAccess }) {
  const { t } = useTranslation()
  const clerk = useClerk()
  const navigate = useNavigate()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      const result = await clerk.client.signIn.create({ identifier: email.trim(), password })
      if (result.status === 'complete') {
        await clerk.setActive({ session: result.createdSessionId })
        navigate('/', { replace: true })
      } else {
        setError(`Unexpected status: ${result.status}`)
      }
    } catch (err) {
      const msg = err?.errors?.[0]?.longMessage || err?.errors?.[0]?.message || err?.message || 'Sign-in failed'
      setError(msg)
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
      <h2 style={{ margin: '0 0 4px', fontSize: 22, fontWeight: 700, color: 'var(--text)' }}>
        {t('login.signIn')}
      </h2>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 14, color: 'var(--text-muted, #999)' }}>{t('login.email')}</label>
        <input type="email" value={email} onChange={e => setEmail(e.target.value)} required autoFocus style={inputStyle} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 14, color: 'var(--text-muted, #999)' }}>{t('login.password')}</label>
        <input type="password" value={password} onChange={e => setPassword(e.target.value)} required style={inputStyle} />
      </div>
      <ErrorBox msg={error} />
      <button type="submit" disabled={loading} style={{
        marginTop: 4, padding: '13px', background: 'var(--accent, #2d93cc)',
        color: '#fff', border: 'none', borderRadius: 8, fontWeight: 700, fontSize: 16,
        cursor: loading ? 'not-allowed' : 'pointer', opacity: loading ? 0.7 : 1,
      }}>
        {loading ? t('login.signingIn') : t('login.signIn')}
      </button>
      <button type="button" onClick={onRequestAccess} style={{
        background: 'transparent', border: 'none', color: 'var(--text-muted, #999)',
        fontSize: 14, cursor: 'pointer', textDecoration: 'underline', padding: 0,
      }}>
        {t('login.requestAccess')}
      </button>
    </form>
  )
}

// ── Agreement step ────────────────────────────────────────────────────────────
function AgreementStep({ onAgree, onBack }) {
  const { t } = useTranslation()
  const [name, setName] = useState('')
  const [checked, setChecked] = useState(false)
  const [checkedComms, setCheckedComms] = useState(false)
  const canProceed = name.trim().length > 1 && checked

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      <div>
        <p style={{ margin: '0 0 6px', fontWeight: 700, fontSize: 15, color: 'var(--text)' }}>
          {t('agreement.title')}
        </p>
        <p style={{ margin: 0, fontSize: 13, color: 'var(--muted, #999)' }}>
          {t('agreement.subtitle')}
        </p>
      </div>
      <div style={{
        maxHeight: 260, overflowY: 'auto', padding: '14px 16px',
        background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8,
        fontSize: 13, lineHeight: 1.65, color: 'var(--muted, #bbb)',
      }}>
        {AGREEMENT_SECTION_KEYS.map(s => (
          <div key={s.titleKey} style={{ marginBottom: 14 }}>
            <p style={{ margin: '0 0 4px', fontWeight: 700, color: 'var(--text)', fontSize: 13 }}>{t(s.titleKey)}</p>
            <p style={{ margin: 0 }}>{t(s.bodyKey)}</p>
          </div>
        ))}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 14, color: 'var(--muted, #999)' }}>{t('agreement.nameLabel')}</label>
        <input
          type="text" value={name} onChange={e => setName(e.target.value)}
          placeholder={t('agreement.namePlaceholder')} autoFocus
          style={{ ...inputStyle, fontSize: 15 }}
        />
      </div>
      <label style={{ display: 'flex', alignItems: 'flex-start', gap: 10, cursor: 'pointer', fontSize: 13, color: 'var(--text)' }}>
        <input type="checkbox" checked={checked} onChange={e => setChecked(e.target.checked)}
          style={{ marginTop: 2, accentColor: 'var(--accent)', width: 16, height: 16, flexShrink: 0 }} />
        {t('agreement.checkbox')}
      </label>
      <label style={{ display: 'flex', alignItems: 'flex-start', gap: 10, cursor: 'pointer', fontSize: 13, color: 'var(--text)' }}>
        <input type="checkbox" checked={checkedComms} onChange={e => setCheckedComms(e.target.checked)}
          style={{ marginTop: 2, accentColor: 'var(--accent)', width: 16, height: 16, flexShrink: 0 }} />
        {t('agreement.checkboxComms')}
      </label>
      <button type="button" disabled={!canProceed} onClick={() => onAgree(name.trim())} style={{
        padding: '13px', background: canProceed ? 'var(--accent, #2d93cc)' : 'var(--border)',
        color: canProceed ? '#fff' : 'var(--muted, #666)',
        border: 'none', borderRadius: 8, fontWeight: 700, fontSize: 16,
        cursor: canProceed ? 'pointer' : 'not-allowed', transition: 'background 0.2s',
      }}>
        {t('agreement.cta')}
      </button>
      <button type="button" onClick={onBack} style={{
        background: 'transparent', border: 'none', color: 'var(--muted, #999)',
        fontSize: 14, cursor: 'pointer', textDecoration: 'underline', padding: 0,
      }}>
        {t('agreement.back')}
      </button>
    </div>
  )
}

// ── Request access form ───────────────────────────────────────────────────────
function RequestAccessForm({ onBack }) {
  const { t } = useTranslation()
  const [step, setStep] = useState('agreement')
  const [agreementName, setAgreementName] = useState('')
  const [agreedAt, setAgreedAt] = useState('')
  const [email, setEmail] = useState('')
  const [message, setMessage] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [done, setDone] = useState(false)

  function handleAgree(name) {
    setAgreementName(name)
    setAgreedAt(new Date().toISOString().replace('T', ' ').slice(0, 19))
    setStep('form')
  }

  if (step === 'agreement') return <AgreementStep onAgree={handleAgree} onBack={onBack} />

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await requestAccess(email.trim(), message.trim(), agreementName, agreedAt)
      setDone(true)
    } catch (err) {
      setError(err.message || 'Failed to submit request')
    } finally {
      setLoading(false)
    }
  }

  if (done) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16, textAlign: 'center' }}>
        <div style={{ fontSize: 40 }}>✓</div>
        <p style={{ margin: 0, color: 'var(--text)', fontSize: 16, fontWeight: 600 }}>{t('login.requestSent')}</p>
        <p style={{ margin: 0, color: 'var(--text-muted, #999)', fontSize: 13 }}>{t('login.requestSentDesc')}</p>
        <button type="button" onClick={onBack} style={{
          marginTop: 8, padding: '11px', background: 'transparent',
          border: '1px solid var(--border)', borderRadius: 8,
          color: 'var(--text)', fontSize: 14, cursor: 'pointer',
        }}>
          {t('login.backToSignIn')}
        </button>
      </div>
    )
  }

  return (
    <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <p style={{ margin: 0, color: 'var(--text-muted, #999)', fontSize: 13, textAlign: 'center' }}>
        {t('login.requestDesc')}
      </p>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 13, color: 'var(--text-muted, #999)' }}>{t('login.email')}</label>
        <input type="email" value={email} onChange={e => setEmail(e.target.value)} required autoFocus style={inputStyle} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 13, color: 'var(--text-muted, #999)' }}>{t('login.message')} <span style={{ opacity: 0.5 }}>{t('login.optional')}</span></label>
        <textarea value={message} onChange={e => setMessage(e.target.value)} rows={3} style={{ ...inputStyle, resize: 'vertical', fontFamily: 'inherit' }} />
      </div>
      <ErrorBox msg={error} />
      <button type="submit" disabled={loading} style={{
        marginTop: 4, padding: '11px', background: 'var(--accent, #2d93cc)',
        color: '#fff', border: 'none', borderRadius: 8, fontWeight: 700, fontSize: 14,
        cursor: loading ? 'not-allowed' : 'pointer', opacity: loading ? 0.7 : 1,
      }}>
        {loading ? t('login.sending') : t('login.sendRequest')}
      </button>
      <button type="button" onClick={onBack} style={{
        background: 'transparent', border: 'none', color: 'var(--text-muted, #999)',
        fontSize: 13, cursor: 'pointer', textDecoration: 'underline', padding: 0,
      }}>
        {t('login.backToSignIn')}
      </button>
    </form>
  )
}

// ── Landing content ───────────────────────────────────────────────────────────
function LandingContent({ onSignIn, onRequest }) {
  const { t } = useTranslation()

  return (
    <div className="landing-bg" style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>

      {/* Hero */}
      <section style={{
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', textAlign: 'center',
        padding: 'clamp(60px, 10vh, 100px) 24px clamp(48px, 8vh, 80px)',
        position: 'relative', zIndex: 1,
      }}>
        <p style={{
          fontSize: 13, fontWeight: 700, letterSpacing: '0.2em', textTransform: 'uppercase',
          margin: '0 0 20px', color: 'rgba(83,229,239,0.8)',
        }}>
          {t('login.aboutUs')}
        </p>

        <h2 className="login-welcome-title" style={{
          fontSize: 'clamp(2.4rem, 7vw, 4.5rem)', fontWeight: 900,
          margin: '0 0 24px', letterSpacing: '-0.03em', lineHeight: 1.05,
          background: 'linear-gradient(135deg, #ffffff 0%, #53e5ef 45%, #2d93cc 100%)',
          WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text',
        }}>
          {t('login.welcome')}
        </h2>

        <p style={{
          fontSize: 'clamp(1rem, 2.2vw, 1.2rem)', color: 'rgba(200,210,230,0.75)',
          maxWidth: 580, lineHeight: 1.75, margin: '0 0 52px',
        }}>
          {t('login.pitch')}
        </p>

        {/* CTA buttons */}
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', justifyContent: 'center' }}>
          <button
            onClick={onSignIn}
            className="landing-btn-primary"
          >
            {t('login.signIn')}
          </button>
          <button
            onClick={onRequest}
            className="landing-btn-secondary"
          >
            {t('login.requestAccess')}
          </button>
        </div>
      </section>

      {/* Stats row */}
      <section style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
        borderTop: '1px solid rgba(255,255,255,0.07)',
        borderBottom: '1px solid rgba(255,255,255,0.07)',
        position: 'relative', zIndex: 1,
        background: 'rgba(255,255,255,0.02)',
      }}>
        {STATS.map((s, i) => (
          <div key={s.labelKey} className={`stat-card stat-card-${i}`} style={{
            padding: 'clamp(24px, 4vw, 40px) 24px',
            textAlign: 'center',
            borderRight: i < STATS.length - 1 ? '1px solid rgba(255,255,255,0.07)' : 'none',
          }}>
            <div style={{
              fontSize: 'clamp(2rem, 4.5vw, 2.8rem)', fontWeight: 900, lineHeight: 1,
              background: 'linear-gradient(135deg, #53e5ef 0%, #2d93cc 100%)',
              WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text',
              marginBottom: 10,
            }}>
              {s.display}
            </div>
            <div style={{
              fontSize: 12, color: 'rgba(180,195,220,0.65)', fontWeight: 600,
              textTransform: 'uppercase', letterSpacing: '0.1em',
            }}>
              {t(s.labelKey)}
            </div>
          </div>
        ))}
      </section>

      {/* Feature cards */}
      <section style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))',
        gap: 20,
        padding: 'clamp(40px, 6vw, 72px) clamp(20px, 5vw, 72px)',
        maxWidth: 1100, margin: '0 auto', width: '100%',
        boxSizing: 'border-box', position: 'relative', zIndex: 1,
      }}>
        {FEATURES.map((f, i) => (
          <div key={f.key} className={`feature-card feature-card-${i}`} style={{
            background: 'rgba(255,255,255,0.035)',
            border: '1px solid rgba(255,255,255,0.08)',
            borderRadius: 14, padding: '32px 28px',
          }}>
            <div style={{ fontSize: 36, marginBottom: 16 }}>{f.icon}</div>
            <p style={{ margin: 0, fontSize: 15, color: 'rgba(220,230,245,0.9)', fontWeight: 500, lineHeight: 1.6 }}>
              {t(f.key)}
            </p>
          </div>
        ))}
      </section>

      {/* Learn more */}
      <div style={{ textAlign: 'center', paddingBottom: 48, position: 'relative', zIndex: 1 }}>
        <Link to="/about" style={{
          color: 'rgba(83,229,239,0.8)', textDecoration: 'none',
          fontWeight: 600, fontSize: 15, letterSpacing: '0.02em',
        }}>
          {t('login.learnMore')} →
        </Link>
      </div>
    </div>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function LoginPage() {
  const [modal, setModal] = useState(null) // null | 'signin' | 'request'

  return (
    <div style={{ minHeight: '100vh', background: '#0b0e18', display: 'flex', flexDirection: 'column' }}>
      <SlimHeader />
      <LandingContent
        onSignIn={() => setModal('signin')}
        onRequest={() => setModal('request')}
      />
      <Footer />

      {modal === 'signin' && (
        <Modal onClose={() => setModal(null)}>
          <SignInForm onRequestAccess={() => setModal('request')} />
        </Modal>
      )}

      {modal === 'request' && (
        <Modal onClose={() => setModal(null)}>
          <RequestAccessForm onBack={() => setModal('signin')} />
        </Modal>
      )}
    </div>
  )
}
