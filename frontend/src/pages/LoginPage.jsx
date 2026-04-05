import { useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { useClerk } from '@clerk/react'
import { useTranslation } from 'react-i18next'
import { requestAccess } from '../api'
import { Footer, LanguageSwitcher } from '../App'

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

const cardStyle = {
  background: 'var(--surface)',
  border: '1px solid var(--border)',
  borderRadius: 12,
  padding: '48px 56px',
  width: 420,
  flexShrink: 0,
  boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
}

const FEATURES = [
  { icon: '📊', key: 'login.feature1' },
  { icon: '📈', key: 'login.feature2' },
  { icon: '💼', key: 'login.feature3' },
]

function Logo() {
  return (
    <div style={{ textAlign: 'center', marginBottom: 32 }}>
      <h1 style={{
        display: 'flex', alignItems: 'flex-end', justifyContent: 'center',
        gap: 2, fontWeight: 900, fontSize: '2.4rem', letterSpacing: '0.08em',
        fontFamily: "'Segoe UI', system-ui, sans-serif", margin: 0,
      }}>
        <img src="/favicon.png" alt="V" style={{ height: '2.8rem', objectFit: 'contain', flexShrink: 0, filter: 'drop-shadow(0 2px 4px rgba(0,210,255,0.6))' }} />
        <span className="title-shimmer" style={{ letterSpacing: '0.08em' }}>esign</span>
      </h1>
    </div>
  )
}

function ErrorBox({ msg }) {
  if (!msg) return null
  return (
    <div style={{ color: '#ff6b6b', fontSize: 13, padding: '8px 12px', background: 'rgba(255,107,107,0.1)', borderRadius: 6, border: '1px solid rgba(255,107,107,0.3)' }}>
      {msg}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sign-in form
// ---------------------------------------------------------------------------
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
    <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 15, color: 'var(--text-muted, #999)' }}>{t('login.email')}</label>
        <input type="email" value={email} onChange={e => setEmail(e.target.value)} required autoFocus style={inputStyle} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 15, color: 'var(--text-muted, #999)' }}>{t('login.password')}</label>
        <input type="password" value={password} onChange={e => setPassword(e.target.value)} required style={inputStyle} />
      </div>
      <ErrorBox msg={error} />
      <button type="submit" disabled={loading} style={{
        marginTop: 8, padding: '13px', background: 'var(--accent, #00d2ff)',
        color: '#000', border: 'none', borderRadius: 6, fontWeight: 700, fontSize: 16,
        cursor: loading ? 'not-allowed' : 'pointer', opacity: loading ? 0.7 : 1,
      }}>
        {loading ? t('login.signingIn') : t('login.signIn')}
      </button>
      <button type="button" onClick={onRequestAccess} style={{
        background: 'transparent', border: 'none', color: 'var(--text-muted, #999)',
        fontSize: 15, cursor: 'pointer', textDecoration: 'underline', padding: 0,
      }}>
        {t('login.requestAccess')}
      </button>
    </form>
  )
}

// ---------------------------------------------------------------------------
// Agreement step
// ---------------------------------------------------------------------------
const AGREEMENT_SECTION_KEYS = [
  { titleKey: 'agreement.s1title', bodyKey: 'agreement.s1body' },
  { titleKey: 'agreement.s2title', bodyKey: 'agreement.s2body' },
  { titleKey: 'agreement.s3title', bodyKey: 'agreement.s3body' },
  { titleKey: 'agreement.s4title', bodyKey: 'agreement.s4body' },
  { titleKey: 'agreement.s5title', bodyKey: 'agreement.s5body' },
]

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

      {/* Scrollable agreement text */}
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

      {/* Signature */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 14, color: 'var(--muted, #999)' }}>{t('agreement.nameLabel')}</label>
        <input
          type="text"
          value={name}
          onChange={e => setName(e.target.value)}
          placeholder={t('agreement.namePlaceholder')}
          autoFocus
          style={{ ...inputStyle, fontSize: 15 }}
        />
      </div>

      {/* Checkbox — terms */}
      <label style={{ display: 'flex', alignItems: 'flex-start', gap: 10, cursor: 'pointer', fontSize: 13, color: 'var(--text)' }}>
        <input
          type="checkbox"
          checked={checked}
          onChange={e => setChecked(e.target.checked)}
          style={{ marginTop: 2, accentColor: 'var(--accent)', width: 16, height: 16, flexShrink: 0 }}
        />
        {t('agreement.checkbox')}
      </label>

      {/* Checkbox — communications */}
      <label style={{ display: 'flex', alignItems: 'flex-start', gap: 10, cursor: 'pointer', fontSize: 13, color: 'var(--text)' }}>
        <input
          type="checkbox"
          checked={checkedComms}
          onChange={e => setCheckedComms(e.target.checked)}
          style={{ marginTop: 2, accentColor: 'var(--accent)', width: 16, height: 16, flexShrink: 0 }}
        />
        {t('agreement.checkboxComms')}
      </label>

      <button
        type="button"
        disabled={!canProceed}
        onClick={() => onAgree(name.trim())}
        style={{
          padding: '13px', background: canProceed ? 'var(--accent, #00d2ff)' : 'var(--border)',
          color: canProceed ? '#000' : 'var(--muted, #666)',
          border: 'none', borderRadius: 6, fontWeight: 700, fontSize: 16,
          cursor: canProceed ? 'pointer' : 'not-allowed', transition: 'background 0.2s',
        }}
      >
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

// ---------------------------------------------------------------------------
// Request access form
// ---------------------------------------------------------------------------
function RequestAccessForm({ onBack }) {
  const { t } = useTranslation()
  const [step, setStep] = useState('agreement') // 'agreement' | 'form'
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

  if (step === 'agreement') {
    return <AgreementStep onAgree={handleAgree} onBack={onBack} />
  }

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
        <div style={{ fontSize: 32 }}>✓</div>
        <p style={{ margin: 0, color: 'var(--text)', fontSize: 15 }}>{t('login.requestSent')}</p>
        <p style={{ margin: 0, color: 'var(--text-muted, #999)', fontSize: 13 }}>
          {t('login.requestSentDesc')}
        </p>
        <button type="button" onClick={onBack} style={{
          marginTop: 8, padding: '11px', background: 'transparent',
          border: '1px solid var(--border)', borderRadius: 6,
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
        marginTop: 8, padding: '11px', background: 'var(--accent, #00d2ff)',
        color: '#000', border: 'none', borderRadius: 6, fontWeight: 700, fontSize: 14,
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

// ---------------------------------------------------------------------------
// Left branding panel
// ---------------------------------------------------------------------------
function BrandingPanel() {
  const { t } = useTranslation()

  return (
    <div style={{
      flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center',
      padding: '60px 56px', maxWidth: 520,
    }}>
      {/* Sub-brand */}
      <p style={{
        fontSize: 16, fontWeight: 700, letterSpacing: '0.15em',
        textTransform: 'uppercase', margin: '0 0 28px',
        background: 'linear-gradient(180deg, #53e5ef 0%, #2d93cc 55%, #2262a8 100%)',
        WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text',
      }}>
        {t('nav.about')}
      </p>

      {/* Pitch */}
      <p style={{
        fontSize: 17, lineHeight: 1.75, color: 'var(--muted, #aaa)',
        margin: '0 0 36px', maxWidth: 400,
      }}>
        {t('login.pitch')}
      </p>

      {/* Feature list */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 14, marginBottom: 40 }}>
        {FEATURES.map(f => (
          <div key={f.key} style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span style={{ fontSize: 24 }}>{f.icon}</span>
            <span style={{ fontSize: 16, color: 'var(--text)', fontWeight: 500 }}>{t(f.key)}</span>
          </div>
        ))}
      </div>

      {/* Learn more */}
      <Link to="/about" style={{
        fontSize: 15, color: 'var(--accent)', textDecoration: 'none', fontWeight: 600,
      }}>
        {t('login.learnMore')} →
      </Link>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------
export default function LoginPage() {
  const [view, setView] = useState('signin') // 'signin' | 'request'

  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg)', display: 'flex', flexDirection: 'column' }}>
      {/* Slim header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '12px 24px', borderBottom: '1px solid var(--border)',
        background: 'var(--surface)',
      }}>
        <h1 style={{
          display: 'flex', alignItems: 'center', gap: 2,
          fontWeight: 900, fontSize: '2rem', letterSpacing: '0.08em',
          fontFamily: "'Segoe UI', system-ui, sans-serif", margin: 0, direction: 'ltr',
        }}>
          <img src="/favicon.png" alt="V" style={{ height: '2.4rem', objectFit: 'contain', filter: 'drop-shadow(0 2px 4px rgba(0,210,255,0.6))' }} />
          <span className="title-shimmer" style={{ letterSpacing: '0.08em' }}>esign</span>
        </h1>
        <LanguageSwitcher />
      </div>

      <div style={{
        flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
        gap: 32, padding: '40px 24px', flexWrap: 'wrap',
      }}>

        {/* Left: branding info — hidden on small screens via inline style trick */}
        <div className="login-branding-panel">
          <BrandingPanel />
        </div>

        {/* Divider */}
        <div className="login-divider" />

        {/* Right: login card */}
        <div style={cardStyle}>
          {view === 'signin'
            ? <SignInForm onRequestAccess={() => setView('request')} />
            : <RequestAccessForm onBack={() => setView('signin')} />
          }
        </div>
      </div>
      <Footer />
    </div>
  )
}
