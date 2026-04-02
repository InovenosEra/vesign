import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useClerk } from '@clerk/react'
import { useTranslation } from 'react-i18next'
import { requestAccess } from '../api'

const inputStyle = {
  padding: '10px 12px',
  background: 'var(--bg)',
  border: '1px solid var(--border)',
  borderRadius: 6,
  color: 'var(--text)',
  fontSize: 14,
  outline: 'none',
  width: '100%',
  boxSizing: 'border-box',
}

const cardStyle = {
  background: 'var(--surface)',
  border: '1px solid var(--border)',
  borderRadius: 12,
  padding: '40px 48px',
  width: 360,
  boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
}

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
        <label style={{ fontSize: 13, color: 'var(--text-muted, #999)' }}>{t('login.email')}</label>
        <input type="email" value={email} onChange={e => setEmail(e.target.value)} required autoFocus style={inputStyle} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 13, color: 'var(--text-muted, #999)' }}>{t('login.password')}</label>
        <input type="password" value={password} onChange={e => setPassword(e.target.value)} required style={inputStyle} />
      </div>
      <ErrorBox msg={error} />
      <button type="submit" disabled={loading} style={{
        marginTop: 8, padding: '11px', background: 'var(--accent, #00d2ff)',
        color: '#000', border: 'none', borderRadius: 6, fontWeight: 700, fontSize: 14,
        cursor: loading ? 'not-allowed' : 'pointer', opacity: loading ? 0.7 : 1,
      }}>
        {loading ? t('login.signingIn') : t('login.signIn')}
      </button>
      <button type="button" onClick={onRequestAccess} style={{
        background: 'transparent', border: 'none', color: 'var(--text-muted, #999)',
        fontSize: 13, cursor: 'pointer', textDecoration: 'underline', padding: 0,
      }}>
        {t('login.requestAccess')}
      </button>
    </form>
  )
}

// ---------------------------------------------------------------------------
// Request access form
// ---------------------------------------------------------------------------
function RequestAccessForm({ onBack }) {
  const { t } = useTranslation()
  const [email, setEmail] = useState('')
  const [message, setMessage] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [done, setDone] = useState(false)

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await requestAccess(email.trim(), message.trim())
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
// Page
// ---------------------------------------------------------------------------
export default function LoginPage() {
  const [view, setView] = useState('signin') // 'signin' | 'request'

  return (
    <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--bg)' }}>
      <div style={cardStyle}>
        <Logo />
        {view === 'signin'
          ? <SignInForm onRequestAccess={() => setView('request')} />
          : <RequestAccessForm onBack={() => setView('signin')} />
        }
      </div>
    </div>
  )
}
