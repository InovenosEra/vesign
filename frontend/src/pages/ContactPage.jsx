import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '@clerk/react'
import { useUser } from '@clerk/react'
import { useTranslation } from 'react-i18next'
import { submitContact } from '../api'
import { Footer, PublicHeader } from '../App'

const inputStyle = {
  padding: '10px 12px',
  background: 'var(--bg)',
  border: '1px solid var(--border)',
  borderRadius: 6,
  color: 'var(--text)',
  fontSize: 15,
  outline: 'none',
  width: '100%',
  boxSizing: 'border-box',
}

function ContactForm() {
  const { t } = useTranslation()
  const { user } = useUser()
  const [name, setName] = useState(
    user ? `${user.firstName || ''} ${user.lastName || ''}`.trim() : ''
  )
  const [email, setEmail] = useState(
    user?.primaryEmailAddress?.emailAddress || ''
  )
  const [subject, setSubject] = useState('')
  const [message, setMessage] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [done, setDone] = useState(false)

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await submitContact(name.trim(), email.trim(), subject.trim(), message.trim())
      setDone(true)
    } catch (err) {
      setError(err.message || t('contact.error'))
    } finally {
      setLoading(false)
    }
  }

  if (done) {
    return (
      <div style={{ textAlign: 'center', padding: '40px 0' }}>
        <div style={{ fontSize: 40, marginBottom: 16 }}>✓</div>
        <p style={{ fontSize: 18, fontWeight: 700, color: 'var(--text)', margin: '0 0 10px' }}>
          {t('contact.sentTitle')}
        </p>
        <p style={{ fontSize: 14, color: 'var(--muted, #aaa)', margin: 0 }}>
          {t('contact.sentDesc')}
        </p>
      </div>
    )
  }

  return (
    <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
        <div style={{ flex: '1 1 200px', display: 'flex', flexDirection: 'column', gap: 6 }}>
          <label style={{ fontSize: 13, color: 'var(--muted, #999)' }}>{t('contact.name')}</label>
          <input
            type="text"
            value={name}
            onChange={e => setName(e.target.value)}
            placeholder={t('contact.namePlaceholder')}
            style={inputStyle}
          />
        </div>
        <div style={{ flex: '1 1 200px', display: 'flex', flexDirection: 'column', gap: 6 }}>
          <label style={{ fontSize: 13, color: 'var(--muted, #999)' }}>{t('contact.email')} *</label>
          <input
            type="email"
            value={email}
            onChange={e => setEmail(e.target.value)}
            required
            placeholder={t('contact.emailPlaceholder')}
            style={inputStyle}
          />
        </div>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 13, color: 'var(--muted, #999)' }}>
          {t('contact.subject')} <span style={{ opacity: 0.5 }}>({t('contact.optional')})</span>
        </label>
        <input
          type="text"
          value={subject}
          onChange={e => setSubject(e.target.value)}
          placeholder={t('contact.subjectPlaceholder')}
          style={inputStyle}
        />
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <label style={{ fontSize: 13, color: 'var(--muted, #999)' }}>{t('contact.message')} *</label>
        <textarea
          value={message}
          onChange={e => setMessage(e.target.value)}
          required
          rows={5}
          placeholder={t('contact.messagePlaceholder')}
          style={{ ...inputStyle, resize: 'vertical', fontFamily: 'inherit' }}
        />
      </div>

      {error && (
        <div style={{ color: '#ff6b6b', fontSize: 13, padding: '8px 12px', background: 'rgba(255,107,107,0.1)', borderRadius: 6, border: '1px solid rgba(255,107,107,0.3)' }}>
          {error}
        </div>
      )}

      <button
        type="submit"
        disabled={loading}
        className="landing-btn-primary"
        style={{ width: '100%', opacity: loading ? 0.7 : 1, cursor: loading ? 'not-allowed' : 'pointer' }}
      >
        {loading ? t('contact.sending') : t('contact.send')}
      </button>
    </form>
  )
}

function ContactContent({ standalone }) {
  const { t } = useTranslation()

  return (
    <div style={{ maxWidth: 680, margin: '0 auto', padding: standalone ? '60px 24px 80px' : '20px 24px 60px' }}>
      {/* Header */}
      <div style={{ marginBottom: 36 }}>
        <p style={{
          fontSize: 12, fontWeight: 700, letterSpacing: '0.2em',
          color: 'var(--accent)', textTransform: 'uppercase', margin: '0 0 10px',
        }}>
          Vesign
        </p>
        <h2 style={{ fontSize: 'clamp(1.6rem, 3vw, 2.2rem)', fontWeight: 900, margin: '0 0 12px', color: 'var(--text)' }}>
          {t('contact.title')}
        </h2>
        <p style={{ fontSize: 15, lineHeight: 1.7, color: 'var(--muted, #aaa)', margin: 0 }}>
          {t('contact.subtitle')}
        </p>
      </div>

      {/* Form card */}
      <div style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 12, padding: '32px 36px',
      }}>
        <ContactForm />
      </div>
    </div>
  )
}

export default function ContactPage({ standalone = false }) {
  const { t } = useTranslation()

  if (standalone) {
    return (
      <div style={{ minHeight: '100vh', background: 'transparent', color: 'var(--text)', display: 'flex', flexDirection: 'column', paddingTop: 60 }}>
        <PublicHeader />
        <div style={{ flex: 1 }}>
          <ContactContent standalone />
        </div>
        <Footer />
      </div>
    )
  }

  return <ContactContent standalone={false} />
}
