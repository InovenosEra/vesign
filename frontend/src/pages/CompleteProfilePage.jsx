import { useState } from 'react'
import { useUser } from '@clerk/react'

export default function CompleteProfilePage() {
  const { user } = useUser()
  const [firstName, setFirstName] = useState('')
  const [lastName, setLastName] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await user.update({ firstName: firstName.trim(), lastName: lastName.trim() })
      // AppLayout will re-render automatically after the user object updates
    } catch (err) {
      setError('Failed to save. Please try again.')
    } finally {
      setLoading(false)
    }
  }

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

  return (
    <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--bg)' }}>
      <div style={{
        background: 'var(--surface)',
        border: '1px solid var(--border)',
        borderRadius: 12,
        padding: '40px 48px',
        width: 360,
        boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
      }}>
        <div style={{ textAlign: 'center', marginBottom: 28 }}>
          <h1 style={{
            display: 'flex', alignItems: 'flex-end', justifyContent: 'center',
            gap: 2, fontWeight: 900, fontSize: '2.4rem', letterSpacing: '0.08em',
            fontFamily: "'Segoe UI', system-ui, sans-serif", margin: '0 0 16px',
          }}>
            <img src="/favicon.png" alt="V" style={{ height: '2.8rem', objectFit: 'contain', flexShrink: 0, filter: 'drop-shadow(0 2px 4px rgba(0,210,255,0.6))' }} />
            <span className="title-shimmer" style={{ letterSpacing: '0.08em' }}>esign</span>
          </h1>
          <p style={{ margin: 0, color: 'var(--text-muted, #999)', fontSize: 14 }}>
            Please complete your profile to continue.
          </p>
        </div>

        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <label style={{ fontSize: 13, color: 'var(--text-muted, #999)' }}>First Name</label>
            <input
              type="text"
              value={firstName}
              onChange={e => setFirstName(e.target.value)}
              required
              autoFocus
              style={inputStyle}
            />
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <label style={{ fontSize: 13, color: 'var(--text-muted, #999)' }}>Last Name</label>
            <input
              type="text"
              value={lastName}
              onChange={e => setLastName(e.target.value)}
              required
              style={inputStyle}
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
            style={{
              marginTop: 8, padding: '11px',
              background: 'var(--accent, #00d2ff)', color: '#000',
              border: 'none', borderRadius: 6, fontWeight: 700, fontSize: 14,
              cursor: loading ? 'not-allowed' : 'pointer', opacity: loading ? 0.7 : 1,
            }}
          >
            {loading ? 'Saving…' : 'Continue'}
          </button>
        </form>
      </div>
    </div>
  )
}
