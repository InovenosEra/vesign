import { Link, useNavigate } from 'react-router-dom'
import { useAuth } from '@clerk/react'
import { useTranslation } from 'react-i18next'
import { Footer, PublicHeader } from '../App'

const FEATURES = [
  { icon: '📊', titleKey: 'about.feature1Title', descKey: 'about.feature1Desc' },
  { icon: '📈', titleKey: 'about.feature3Title', descKey: 'about.feature3Desc' },
  { icon: '💼', titleKey: 'about.feature2Title', descKey: 'about.feature2Desc' },
]

function AboutContent({ standalone }) {
  const { t } = useTranslation()
  const { userId } = useAuth()
  const navigate = useNavigate()

  return (
    <div style={{ maxWidth: 860, margin: '0 auto', padding: standalone ? '64px 24px 80px' : '4px 24px 60px' }}>
      {/* Hero */}
      <div style={{ textAlign: 'center', marginBottom: 56 }}>
        <h1 style={{
          display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4,
          fontWeight: 900, fontSize: 'clamp(2.2rem, 5vw, 3.8rem)',
          letterSpacing: '0.06em', fontFamily: "'Segoe UI', system-ui, sans-serif",
          margin: '0 0 18px', lineHeight: 1, direction: 'ltr',
        }}>
          <img
            src="/favicon.png" alt="V"
            style={{ height: 'clamp(2.2rem, 5vw, 3.8rem)', objectFit: 'contain', filter: 'drop-shadow(0 2px 8px rgba(0,210,255,0.6))' }}
          />
          <span className="title-shimmer" style={{ letterSpacing: '0.06em' }}>esign</span>
        </h1>
        <p style={{
          fontSize: 'clamp(1rem, 2.5vw, 1.35rem)', fontWeight: 700,
          letterSpacing: '0.08em', textTransform: 'uppercase', margin: '0 0 28px',
          background: 'linear-gradient(180deg, #53e5ef 0%, #2d93cc 55%, #2262a8 100%)',
          WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent',
          backgroundClip: 'text',
        }}>
          Investing Signals
        </p>
        <p style={{
          maxWidth: 620, margin: '0 auto', fontSize: 16,
          lineHeight: 1.75, color: 'var(--muted, #aaa)',
        }}>
          {t('about.intro')}
        </p>
      </div>

      {/* Feature cards */}
      <div style={{
        display: 'flex', gap: 20, flexWrap: 'wrap',
        justifyContent: 'center', marginBottom: 56,
      }}>
        {FEATURES.map(f => (
          <div key={f.titleKey} style={{
            flex: '1 1 220px', maxWidth: 260,
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 12, padding: '28px 24px',
          }}>
            <div style={{ fontSize: 28, marginBottom: 14 }}>{f.icon}</div>
            <h3 style={{ fontSize: 15, fontWeight: 700, margin: '0 0 10px', color: 'var(--text)' }}>
              {t(f.titleKey)}
            </h3>
            <p style={{ fontSize: 13, lineHeight: 1.65, color: 'var(--muted, #aaa)', margin: 0 }}>
              {t(f.descKey)}
            </p>
          </div>
        ))}
      </div>

      {/* How we analyze */}
      <div style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 12, padding: '32px 36px', marginBottom: 28, textAlign: 'center',
      }}>
        <h3 style={{ fontSize: 17, fontWeight: 700, margin: '0 0 14px', color: 'var(--text)' }}>
          {t('about.howTitle')}
        </h3>
        <p style={{ fontSize: 14, lineHeight: 1.75, color: 'var(--muted, #aaa)', margin: 0, maxWidth: 600, marginInline: 'auto' }}>
          {t('about.howDesc')}
        </p>
      </div>

      {/* Disclaimer */}
      <div style={{
        padding: '16px 24px', marginBottom: 48,
        background: 'rgba(255,200,0,0.05)', border: '1px solid rgba(255,200,0,0.18)',
        borderRadius: 10, textAlign: 'center',
      }}>
        <p style={{ fontSize: 14, lineHeight: 1.7, color: 'var(--text)', fontWeight: 600, margin: 0 }}>
          ⚠ {t('about.disclaimer')}
        </p>
      </div>

      {/* CTA */}
      <div style={{ textAlign: 'center' }}>
        {userId
          ? (
            <button
              onClick={() => navigate('/')}
              style={{
                padding: '12px 36px', background: 'var(--accent)', color: '#000',
                border: 'none', borderRadius: 8, fontWeight: 700, fontSize: 15, cursor: 'pointer',
              }}
            >
              {t('about.goToDashboard')}
            </button>
          ) : (
            <Link
              to="/sign-in"
              style={{
                display: 'inline-block', padding: '12px 36px',
                background: 'transparent', border: '1px solid var(--accent)',
                color: 'var(--accent)', borderRadius: 8, fontWeight: 700,
                fontSize: 15, textDecoration: 'none',
              }}
            >
              {t('about.signIn')}
            </Link>
          )
        }
      </div>
    </div>
  )
}

export default function AboutPage({ standalone = false }) {
  const { t } = useTranslation()

  if (standalone) {
    return (
      <div style={{ minHeight: '100vh', background: 'var(--bg)', color: 'var(--text)' }}>
        <PublicHeader />
        <AboutContent standalone />
        <Footer />
      </div>
    )
  }

  return <AboutContent standalone={false} />
}
