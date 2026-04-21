import { useNavigate } from 'react-router-dom'
import { useAuth } from '@clerk/react'
import { useTranslation } from 'react-i18next'
import { Footer, PublicHeader } from '../App'

const FEATURES = [
  {
    Icon: ({ color }) => (
      <svg width="26" height="26" viewBox="0 0 24 24" fill="none">
        <rect x="2" y="12" width="5" height="10" rx="1.5" fill={color} fillOpacity="0.5"/>
        <rect x="9.5" y="7" width="5" height="15" rx="1.5" fill={color}/>
        <rect x="17" y="3" width="5" height="19" rx="1.5" fill={color} fillOpacity="0.75"/>
      </svg>
    ),
    titleKey: 'about.feature1Title', descKey: 'about.feature1Desc', rgb: '83,229,239',
  },
  {
    Icon: ({ color }) => (
      <svg width="26" height="26" viewBox="0 0 24 24" fill="none">
        <path d="M3 17L9 11L13 15L21 7" stroke={color} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
        <path d="M16 7H21V12" stroke={color} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
      </svg>
    ),
    titleKey: 'about.feature3Title', descKey: 'about.feature3Desc', rgb: '45,147,204',
  },
  {
    Icon: ({ color }) => (
      <svg width="26" height="26" viewBox="0 0 24 24" fill="none">
        <circle cx="12" cy="12" r="9" stroke={color} strokeWidth="2" strokeOpacity="0.4"/>
        <path d="M12 3C16.97 3 21 7.03 21 12H12V3Z" fill={color} fillOpacity="0.8"/>
        <path d="M12 3C16.97 3 21 7.03 21 12H12V3Z" stroke={color} strokeWidth="1.5"/>
      </svg>
    ),
    titleKey: 'about.feature2Title', descKey: 'about.feature2Desc', rgb: '79,142,247',
  },
]

const DIMENSIONS = [
  { num: '01', key: 'about.dimension1' },
  { num: '02', key: 'about.dimension2' },
  { num: '03', key: 'about.dimension3' },
  { num: '04', key: 'about.dimension4' },
]

const STATS = [
  { display: '1,500+', labelKey: 'landing.statStocks' },
  { display: '900+',   labelKey: 'landing.statSignals' },
  { display: '~80%',   labelKey: 'landing.statWinRate' },
  { display: '~25%',   labelKey: 'landing.statAvgYield' },
]

function AboutContent({ standalone }) {
  const { t } = useTranslation()
  const { userId } = useAuth()
  const navigate = useNavigate()

  return (
    <div style={{ width: '100%' }}>

      {/* Hero */}
      <section style={{
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', textAlign: 'center',
        padding: standalone
          ? 'clamp(60px, 10vh, 100px) 24px clamp(48px, 8vh, 80px)'
          : 'clamp(32px, 6vh, 64px) 24px clamp(32px, 5vh, 56px)',
        position: 'relative', zIndex: 1,
      }}>
        <h1 style={{
          fontSize: 'clamp(2rem, 5vw, 3.8rem)', fontWeight: 900,
          margin: '0 0 28px', letterSpacing: '-0.03em', lineHeight: 1.08, paddingBottom: '0.12em',
          background: 'linear-gradient(135deg, #ffffff 0%, #53e5ef 45%, #2d93cc 100%)',
          WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text',
          maxWidth: 760,
        }}>
          {t('about.heroHeadline')}
        </h1>
        <p style={{
          maxWidth: 600, margin: 0, fontSize: 'clamp(14px, 1.8vw, 17px)',
          lineHeight: 1.75, color: 'rgba(180,195,220,0.65)',
        }}>
          {t('about.intro')}
        </p>
      </section>

      {/* Stats bar */}
      <section style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
        borderTop: '1px solid rgba(255,255,255,0.07)',
        borderBottom: '1px solid rgba(255,255,255,0.07)',
        position: 'relative', zIndex: 1,
      }}>
        {STATS.map((s, i) => (
          <div key={s.labelKey} style={{
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
        padding: 'clamp(48px, 7vw, 80px) clamp(20px, 5vw, 72px)',
        maxWidth: 1100, margin: '0 auto', width: '100%',
        boxSizing: 'border-box', position: 'relative', zIndex: 1,
      }}>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))',
          gap: 24,
        }}>
          {FEATURES.map((f, i) => (
            <div key={f.titleKey} className={`feature-card feature-card-${i}`} style={{
              background: 'rgba(12,16,30,0.75)',
              border: '1px solid rgba(255,255,255,0.07)',
              borderRadius: 18, padding: '36px 30px',
              display: 'flex', flexDirection: 'column', gap: 16,
            }}>
              <div style={{
                width: 56, height: 56, borderRadius: 14,
                background: `rgba(${f.rgb},0.1)`,
                border: `1px solid rgba(${f.rgb},0.28)`,
                boxShadow: `0 0 20px rgba(${f.rgb},0.18)`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
              }}>
                <f.Icon color={`rgb(${f.rgb})`} />
              </div>
              <h3 style={{ margin: 0, fontSize: 17, fontWeight: 800, color: '#e8eaf0', lineHeight: 1.2 }}>
                {t(f.titleKey)}
              </h3>
              <p style={{ margin: 0, fontSize: 14, color: 'rgba(180,195,220,0.65)', lineHeight: 1.7 }}>
                {t(f.descKey)}
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* How We Analyze */}
      <section style={{
        padding: '0 clamp(20px, 5vw, 72px) clamp(48px, 7vw, 80px)',
        maxWidth: 1100, margin: '0 auto', width: '100%',
        boxSizing: 'border-box', position: 'relative', zIndex: 1,
      }}>
        <div style={{
          background: 'rgba(12,16,30,0.75)',
          border: '1px solid rgba(255,255,255,0.07)',
          borderRadius: 18, padding: '40px 44px',
        }}>
          <h2 style={{
            fontSize: 'clamp(1.2rem, 2.5vw, 1.6rem)', fontWeight: 800,
            margin: '0 0 12px', color: '#e8eaf0', textAlign: 'center',
          }}>
            {t('about.howTitle')}
          </h2>
          <p style={{
            fontSize: 14, lineHeight: 1.75, color: 'rgba(180,195,220,0.65)',
            margin: '0 0 36px', textAlign: 'center', maxWidth: 580, marginInline: 'auto',
          }}>
            {t('about.howDesc')}
          </p>
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
            gap: 16,
          }}>
            {DIMENSIONS.map(d => (
              <div key={d.key} style={{
                background: 'rgba(45,147,204,0.06)',
                border: '1px solid rgba(83,229,239,0.15)',
                borderRadius: 12, padding: '20px 18px',
                display: 'flex', flexDirection: 'column', gap: 8,
              }}>
                <span style={{
                  fontSize: 11, fontWeight: 700, letterSpacing: '0.12em',
                  color: 'rgba(83,229,239,0.5)', textTransform: 'uppercase',
                }}>
                  {d.num}
                </span>
                <span style={{ fontSize: 14, fontWeight: 700, color: '#c8d8f0', lineHeight: 1.3 }}>
                  {t(d.key)}
                </span>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Disclaimer */}
      <section style={{
        padding: '0 clamp(20px, 5vw, 72px) clamp(32px, 5vw, 56px)',
        maxWidth: 1100, margin: '0 auto', width: '100%',
        boxSizing: 'border-box', position: 'relative', zIndex: 1,
      }}>
        <div style={{
          padding: '16px 24px',
          background: 'rgba(255,200,0,0.05)', border: '1px solid rgba(255,200,0,0.18)',
          borderRadius: 12,
        }}>
          <p style={{ fontSize: 13, lineHeight: 1.7, color: 'rgba(220,195,130,0.8)', margin: 0 }}>
            ⚠ {t('about.disclaimer')}
          </p>
        </div>
      </section>

      {/* CTA */}
      <section style={{
        textAlign: 'center',
        padding: '0 24px clamp(56px, 8vw, 88px)',
        position: 'relative', zIndex: 1,
      }}>
        {userId
          ? (
            <button onClick={() => navigate('/')} className="landing-btn-primary">
              {t('about.goToDashboard')}
            </button>
          ) : (
            <button onClick={() => navigate('/sign-in')} className="landing-btn-primary">
              {t('about.signIn')}
            </button>
          )
        }
      </section>
    </div>
  )
}

export default function AboutPage({ standalone = false }) {
  if (standalone) {
    return (
      <div style={{ minHeight: '100vh', background: 'transparent', color: 'var(--text)', paddingTop: 60 }}>
        <PublicHeader />
        <AboutContent standalone />
        <Footer />
      </div>
    )
  }

  return <AboutContent standalone={false} />
}
