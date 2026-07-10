/* VeSign public landing page — standalone marketing page for logged-out
 * visitors. Renders OUTSIDE AppShell (own nav + footer), inside a `.rd ld`
 * wrapper so the scoped redesign tokens apply. This file is a thin
 * composition root; all section content/behavior lives in ./landing/.
 * Real data (win rate / trade count / etc.) comes from the public
 * /api/stats endpoint — see ./landing/useStats.js. */
import { useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { LandingNav } from './landing/Nav'
import { Hero } from './landing/Hero'
import { HowItWorks } from './landing/HowItWorks'
import { Proof } from './landing/Proof'
import { Statement } from './landing/Statement'
import { Platform } from './landing/Platform'
import { Pricing } from './landing/Pricing'
import { Faq } from './landing/Faq'
import { FinalCta } from './landing/FinalCta'
import { LandingFooter } from './landing/Footer'
import { useStats } from './landing/useStats'
import { useSeo } from './landing/useSeo'
import { FAQ_IDS, generateFaqJsonLd } from './landing/faqData'
import { track } from './landing/analytics'
import './redesign.css'
import './signals/signals.css'
import './portfolio/portfolio.css'
import './landing.css'
import './landing/landing.css'

export { LandingNav } from './landing/Nav'
export { LandingFooter } from './landing/Footer'

const SITE_URL = 'https://ve-sign.com'

function JsonLd() {
  const { t } = useTranslation()
  const organization = {
    '@context': 'https://schema.org',
    '@type': 'Organization',
    name: 'VeSign',
    url: SITE_URL,
    logo: `${SITE_URL}/favicon.png`,
  }
  const website = {
    '@context': 'https://schema.org',
    '@type': 'WebSite',
    name: 'VeSign',
    url: SITE_URL,
  }
  const faqs = FAQ_IDS.map((id) => ({ q: t(`ld.faq.items.${id}.q`), a: t(`ld.faq.items.${id}.a`) }))
  const faqPage = generateFaqJsonLd(faqs)
  return (
    <>
      <script type="application/ld+json">{JSON.stringify(organization)}</script>
      <script type="application/ld+json">{JSON.stringify(website)}</script>
      <script type="application/ld+json">{JSON.stringify(faqPage)}</script>
    </>
  )
}

export default function LandingPage() {
  const { t } = useTranslation()
  useSeo({ title: t('ld.seo.title'), description: t('ld.seo.description') })

  // Match the document canvas to the redesign --bg while mounted (same
  // pattern as AppShell) so overscroll never flashes the legacy background,
  // and enable smooth scrolling for in-page anchor links only while this
  // page is mounted (scoped via JS, not a global stylesheet rule).
  useEffect(() => {
    const html = document.documentElement
    const prevBg = html.style.background
    const prevScrollBehavior = html.style.scrollBehavior
    html.style.background = '#0a0e15'
    html.style.scrollBehavior = 'smooth'

    // The browser can restore a stale non-zero scroll position across
    // reloads — force a clean start every time the landing page mounts.
    const prevRestoration = window.history.scrollRestoration
    window.history.scrollRestoration = 'manual'
    window.scrollTo(0, 0)

    track('landing_view', {})

    return () => {
      html.style.background = prevBg
      html.style.scrollBehavior = prevScrollBehavior
      window.history.scrollRestoration = prevRestoration
    }
  }, [])

  const stats = useStats()

  return (
    <div className="rd ld">
      <JsonLd />
      <LandingNav />
      <main id="main">
        <Hero />
        <HowItWorks stats={stats} />
        <Proof stats={stats} />
        <Statement />
        <Platform />
        <Pricing />
        <Faq />
        <FinalCta />
      </main>
      <LandingFooter />
    </div>
  )
}
