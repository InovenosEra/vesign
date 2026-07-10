import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'
import { TIERS } from '../tiers'
import { useSectionView } from './hooks'
import { track } from './analytics'

const CTA_LOCATION = { free: 'pricing_free', pro: 'pricing_pro', max: 'pricing_max' }

export function Pricing() {
  const { t } = useTranslation()
  const sectionRef = useSectionView('pricing', 'landing_pricing_view')

  return (
    <section className="ld-section" id="pricing" ref={sectionRef}>
      <div className="ld-section-inner">
        <div className="ld-section-head">
          <p className="tag">{t('ld.pricing.tag')}</p>
          <h2>{t('ld.pricing.title')}</h2>
          <p><span className="ld-placeholder-tag">{t('ld.pricing.placeholderTag')}</span></p>
        </div>
        <div className="ld-pricing">
          {TIERS.map((p) => (
            <div className={'ld-plan' + (p.featured ? ' featured' : '')} key={p.id}>
              {p.featured && <div className="ld-plan-flag">{t('ld.pricing.mostPopular')}</div>}
              <div className="ld-plan-name">{t(`ld.pricing.tiers.${p.id}.name`)}</div>
              <div className="ld-plan-price" dir="ltr">{p.price}<span>{t(`ld.pricing.periods.${p.period === 'forever' ? 'forever' : 'perMonth'}`)}</span></div>
              <div className="ld-plan-blurb">{t(`ld.pricing.tiers.${p.id}.blurb`)}</div>
              <ul>
                {p.features.map((_, i) => <li key={i}>{t(`ld.pricing.tiers.${p.id}.features.${i}`)}</li>)}
              </ul>
              <Link
                to="/sign-up"
                className={'ld-btn ' + (p.featured ? 'primary' : 'ghost')}
                onClick={() => track('landing_cta_click', { location: CTA_LOCATION[p.id], label: p.id })}
              >
                {t(`ld.pricing.tiers.${p.id}.cta`)}
              </Link>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
