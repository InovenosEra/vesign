import { useTranslation, Trans } from 'react-i18next'
import { Link } from 'react-router-dom'
import { track } from './analytics'
import { useReveal, useSectionView } from './hooks'

export function FinalCta() {
  const { t } = useTranslation()
  const [revealRef, visible] = useReveal({ threshold: 0.3 })
  const gaRef = useSectionView('finalCta')
  return (
    <section
      className={'ld-final' + (visible ? ' visible' : '')}
      ref={(el) => { revealRef.current = el; gaRef.current = el }}
    >
      <h2><Trans i18nKey="ld.cta.title" components={{ br: <br /> }} /></h2>
      <p>{t('ld.cta.sub')}</p>
      <Link
        to="/sign-up"
        className="ld-btn primary lg"
        onClick={() => track('landing_cta_click', { location: 'final_cta', label: 'sign_up' })}
      >
        {t('ld.cta.button')}
      </Link>
    </section>
  )
}
