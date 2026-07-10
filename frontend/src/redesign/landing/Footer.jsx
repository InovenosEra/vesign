import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'
import { LogoMark } from './Nav'

export function LandingFooter() {
  const { t } = useTranslation()
  return (
    <footer className="ld-footer">
      <div className="ld-footer-top">
        <div className="ld-footer-brand">
          <LogoMark id="footer" className="ld-logo-mark sm" />
          <span className="ld-logo-text">VeSign</span>
        </div>
        <nav className="ld-footer-links" aria-label={t('ld.footer.linksLabel')}>
          <a href="#how">{t('ld.nav.how')}</a>
          <Link to="/performance">{t('ld.footer.performance')}</Link>
          <a href="#pricing">{t('ld.nav.pricing')}</a>
          <a href="#faq">{t('ld.nav.faq')}</a>
          <Link to="/contact">{t('ld.footer.contact')}</Link>
          <Link to="/sign-in">{t('ld.nav.logIn')}</Link>
        </nav>
      </div>
      <div className="ld-footer-legal">
        <p className="ld-disclaimer">{t('ld.footer.disclaimer')}</p>
        <span className="ld-copy">{t('ld.footer.copyright', { year: new Date().getFullYear() })}</span>
      </div>
    </footer>
  )
}
