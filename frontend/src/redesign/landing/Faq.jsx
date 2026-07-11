import { useTranslation } from 'react-i18next'
import { FAQ_IDS } from './faqData'
import { useSectionView } from './hooks'
import { track } from './analytics'

export function Faq() {
  const { t } = useTranslation()
  const sectionRef = useSectionView('faq')

  return (
    <section className="ld-section narrow" id="faq" ref={sectionRef}>
      <div className="ld-section-inner">
        <div className="ld-section-head">
          <p className="tag">{t('ld.faq.tag')}</p>
          <h2>{t('ld.faq.title')}</h2>
        </div>
        <div className="ld-faq">
          {FAQ_IDS.map((id) => (
            <details
              className="ld-faq-item"
              key={id}
              onToggle={(e) => { if (e.target.open) track('landing_faq_open', { question_id: id }) }}
            >
              <summary>
                {t(`ld.faq.items.${id}.q`)}
                {/* Plain "+" that rotates 45deg into an "x" on open (CSS-driven,
                 * see .ld-faq-item[open] .ld-faq-caret) — a symmetric glyph, so
                 * unlike the old arrow caret it needs no RTL mirroring. */}
                <span className="ld-faq-caret" aria-hidden="true">
                  <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round">
                    <path d="M10 4v12M4 10h12" />
                  </svg>
                </span>
              </summary>
              <p>{t(`ld.faq.items.${id}.a`)}</p>
            </details>
          ))}
        </div>
      </div>
    </section>
  )
}
