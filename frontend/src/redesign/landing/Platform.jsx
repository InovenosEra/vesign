import { useTranslation } from 'react-i18next'
import { useSectionView } from './hooks'

/* ── Platform: the REAL Signals/Market/Portfolio component CSS (imported
 * from signals.css / portfolio.css) inside device frames — these are the
 * actual product components, not mockups drawn to look like them. The
 * ticker data inside is fixed/illustrative (not a live fetch); explicitly
 * labeled as such so it can never be mistaken for a performance claim
 * alongside Proof's real numbers a few sections up. */
export function Platform() {
  const { t } = useTranslation()
  const sectionRef = useSectionView('platform')

  return (
    <section className="ld-section" id="platform" ref={sectionRef}>
      <div className="ld-section-inner">
        <div className="ld-section-head">
          <p className="tag">{t('ld.platform.tag')}</p>
          <h2>{t('ld.platform.title')}</h2>
          <p>
            {t('ld.platform.sub')}{' '}
            <span className="ld-placeholder-tag">{t('ld.platform.illustrativeTag')}</span>
          </p>
        </div>
        <div className="ld-shots">
          <div className="ld-shots-grid">
            <figure className="ld-device tall">
              <div className="ld-device-bar"><i /><i /><i /><span className="u" dir="ltr">ve-sign.com/signals</span></div>
              <div className="ld-device-body">
                <div className="sigcard buy">
                  <div className="sc-head">
                    <img className="sc-logo" src="/logos/NVDA.png" alt="" />
                    <div className="sc-id"><div className="trow"><span className="tk" dir="ltr">NVDA</span></div><div className="co">NVIDIA Corp</div></div>
                    <div className="sig-why"><div className="sig-why-head">{t('ld.platform.sampleReasonNvda')}</div></div>
                  </div>
                  <div className="sc-cockpit">
                    <div className="cell"><div className="l">{t('ld.platform.currentPrice')}</div><div className="v num" dir="ltr">$194.83</div><div className="sub2 num up" dir="ltr">+2.41%</div></div>
                    <div className="cell"><div className="l">{t('ld.platform.priceTarget')}</div><div className="v num" dir="ltr">$229.90</div><div className="sub2 num up" dir="ltr">+18.0%</div></div>
                    <div className="cell"><div className="l">{t('ld.platform.fiveDayMl')}</div><div className="v num" dir="ltr">$206.90</div><div className="sub2 num up" dir="ltr">+6.2%</div></div>
                    <div className="cell"><div className="l">{t('ld.platform.health')}</div><span className="health"><span className="d" /><span className="d" /><span className="d" /><span className="d" /><span className="d off" /></span></div>
                  </div>
                </div>
                <div className="sigcard buy">
                  <div className="sc-head">
                    <img className="sc-logo" src="/logos/AVGO.png" alt="" />
                    <div className="sc-id"><div className="trow"><span className="tk" dir="ltr">AVGO</span></div><div className="co">Broadcom Inc</div></div>
                    <div className="sig-why"><div className="sig-why-head">{t('ld.platform.sampleReasonAvgo')}</div></div>
                  </div>
                  <div className="sc-cockpit">
                    <div className="cell"><div className="l">{t('ld.platform.currentPrice')}</div><div className="v num" dir="ltr">$360.45</div><div className="sub2 num up" dir="ltr">+1.87%</div></div>
                    <div className="cell"><div className="l">{t('ld.platform.priceTarget')}</div><div className="v num" dir="ltr">$400.10</div><div className="sub2 num up" dir="ltr">+11.0%</div></div>
                    <div className="cell"><div className="l">{t('ld.platform.fiveDayMl')}</div><div className="v num" dir="ltr">$374.15</div><div className="sub2 num up" dir="ltr">+3.8%</div></div>
                    <div className="cell"><div className="l">{t('ld.platform.health')}</div><span className="health"><span className="d" /><span className="d" /><span className="d" /><span className="d" /><span className="d" /></span></div>
                  </div>
                </div>
              </div>
              <figcaption><b>{t('ld.platform.captionSignalsTitle')}</b>{t('ld.platform.captionSignalsBody')}</figcaption>
            </figure>

            <div className="ld-shots-side">
              <figure className="ld-device">
                <div className="ld-device-bar"><i /><i /><i /><span className="u" dir="ltr">ve-sign.com/market</span></div>
                <div className="ld-device-body">
                  <div className="ld-indices-mini">
                    <div className="idx-card"><div className="name">S&amp;P 500</div><div className="price" dir="ltr">7,483.20</div><div className="change up"><span className="pct" dir="ltr">+0.34%</span></div></div>
                    <div className="idx-card"><div className="name">Nasdaq 100</div><div className="price" dir="ltr">29,329</div><div className="change down"><span className="pct" dir="ltr">-1.61%</span></div></div>
                    <div className="idx-card"><div className="name">VIX</div><div className="price" dir="ltr">16.15</div><div className="change down"><span className="pct" dir="ltr">-2.62%</span></div></div>
                  </div>
                  <div className="mover-panel">
                    <div className="mover-head"><h3>{t('ld.platform.mostActive')}</h3></div>
                    <div className="mover-list">
                      <div className="mover-row"><img className="logo-mini" src="/logos/NVDA.png" alt="" /><div className="mname"><div className="tk" dir="ltr">NVDA</div><div className="co">NVIDIA Corp</div></div><div className="px" dir="ltr">194.83</div><div className="ch up" dir="ltr">+2.41%</div></div>
                      <div className="mover-row"><img className="logo-mini" src="/logos/AAPL.png" alt="" /><div className="mname"><div className="tk" dir="ltr">AAPL</div><div className="co">Apple Inc</div></div><div className="px" dir="ltr">308.63</div><div className="ch up" dir="ltr">+4.84%</div></div>
                    </div>
                  </div>
                </div>
                <figcaption><b>{t('ld.platform.captionMarketTitle')}</b>{t('ld.platform.captionMarketBody')}</figcaption>
              </figure>

              <figure className="ld-device">
                <div className="ld-device-bar"><i /><i /><i /><span className="u" dir="ltr">ve-sign.com/portfolio</span></div>
                <div className="ld-device-body">
                  <div className="nw-hero ld-nw-mini">
                    <div className="nw-main">
                      <div className="nw-label">{t('ld.platform.currentValue')}</div>
                      <div className="nw-value" dir="ltr"><span className="s">$</span>142,318.60</div>
                      <div className="nw-sub"><span className="nw-today up" dir="ltr">▲ $612.40 (+0.43%) {t('ld.platform.today')}</span></div>
                    </div>
                    <div className="nw-chips">
                      <div className="nw-chip"><div className="lbl">{t('ld.platform.invested')}</div><div className="val" dir="ltr">$103,378</div></div>
                      <div className="nw-chip"><div className="lbl">{t('ld.platform.vsVesign')}</div><div className="val down" dir="ltr">-3.8%</div></div>
                    </div>
                  </div>
                </div>
                <figcaption><b>{t('ld.platform.captionPortfolioTitle')}</b>{t('ld.platform.captionPortfolioBody')}</figcaption>
              </figure>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
