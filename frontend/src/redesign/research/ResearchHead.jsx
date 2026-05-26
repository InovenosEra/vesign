/* Page-head context strip + Screener/Deep-dive tabs.
 * Mirrors the mockup's .page-head; tab clicks switch the active pane (state in
 * ResearchPage). The screener count badge fills in once signals load. */
export default function ResearchHead({ tab, setTab, count }) {
  const day = new Date().toLocaleDateString(undefined, {
    weekday: 'long', month: 'long', day: 'numeric', year: 'numeric',
  })
  return (
    <div className="page-head">
      <div className="ph-tabs">
        <a className={'ph-tab' + (tab === 'screener' ? ' active' : '')}
          href="#screener" onClick={(e) => { e.preventDefault(); setTab('screener') }}>
          Screener {count != null && <span className="count">{count}</span>}
        </a>
        <a className={'ph-tab' + (tab === 'deep-dive' ? ' active' : '')}
          href="#deep-dive" onClick={(e) => { e.preventDefault(); setTab('deep-dive') }}>
          Deep dive
        </a>
        <span className="day ph-inline-day">{day}</span>
      </div>
    </div>
  )
}
