/* Signals page-head: context strip (date · NYSE) + Active / Closed-trades tabs.
 * Ported from the trades-v5.html .page-head block. Tab state is lifted into
 * SignalsPage. (Counts now live on the in-pane section headers, not the tabs.) */
export default function PageHead({ tab, setTab }) {
  const day = new Date().toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })

  return (
    <div className="page-head">
      <div className="ph-tabs">
        <a
          className={'ph-tab' + (tab === 'today' ? ' active' : '')}
          href="/signals/active-trades"
          onClick={(e) => { e.preventDefault(); setTab('today') }}
        >Active trades</a>
        <a
          className={'ph-tab' + (tab === 'closed' ? ' active' : '')}
          href="/signals/closed-trades"
          onClick={(e) => { e.preventDefault(); setTab('closed') }}
        >Closed trades</a>
        <span className="day ph-inline-day">{day}</span>
      </div>
    </div>
  )
}
