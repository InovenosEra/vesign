/* Market page-head: context strip + tab bar (Overview / Market Trend / News /
 * Calendar). Same structure/height as the other pages' page-heads. */
const TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'news', label: 'News' },
  { id: 'calendar', label: 'Calendar' },
]

export default function PageHead({ tab, setTab }) {
  const day = new Date().toLocaleDateString(undefined, { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })
  return (
    <div className="page-head">
      <div className="ph-top">
        <div className="ph-context">
          <span className="day">{day}</span>
        </div>
      </div>
      <div className="ph-tabs">
        {TABS.map(t => (
          <a key={t.id} href="#" className={'ph-tab' + (tab === t.id ? ' active' : '')}
            onClick={(e) => { e.preventDefault(); setTab(t.id) }}>{t.label}</a>
        ))}
      </div>
    </div>
  )
}
