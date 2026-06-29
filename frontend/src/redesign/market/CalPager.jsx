/* Page navigation footer for the calendar panels. Shows "x–y of n" plus
 * prev/next controls. Hidden when everything fits on one page. */
export default function CalPager({ page, pageSize, total, onPage }) {
  const pages = Math.max(1, Math.ceil(total / pageSize))
  if (pages <= 1) return null
  const from = total === 0 ? 0 : page * pageSize + 1
  const to = Math.min(total, (page + 1) * pageSize)
  return (
    <div className="cal-pager">
      <span className="range">{from}–{to} of {total}</span>
      <div className="pg-btns">
        <button className="pg-btn" disabled={page === 0} onClick={() => onPage(page - 1)} aria-label="Previous page">‹</button>
        <span className="pg-num">{page + 1} / {pages}</span>
        <button className="pg-btn" disabled={page >= pages - 1} onClick={() => onPage(page + 1)} aria-label="Next page">›</button>
      </div>
    </div>
  )
}
