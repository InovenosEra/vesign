/* Paginated data table — reproduces the trades-v5.html `<table class="data-table">`
 * plus its `<div class="pagination">` control. 10 rows/page, « ‹ Prev p/n Next › »
 * (ported from the mockup's paginate() helper). */
import { useState } from 'react'

export const PAGE_SIZE = 10

export default function PagedTable({ head, rows, row, emptyLabel = 'No rows.', colspan = 1 }) {
  const [page, setPage] = useState(1)
  const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE))
  const cur = Math.min(page, pages)
  const slice = rows.slice((cur - 1) * PAGE_SIZE, cur * PAGE_SIZE)

  return (
    <>
      <table className="data-table">
        <thead>{head}</thead>
        <tbody>
          {rows.length
            ? slice.map((r, i) => row(r, i))
            : <tr><td colSpan={colspan} className="muted" style={{ textAlign: 'center', padding: 24 }}>{emptyLabel}</td></tr>}
        </tbody>
      </table>
      <div className="pagination">
        {pages > 1 && (
          <>
            <button data-go="first" disabled={cur === 1} onClick={() => setPage(1)}>«</button>
            <button data-go="prev" disabled={cur === 1} onClick={() => setPage(Math.max(1, cur - 1))}>‹ Prev</button>
            <span>{cur} / {pages}</span>
            <button data-go="next" disabled={cur === pages} onClick={() => setPage(Math.min(pages, cur + 1))}>Next ›</button>
            <button data-go="last" disabled={cur === pages} onClick={() => setPage(pages)}>»</button>
          </>
        )}
      </div>
    </>
  )
}
