/* Shared "appear together" load gate for redesign pages.
 *
 * useReady(enabled, defs): runs every query a view needs in parallel (same
 * queryKeys as the child components, so React Query dedupes — no double fetch —
 * and children read from cache instantly once revealed). Returns true once no
 * query is on its FIRST load. Errored queries count as settled, so a failing
 * endpoint can never freeze a page in skeleton.
 *
 * PageSkeleton: a shimmer placeholder shown until a view is ready, so sections
 * paint together instead of popping in one by one. */
import { Fragment } from 'react'
import { useQueries } from '@tanstack/react-query'

export function useReady(enabled, defs) {
  const qs = useQueries({
    queries: defs.map(([queryKey, queryFn]) => ({ queryKey, queryFn, enabled })),
  })
  return !qs.some(q => q.isLoading)
}

const DEFAULT_ROWS = [
  { n: 4, h: 92 },   // a strip of small cards (kpi / metrics / filters)
  { n: 2, h: 240 },  // big panels (charts / split columns)
  { n: 1, h: 320 },  // a table block
]

export function PageSkeleton({ rows = DEFAULT_ROWS }) {
  return (
    <div className="ov-skel" aria-busy="true">
      {rows.map((r, i) => (
        <Fragment key={i}>
          <div className="rd-skel ov-skel-h" />
          <div className="ov-skel-grid">
            {Array.from({ length: r.n }).map((_, j) => (
              <div key={j} className="rd-skel" style={{ height: r.h }} />
            ))}
          </div>
        </Fragment>
      ))}
    </div>
  )
}
