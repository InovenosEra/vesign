import { useEffect, useState } from 'react'

// GET /api/stats (unauthenticated, real numbers) — three explicit states so
// callers (Proof's skeleton/error/count-up render, How-it-works' universe-
// size line) never have to guess whether `data` is null because it's still
// loading or because the fetch actually failed.
export function useStats() {
  const [state, setState] = useState({ status: 'loading', data: null })
  useEffect(() => {
    let alive = true
    fetch('/api/stats')
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((d) => { if (alive) setState({ status: 'ready', data: d }) })
      .catch(() => { if (alive) setState({ status: 'error', data: null }) })
    return () => { alive = false }
  }, [])
  return state
}
