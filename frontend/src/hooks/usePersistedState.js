import { useState, useEffect } from 'react'

// Module-level store. Survives route changes but resets on full page refresh —
// which is exactly the UX we want for filter persistence.
const _store = {}

export function usePersistedState(key, initial) {
  const [state, setState] = useState(() => (key in _store ? _store[key] : initial))
  useEffect(() => { _store[key] = state }, [key, state])
  return [state, setState]
}
