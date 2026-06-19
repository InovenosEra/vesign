import { createContext, useContext } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getMe } from '../api'

// `ready` is false until /api/me resolves. Plan-gated UI must wait for it,
// otherwise it briefly renders the default (free) view before the real plan
// arrives — a free→pro flash on every reload.
const DEFAULTS = { plan: 'free', balance_cents: 0, per_row_price_cents: 10, see_all_price_cents: 50, ready: false }
const MeContext = createContext(DEFAULTS)

export function MeProvider({ children }) {
  const { data, isLoading } = useQuery({ queryKey: ['me'], queryFn: getMe, staleTime: 30_000 })
  // `ready` = the query has SETTLED (success or error). On error we fall back to
  // DEFAULTS but still mark ready, so a failing /api/me degrades to the free view
  // instead of hanging the page forever on the loading skeleton.
  const value = data ? { ...data, ready: true } : { ...DEFAULTS, ready: !isLoading }
  return <MeContext.Provider value={value}>{children}</MeContext.Provider>
}

export const useMe = () => useContext(MeContext)
