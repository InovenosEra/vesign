import { createContext, useContext } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getMe } from '../api'

const DEFAULTS = { plan: 'free', balance_cents: 0, per_row_price_cents: 10, see_all_price_cents: 50 }
const MeContext = createContext(DEFAULTS)

export function MeProvider({ children }) {
  const { data } = useQuery({ queryKey: ['me'], queryFn: getMe, staleTime: 30_000 })
  return <MeContext.Provider value={data || DEFAULTS}>{children}</MeContext.Provider>
}

export const useMe = () => useContext(MeContext)
