import { createContext } from 'react'

// TASE market hidden for now — force US everywhere. Revisit later to re-enable.
export const MarketContext = createContext({ market: 'US', setMarket: () => {} })

export function MarketProvider({ children }) {
  const value = { market: 'US', setMarket: () => {} }
  // Clear any stale 'IL' in localStorage from earlier sessions
  try { localStorage.removeItem('market') } catch {}
  return (
    <MarketContext.Provider value={value}>
      {children}
    </MarketContext.Provider>
  )
}
