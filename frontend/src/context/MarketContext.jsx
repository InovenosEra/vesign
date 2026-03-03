import { createContext, useState, useEffect } from 'react'

export const MarketContext = createContext({ market: 'US', setMarket: () => {} })

export function MarketProvider({ children }) {
  const [market, setMarket] = useState(() => localStorage.getItem('market') || 'US')

  useEffect(() => {
    localStorage.setItem('market', market)
  }, [market])

  return (
    <MarketContext.Provider value={{ market, setMarket }}>
      {children}
    </MarketContext.Provider>
  )
}
