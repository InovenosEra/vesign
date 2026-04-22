import { createContext, useContext, useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getFxRates } from '../api'

const SYMBOL = { USD: '$', EUR: '€', ILS: '₪' }

export const CurrencyContext = createContext(null)

export function CurrencyProvider({ children }) {
  const [currency, setCurrency] = useState(() => {
    try {
      const v = localStorage.getItem('currency')
      return v === 'EUR' || v === 'ILS' ? v : 'USD'
    } catch { return 'USD' }
  })

  useEffect(() => {
    try { localStorage.setItem('currency', currency) } catch {}
  }, [currency])

  const { data: rates = { USD: 1, EUR: 1, ILS: 1 } } = useQuery({
    queryKey: ['fx-rates'],
    queryFn: getFxRates,
    staleTime: 60 * 60_000,
    gcTime: 24 * 60 * 60_000,
  })

  const rate = Number(rates?.[currency] ?? 1) || 1
  const symbol = SYMBOL[currency] || '$'

  const fmtPrice = (usd, decimals = 2) => {
    if (usd == null || Number.isNaN(Number(usd))) return '—'
    const v = Number(usd) * rate
    return symbol + v.toLocaleString('en-US', {
      minimumFractionDigits: decimals, maximumFractionDigits: decimals,
    })
  }

  const fmtAmount = (usd, decimals = 2) => {
    if (usd == null || Number.isNaN(Number(usd))) return '—'
    const v = Number(usd) * rate
    const sign = v >= 0 ? '' : '-'
    return sign + symbol + Math.abs(v).toLocaleString('en-US', {
      minimumFractionDigits: decimals, maximumFractionDigits: decimals,
    })
  }

  const value = { currency, setCurrency, rate, symbol, rates, fmtPrice, fmtAmount }
  return <CurrencyContext.Provider value={value}>{children}</CurrencyContext.Provider>
}

export function useCurrency() {
  const ctx = useContext(CurrencyContext)
  if (!ctx) throw new Error('useCurrency must be inside CurrencyProvider')
  return ctx
}
