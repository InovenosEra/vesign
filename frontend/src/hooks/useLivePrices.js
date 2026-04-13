import { useQuery } from '@tanstack/react-query'
import { getLivePrices } from '../api'

/**
 * Polls /api/prices/live every 60 s for the given tickers.
 * Returns { prices: { TICKER: number|null }, marketOpen: bool }.
 * Does nothing if tickers is empty.
 */
export function useLivePrices(tickers) {
  const key = [...tickers].sort().join(',')

  const { data } = useQuery({
    queryKey: ['live-prices', key],
    queryFn: () => getLivePrices(tickers),
    enabled: tickers.length > 0,
    refetchInterval: 5_000,
    staleTime: 4_000,
  })

  return {
    prices: data?.prices ?? {},
    // null = still loading (no data yet); false = confirmed closed; true = confirmed open
    marketOpen: data == null ? null : data.market_open,
  }
}
