import { useQuery } from '@tanstack/react-query'
import { getLivePrices } from '../api'

/**
 * Polls /api/prices/live every 3 s for the given tickers.
 * Returns { prices: { TICKER: number|null }, phase: 'idle'|'pre'|'regular'|'post'|null }.
 * phase === null while still loading.
 */
export function useLivePrices(tickers) {
  const key = [...tickers].sort().join(',')

  const { data } = useQuery({
    queryKey: ['live-prices', key],
    queryFn: () => getLivePrices(tickers),
    enabled: tickers.length > 0,
    refetchInterval: 3_000,
    refetchIntervalInBackground: true,
    staleTime: 0,
  })

  return {
    prices: data?.prices ?? {},
    phase: data == null ? null : data.phase,
  }
}
