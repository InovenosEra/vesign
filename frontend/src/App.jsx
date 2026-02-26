import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query'
import { getMarketStatus } from './api'
import SignalsPage from './pages/SignalsPage'
import WatchlistPage from './pages/WatchlistPage'
import TradesPage from './pages/TradesPage'
import './App.css'

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30_000 } },
})

function MarketStatus() {
  const { data } = useQuery({
    queryKey: ['market-status'],
    queryFn: getMarketStatus,
    refetchInterval: 60_000,
  })
  if (!data) return null
  return data.is_open
    ? <span className="market-open">● Market Open</span>
    : <span className="market-closed">● Market Closed</span>
}

function Header() {
  return (
    <header className="app-header">
      <h1>Vesign Trading System</h1>
      <nav>
        <NavLink to="/">Signals</NavLink>
        <NavLink to="/watchlist">Watchlist</NavLink>
        <NavLink to="/trades">Trades</NavLink>
      </nav>
      <MarketStatus />
    </header>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Header />
        <main className="app-main">
          <Routes>
            <Route path="/" element={<SignalsPage />} />
            <Route path="/watchlist" element={<WatchlistPage />} />
            <Route path="/trades" element={<TradesPage />} />
          </Routes>
        </main>
      </BrowserRouter>
    </QueryClientProvider>
  )
}
