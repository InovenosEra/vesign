import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSignalsToday, getSignals, runPipeline, getPipelineStatus } from '../api'
import { useLivePrices } from '../hooks/useLivePrices'

function SignalBadge({ signal }) {
  return <span className={`badge badge-${signal}`}>{signal}</span>
}

function PredictionCell({ value }) {
  if (value == null) return <td>—</td>
  const pct = (value * 100).toFixed(2)
  return <td className={value > 0 ? 'up' : 'down'}>{value > 0 ? '▲' : '▼'} {Math.abs(pct)}%</td>
}

function LivePriceCell({ ticker, closePrice, prices, marketOpen }) {
  if (!marketOpen) return <td style={{ color: 'var(--muted)', fontSize: 12 }}>mkt closed</td>

  const live = prices[ticker]
  if (live == null) return <td style={{ color: 'var(--muted)' }}>—</td>

  const diff = live - closePrice
  const pct  = closePrice ? (diff / closePrice) * 100 : 0
  const cls  = diff >= 0 ? 'up' : 'down'
  const arrow = diff >= 0 ? '▲' : '▼'

  return (
    <td>
      <div>{live.toFixed(2)}</div>
      <div className={cls} style={{ fontSize: 11 }}>
        {arrow} {Math.abs(diff).toFixed(2)} ({Math.abs(pct).toFixed(2)}%)
      </div>
    </td>
  )
}

function SignalsTable({ rows, showSignal = true, prices = {}, marketOpen = false, showLive = false }) {
  if (!rows || rows.length === 0) {
    return <p className="empty">No signals found.</p>
  }

  return (
    <div className="data-table-wrap">
      <table>
        <thead>
          <tr>
            <th></th>
            <th>Ticker</th>
            <th>Company</th>
            <th>Date</th>
            {showSignal && <th>Signal</th>}
            <th>Price</th>
            {showLive && <th>Live Price</th>}
            <th>RSI</th>
            <th>Prediction</th>
            <th>Base Price</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              <td>
                {r.logo_url
                  ? <img className="logo" src={r.logo_url} alt="" />
                  : null}
              </td>
              <td><strong>{r.ticker}</strong></td>
              <td>{r.company ?? '—'}</td>
              <td>{r.date ? r.date.slice(0, 10) : '—'}</td>
              {showSignal && <td><SignalBadge signal={r.signal} /></td>}
              <td>{r.close != null ? r.close.toFixed(2) : '—'}</td>
              {showLive && (
                <LivePriceCell
                  ticker={r.ticker}
                  closePrice={r.close}
                  prices={prices}
                  marketOpen={marketOpen}
                />
              )}
              <td>{r.rsi != null ? r.rsi.toFixed(1) : '—'}</td>
              <PredictionCell value={r.fair_value_upside} />
              <td>{r.target_mean_price != null ? r.target_mean_price.toFixed(2) : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function PipelineBar() {
  const [triggered, setTriggered] = useState(false)

  const { data: status, refetch } = useQuery({
    queryKey: ['pipeline-status'],
    queryFn: getPipelineStatus,
    refetchInterval: triggered ? 3000 : false,
  })

  async function handleRun() {
    try {
      await runPipeline()
      setTriggered(true)
      refetch()
    } catch (e) {
      alert(e.message)
    }
  }

  const running = status?.status === 'running'

  return (
    <div className="pipeline-bar">
      <button className="primary" onClick={handleRun} disabled={running}>
        {running ? 'Running…' : '↺ Run Pipeline'}
      </button>
      {status && status.status !== 'idle' && (
        <div className="pipeline-log">
          <strong style={{ color: status.status === 'error' ? 'var(--red)' : 'var(--muted)' }}>
            {status.status.toUpperCase()}
          </strong>
          {status.log ? '\n' + status.log : ''}
        </div>
      )}
    </div>
  )
}

export default function SignalsPage() {
  const [signalFilter, setSignalFilter] = useState('ALL')
  const [search, setSearch] = useState('')

  const { data: todayBuy,  isLoading: loadingBuy  } = useQuery({
    queryKey: ['signals-today', 'BUY'],
    queryFn: () => getSignalsToday('BUY'),
    refetchInterval: 120_000,
  })

  const { data: todaySell, isLoading: loadingSell } = useQuery({
    queryKey: ['signals-today', 'SELL'],
    queryFn: () => getSignalsToday('SELL'),
    refetchInterval: 120_000,
  })

  const { data: allSignals, isLoading: loadingAll } = useQuery({
    queryKey: ['signals', signalFilter, search],
    queryFn: () => getSignals({
      signal: signalFilter === 'ALL' ? undefined : signalFilter,
      search: search || undefined,
    }),
  })

  // Collect unique tickers from today's sections for live price polling
  const todayTickers = useMemo(() => {
    const set = new Set()
    todayBuy?.forEach(r => r.ticker && set.add(r.ticker))
    todaySell?.forEach(r => r.ticker && set.add(r.ticker))
    return [...set]
  }, [todayBuy, todaySell])

  const { prices, marketOpen } = useLivePrices(todayTickers)

  return (
    <div>
      <PipelineBar />

      <div className="section">
        <p className="section-title">
          Today's BUY Signals ({loadingBuy ? '…' : (todayBuy?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingBuy
          ? <p className="loading">Loading…</p>
          : <SignalsTable rows={todayBuy} showSignal={false} prices={prices} marketOpen={marketOpen} showLive />}
      </div>

      <div className="section">
        <p className="section-title">
          Today's SELL Signals ({loadingSell ? '…' : (todaySell?.length ?? 0)})
          {marketOpen && <span style={{ color: 'var(--green)', fontSize: 12, marginLeft: 10 }}>● live</span>}
        </p>
        {loadingSell
          ? <p className="loading">Loading…</p>
          : <SignalsTable rows={todaySell} showSignal={false} prices={prices} marketOpen={marketOpen} showLive />}
      </div>

      <div className="section">
        <p className="section-title">All Signals (last 12 months)</p>
        <div className="controls">
          <input
            placeholder="🔍 Search ticker or company"
            value={search}
            onChange={e => setSearch(e.target.value)}
            style={{ width: 240 }}
          />
          {['ALL', 'BUY', 'HOLD', 'SELL'].map(s => (
            <button
              key={s}
              onClick={() => setSignalFilter(s)}
              className={signalFilter === s ? 'primary' : ''}
            >
              {s}
            </button>
          ))}
          {search && <button onClick={() => setSearch('')}>Clear</button>}
        </div>
        {loadingAll
          ? <p className="loading">Loading…</p>
          : <SignalsTable rows={allSignals} />}
      </div>
    </div>
  )
}
