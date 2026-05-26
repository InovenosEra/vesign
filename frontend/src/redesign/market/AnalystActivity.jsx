/* Analyst activity — one feed (rating actions + consensus target moves) split
 * into two meaningful columns: "Your stocks" (activity on tickers you hold) and
 * "Notable" (the rest, ranked by market cap → recognizable names, not random
 * small caps). Replaces the old AnalystChanges + Upgrades panels. Pure market
 * data — analyst targets/ratings only, no Vesign model. */
import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getAnalystActivity, getAnalystConsensus, getPortfolioHoldings } from '../../api'
import { useCurrency } from '../../context/CurrencyContext'
import { useTickerModal } from '../TickerModalContext'
import { LOGO } from '../fmt'

const TP = (k) => k === 'RAISE-TP' || k === 'LOWER-TP'
const kindClass = (k) =>
  k === 'UPGRADE' || k === 'RAISE-TP' ? 'upgrade'
    : k === 'DOWNGRADE' || k === 'LOWER-TP' ? 'downgrade'
      : k === 'CONSENSUS' ? 'consensus'
        : 'initiate'
const kindLabel = (k) => ({
  UPGRADE: 'Upgrade', DOWNGRADE: 'Downgrade', INITIATE: 'Initiate',
  'RAISE-TP': 'Raise TP', 'LOWER-TP': 'Lower TP', CONSENSUS: 'Consensus',
})[k] || k

function Row({ a, open, fmtPrice }) {
  return (
    <div className="an-row" data-ticker={a.ticker} onClick={() => open(a.ticker, a.company)}>
      <img className="logo-mini" src={LOGO(a.ticker)} alt={a.ticker} />
      <div className="info">
        <div className="tk">{a.ticker}</div>
        <div className="firm">{a.firm || a.company || ''}</div>
      </div>
      <span className={'action ' + kindClass(a.kind)}>{kindLabel(a.kind)}</span>
      <div className="grade">
        {a.kind === 'CONSENSUS' ? (
          <>
            <div className="tp-row">
              {fmtPrice(a.target, 0)}
              {a.upside_pct != null && (
                <span className={'tp-chg ' + (a.upside_pct >= 0 ? 'up' : 'down')}>
                  {a.upside_pct >= 0 ? '+' : ''}{a.upside_pct}%
                </span>
              )}
            </div>
            <div className="g-pt">consensus target</div>
          </>
        ) : TP(a.kind) ? (
          <div className="tp-row">
            {a.prev_target != null
              ? <><span className="old">{fmtPrice(a.prev_target, 0)}</span><span className="arr">→</span>{fmtPrice(a.target, 0)}</>
              : fmtPrice(a.target, 0)}
            {a.change_pct != null && (
              <span className={'tp-chg ' + (a.change_pct >= 0 ? 'up' : 'down')}>
                {a.change_pct >= 0 ? '+' : ''}{Math.round(a.change_pct)}%
              </span>
            )}
          </div>
        ) : (
          <>
            <div>
              {a.from_grade
                ? <><span className="old">{a.from_grade}</span><span className="arr">→</span>{a.to_grade}</>
                : a.to_grade}
            </div>
            {a.price_target != null && <div className="g-pt">PT {fmtPrice(a.price_target, 0)}</div>}
          </>
        )}
      </div>
    </div>
  )
}

export default function AnalystActivity() {
  const open = useTickerModal()
  const { fmtPrice } = useCurrency()
  const { data } = useQuery({ queryKey: ['market-analyst-activity'], queryFn: () => getAnalystActivity(50), refetchInterval: 300_000 })
  const { data: holdings } = useQuery({ queryKey: ['portfolio-holdings'], queryFn: () => getPortfolioHoldings('US') })

  const activity = data?.activity || []
  const heldTickers = useMemo(() => (holdings || []).map((h) => h.ticker), [holdings])
  const mySet = useMemo(() => new Set(heldTickers), [heldTickers])

  // Consensus snapshot for every holding — the fallback so a holding with no
  // recent move still shows its analyst target + upside.
  const { data: consensusData } = useQuery({
    queryKey: ['market-analyst-consensus', heldTickers],
    queryFn: () => getAnalystConsensus(heldTickers),
    enabled: heldTickers.length > 0,
  })
  const consensus = useMemo(() => {
    const m = {}
    for (const c of consensusData?.consensus || []) m[c.ticker] = c
    return m
  }, [consensusData])

  // "Your stocks": recent moves first, then consensus rows for the rest.
  const mine = useMemo(() => {
    const moves = activity.filter((a) => mySet.has(a.ticker))
    const moved = new Set(moves.map((a) => a.ticker))
    const rest = heldTickers
      .filter((t) => !moved.has(t) && consensus[t] && consensus[t].target != null)
      .map((t) => ({ kind: 'CONSENSUS', ...consensus[t] }))
      .sort((x, y) => (y.upside_pct ?? -999) - (x.upside_pct ?? -999))
    return [...moves, ...rest].slice(0, 8)
  }, [activity, mySet, heldTickers, consensus])

  const notable = useMemo(
    () => activity.filter((a) => !mySet.has(a.ticker))
      .sort((x, y) => (y.market_cap || 0) - (x.market_cap || 0)).slice(0, 7),
    [activity, mySet],
  )

  if (!activity.length) return null

  return (
    <div className="two-col-2 analyst-cols">
      <div className="news-panel">
        <div className="news-head"><h3>Your stocks</h3><span className="day">Analyst activity</span></div>
        <div className="cal-list">
          {mine.length
            ? mine.map((a, i) => <Row key={i} a={a} open={open} fmtPrice={fmtPrice} />)
            : <div className="an-empty">No analyst moves on your holdings recently.</div>}
        </div>
      </div>
      <div className="news-panel">
        <div className="news-head"><h3>Notable</h3><span className="day">By market cap</span></div>
        <div className="cal-list">
          {notable.map((a, i) => <Row key={i} a={a} open={open} fmtPrice={fmtPrice} />)}
        </div>
      </div>
    </div>
  )
}
