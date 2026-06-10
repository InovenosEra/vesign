/* Shared AI "Why this signal" block — used by the signal modal (SignalModalRd)
 * and the inline BUY cards (BuySignalCard). Self-fetching by ticker; the PARENT
 * is responsible for only mounting it for BUY signals (it does not re-check the
 * signal type). Ticker-centric: no date → the backend uses the latest signal. */
import { useQuery } from '@tanstack/react-query'
import { getSignalExplanation } from '../api'
import './signal-explanation.css'

export default function SignalExplanation({ ticker }) {
  const { data: expl, isLoading, isError } = useQuery({
    queryKey: ['signal-explanation', ticker],
    queryFn: () => getSignalExplanation(ticker),
    enabled: !!ticker,
    staleTime: 600_000,
  })
  return (
    <div className="sig-why">
      {isLoading && <div className="sig-why-note">Generating…</div>}
      {!isLoading && expl?.locked && <div className="sig-why-note">Upgrade to Pro or Max to see AI explanations.</div>}
      {!isLoading && isError && <div className="sig-why-note">Explanation unavailable — please try again.</div>}
      {expl && !expl.locked && !isError && (
        <>
          {expl.headline && <p className="sig-why-headline">{expl.headline}</p>}
          {expl.strengths?.length > 0 && (
            <ul className="sig-why-list str">{expl.strengths.map((s, i) => <li key={i}>{s}</li>)}</ul>
          )}
          {expl.risks?.length > 0 && (
            <ul className="sig-why-list rsk">{expl.risks.map((s, i) => <li key={i}>{s}</li>)}</ul>
          )}
          {expl.key_numbers?.length > 0 && (
            <div className="sig-why-nums">
              {expl.key_numbers.map((k, i) => (
                <div className="sig-why-num" key={i}><span className="l">{k.label}</span><span className="v">{k.value}</span></div>
              ))}
            </div>
          )}
          <p className="sig-why-disc">AI summary of model data, not new analysis.</p>
        </>
      )}
    </div>
  )
}
