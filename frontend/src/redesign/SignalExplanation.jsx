/* Shared AI explanation block — used by the inline signal cards (SignalCard) and
 * the signal modal (SignalModalRd). Self-fetching by ticker; the parent only
 * mounts it for signals that have an explanation. Ticker-centric: no date → the
 * backend uses the ticker's latest signal and frames the rationale by its action
 * (BUY vs SELL). Compact layout: headline + ✓/⚠ two-column points + inline key
 * numbers. */
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
  if (isLoading) return <div className="sig-why"><div className="sig-why-note">Generating…</div></div>
  if (expl?.locked) return <div className="sig-why"><div className="sig-why-note">Upgrade to Pro or Max to see AI explanations.</div></div>
  if (isError || !expl) return <div className="sig-why"><div className="sig-why-note">Explanation unavailable — please try again.</div></div>
  return (
    <div className="sig-why">
      {expl.headline && <div className="sig-why-head">{expl.headline}</div>}
      {(expl.strengths?.length > 0 || expl.risks?.length > 0) && (
        <ul className="sig-why-pts">
          {(expl.strengths || []).map((x, i) => <li className="p" key={'s' + i}>{x}</li>)}
          {(expl.risks || []).map((x, i) => <li className="n" key={'r' + i}>{x}</li>)}
        </ul>
      )}
      {expl.key_numbers?.length > 0 && (
        <div className="sig-why-nums">
          {expl.key_numbers.map((k, i) => <span key={i}><b>{k.label}</b> {k.value}</span>)}
        </div>
      )}
    </div>
  )
}
