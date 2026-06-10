/* Shared AI explanation block — used by the inline signal cards (SignalCard) and
 * the signal modal (SignalModalRd). Self-fetching by ticker; the parent only
 * mounts it for signals that have an explanation. Ticker-centric: no date → the
 * backend uses the ticker's latest signal and frames the rationale by its action
 * (BUY vs SELL).
 *
 * `part` controls which slice renders (same query → one fetch, cached):
 *   - 'full'     (default, modal): headline + ✓/⚠ columns + key numbers
 *   - 'headline' (card top): just the one-line "why"
 *   - 'body'     (card expanded): ✓/⚠ columns + key numbers (no headline) */
import { useQuery } from '@tanstack/react-query'
import { getSignalExplanation } from '../api'
import './signal-explanation.css'

export default function SignalExplanation({ ticker, part = 'full' }) {
  const { data: expl, isLoading, isError } = useQuery({
    queryKey: ['signal-explanation', ticker],
    queryFn: () => getSignalExplanation(ticker),
    enabled: !!ticker,
    staleTime: 600_000,
  })
  const note = isLoading ? 'Generating…'
    : expl?.locked ? 'Upgrade to Pro or Max to see AI explanations.'
    : (isError || !expl) ? 'Explanation unavailable — please try again.'
    : null
  if (note) return <div className="sig-why"><div className="sig-why-note">{note}</div></div>

  const headline = expl.headline && <div className="sig-why-head">{expl.headline}</div>
  const body = (
    <>
      {(expl.strengths?.length > 0 || expl.risks?.length > 0) && (
        <div className="sig-why-cols">
          <ul className="sig-why-pts pos">
            {(expl.strengths || []).map((x, i) => <li key={'s' + i}>{x}</li>)}
          </ul>
          <ul className="sig-why-pts neg">
            {(expl.risks || []).map((x, i) => <li key={'r' + i}>{x}</li>)}
          </ul>
        </div>
      )}
      {expl.key_numbers?.length > 0 && (
        <div className="sig-why-nums">
          {expl.key_numbers.map((k, i) => <span key={i}><b>{k.label}</b> {k.value}</span>)}
        </div>
      )}
    </>
  )

  if (part === 'headline') return <div className="sig-why">{headline}</div>
  if (part === 'body') return <div className="sig-why">{body}</div>
  return <div className="sig-why">{headline}{body}</div>
}
