/* Shared "Unlock all" controls used above every section that bulk-unlocks locked
 * rows for the wallet (BUY / SELL signal columns and the Open trades table). Both
 * pieces live here so the three sections stay visually + behaviourally identical. */
import { useState, useEffect } from 'react'
import { fmtCents } from './gating'

// Compact "Unlock all" button. It opens the confirm dialog (the misclick guard),
// so a single click never spends money directly — the dialog is the deliberate step.
// `tone` ('buy' | 'sell') tints it like that section's tag; omit for neutral.
// `anchor` (optional) is the undiscounted "was" price — shown struck through
// before the real price when it's higher, to surface the saving.
export function UnlockAllButton({ price, anchor, onClick, label = 'Unlock all', tone }) {
  const showAnchor = anchor > price
  return (
    <button type="button" className={'unlock-all-btn' + (tone ? ' unlock-all-btn--' + tone : '')} onClick={onClick}>
      {label} · {showAnchor && <span className="ua-was">{fmtCents(anchor)}</span>} {fmtCents(price)}
    </button>
  )
}

// Confirm dialog for the bulk "Unlock all" — a single click charges the wallet
// (unlike the deliberate per-row slide), so a misclick must not spend money.
export function ConfirmUnlockDialog({ title, body, price, onConfirm, onCancel }) {
  const [busy, setBusy] = useState(false)
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape' && !busy) onCancel() }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [busy, onCancel])
  const handleConfirm = async () => { setBusy(true); await onConfirm() }
  return (
    <div className="confirm-overlay" onClick={() => !busy && onCancel()} role="dialog" aria-modal="true">
      <div className="confirm-box" onClick={(e) => e.stopPropagation()}>
        <div className="confirm-title">{title}</div>
        <div className="confirm-body">{body}</div>
        <div className="confirm-actions">
          <button className="confirm-cancel" onClick={onCancel} disabled={busy}>Cancel</button>
          <button className="confirm-ok" onClick={handleConfirm} disabled={busy}>
            {busy ? 'Unlocking…' : `Unlock · ${fmtCents(price)}`}
          </button>
        </div>
      </div>
    </div>
  )
}
