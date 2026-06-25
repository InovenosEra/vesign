/* Shared "Unlock all" controls used above every section that bulk-unlocks locked
 * rows for the wallet (BUY / SELL signal columns and the Open trades table). Both
 * pieces live here so the three sections stay visually + behaviourally identical. */
import { useState, useEffect } from 'react'
import { fmtCents } from './gating'

// A switch, not a plain button: flipping it opens the confirm dialog (a misclick
// must not spend money). `active` is true only while that dialog is pending — the
// knob animates via `left` (NOT transform) so nothing gets a compositing layer
// that would flash the page's filter:blur ghost-smear in Chrome.
export function UnlockAllToggle({ price, active, onToggle, label = 'Unlock all today' }) {
  return (
    <button
      type="button"
      className={'see-all-toggle' + (active ? ' on' : '')}
      role="switch"
      aria-checked={active}
      onClick={onToggle}
    >
      <span className="sat-label">{label} · {fmtCents(price)}</span>
      <span className="sat-switch" aria-hidden="true"><span className="sat-knob" /></span>
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
