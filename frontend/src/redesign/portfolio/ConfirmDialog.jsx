/* Generic confirm/cancel modal — same visual language as the signals unlock
 * flow's ConfirmUnlockDialog (signals/UnlockAll.jsx), but content-agnostic so
 * watchlist delete/remove actions don't need to pull in unlock-specific code. */
import { useState } from 'react'

export default function ConfirmDialog({ title, body, confirmLabel = 'Confirm', danger = false, onConfirm, onCancel }) {
  const [busy, setBusy] = useState(false)
  const handleConfirm = async () => {
    setBusy(true)
    await onConfirm()
  }
  return (
    <div className="confirm-overlay" onClick={() => !busy && onCancel()} role="dialog" aria-modal="true">
      <div className="confirm-box" onClick={(e) => e.stopPropagation()}>
        <div className="confirm-title">{title}</div>
        <div className="confirm-body">{body}</div>
        <div className="confirm-actions">
          <button className="confirm-cancel" onClick={onCancel} disabled={busy}>Cancel</button>
          <button className={'confirm-ok' + (danger ? ' danger' : '')} onClick={handleConfirm} disabled={busy}>
            {busy ? 'Working…' : confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
