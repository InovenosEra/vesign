/* Slide-to-unlock control — drag the knob left→right to confirm a per-row
 * unlock purchase. Replaces the old click "Unlock · $price" button. On a full
 * slide it calls onUnlock() (which charges the wallet + invalidates the query;
 * a successful unlock unmounts this card). On failure (or short slide) the knob
 * snaps back. Pointer events → works for mouse and touch. */
import { useRef, useState, useEffect } from 'react'

const KNOB = 38   // knob size (px), keep in sync with CSS
const PAD = 4     // track inner padding (px), keep in sync with CSS

export default function SlideToUnlock({ priceLabel, onUnlock }) {
  const trackRef = useRef(null)
  const maxRef = useRef(0)
  const mounted = useRef(true)
  const [x, setX] = useState(0)
  const [drag, setDrag] = useState(false)
  const [busy, setBusy] = useState(false)

  useEffect(() => () => { mounted.current = false }, [])

  const measure = () => {
    const w = trackRef.current?.clientWidth || 0
    maxRef.current = Math.max(0, w - KNOB - PAD * 2)
  }

  const onDown = (e) => {
    if (busy) return
    measure()
    setDrag(true)
    e.currentTarget.setPointerCapture?.(e.pointerId)
  }
  const onMove = (e) => {
    if (!drag) return
    const rect = trackRef.current.getBoundingClientRect()
    const nx = Math.max(0, Math.min(e.clientX - rect.left - PAD - KNOB / 2, maxRef.current))
    setX(nx)
  }
  const onUp = async () => {
    if (!drag) return
    setDrag(false)
    if (x < maxRef.current - 6) { setX(0); return }   // didn't reach the end → snap back
    setX(maxRef.current)
    setBusy(true)
    try {
      await onUnlock?.()
    } finally {
      // On success the row unlocks and this card unmounts; on failure we're
      // still mounted, so reset for another try.
      if (mounted.current) { setBusy(false); setX(0) }
    }
  }

  return (
    <div className="slide-unlock" ref={trackRef}>
      <span className="slide-fill" style={{ width: x + KNOB + PAD }} aria-hidden="true" />
      <span className="slide-label">{busy ? 'Unlocking…' : `Slide to unlock · ${priceLabel}`}</span>
      <span
        className="slide-knob"
        role="button" tabIndex={0} aria-label={`Slide to unlock for ${priceLabel}`}
        style={{ transform: `translateX(${x}px)`, transition: drag ? 'none' : 'transform .2s ease' }}
        onPointerDown={onDown} onPointerMove={onMove} onPointerUp={onUp} onPointerCancel={onUp}
      >
        {busy
          ? <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round"><path d="M20 6L9 17l-5-5" /></svg>
          : <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round"><path d="M9 6l6 6-6 6" /></svg>}
      </span>
    </div>
  )
}
