/* Slide-to-unlock control — drag the knob left→right to confirm a per-row
 * unlock purchase. The circular knob is the full height of the track and sits
 * flush on the left edge, so the blue fill is hidden under it at rest and only
 * reveals in its wake as you slide. On a full slide it calls onUnlock() (charges
 * the wallet + invalidates the query; a successful unlock unmounts this card).
 * On failure / short slide the knob snaps back. Pointer events → mouse + touch. */
import { useRef, useState, useEffect } from 'react'

const KNOB = 50   // knob diameter (px) = track height, keep in sync with CSS

export default function SlideToUnlock({ priceLabel, onUnlock }) {
  const rootRef = useRef(null)
  const maxRef = useRef(0)
  const mounted = useRef(true)
  const [x, setX] = useState(0)
  const [drag, setDrag] = useState(false)
  const [busy, setBusy] = useState(false)

  useEffect(() => () => { mounted.current = false }, [])

  const measure = () => {
    const w = rootRef.current?.clientWidth || 0
    maxRef.current = Math.max(0, w - KNOB)
  }

  const onDown = (e) => {
    if (busy) return
    measure()
    setDrag(true)
    e.currentTarget.setPointerCapture?.(e.pointerId)
  }
  const onMove = (e) => {
    if (!drag) return
    const rect = rootRef.current.getBoundingClientRect()
    const nx = Math.max(0, Math.min(e.clientX - rect.left - KNOB / 2, maxRef.current))
    setX(nx)
  }
  const onUp = async () => {
    if (!drag) return
    setDrag(false)
    if (x < maxRef.current - 6) { setX(0); return }   // didn't reach the end → snap back
    setX(maxRef.current)
    setBusy(true)
    const ok = await onUnlock?.()
    // Success → stay at the end; the card fades out and unmounts. Only reset on failure.
    if (ok === false && mounted.current) { setBusy(false); setX(0) }
  }

  return (
    <div className="slide-unlock" ref={rootRef}>
      <span className="slide-track" aria-hidden="true">
        {/* 0 at rest (no blue); once sliding, the fill reaches the knob's CENTER so
            it sits behind the circle with no uncovered gap where they meet */}
        <span className="slide-fill" style={{ width: x > 0 ? x + KNOB / 2 : 0 }} />
        <span className="slide-label">{busy ? 'Unlocking…' : 'Slide to unlock'}</span>
      </span>
      <span
        className="slide-knob"
        role="button" tabIndex={0} aria-label={`Slide to unlock for ${priceLabel}`}
        style={{ transform: `translateX(${x}px)`, transition: drag ? 'none' : 'transform .2s ease' }}
        onPointerDown={onDown} onPointerMove={onMove} onPointerUp={onUp} onPointerCancel={onUp}
      >
        {busy
          ? <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.8" strokeLinecap="round" strokeLinejoin="round"><path d="M20 6L9 17l-5-5" /></svg>
          : <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round"><path d="M7 6l6 6-6 6" /><path d="M13 6l6 6-6 6" /></svg>}
      </span>
    </div>
  )
}
