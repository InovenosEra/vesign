/* Slide-to-unlock control — drag the knob left→right to confirm a per-row unlock.
 * The circular knob is the full height of the track and sits flush on the left,
 * so the blue fill is hidden under it at rest and only reveals in its wake.
 *
 * Drag is implemented with WINDOW pointer listeners + refs (not element handlers
 * / setPointerCapture / React state). This is immune to the component re-rendering
 * mid-drag — the Signals list refetches every few seconds, and the old approach
 * lost the pointer/drag-state on those re-renders so the release was dropped. */
import { useRef, useState, useEffect, useCallback } from 'react'

const KNOB = 50   // knob diameter (px) = track height, keep in sync with CSS

export default function SlideToUnlock({ priceLabel, onUnlock }) {
  const rootRef = useRef(null)
  const dragRef = useRef(false)
  const xRef = useRef(0)
  const maxRef = useRef(0)
  const mounted = useRef(true)
  const onUnlockRef = useRef(onUnlock)
  onUnlockRef.current = onUnlock

  const [x, setX] = useState(0)
  const [dragging, setDragging] = useState(false)   // drives knob transition only
  const [busy, setBusy] = useState(false)

  const measure = () => {
    const w = rootRef.current?.clientWidth || 0
    maxRef.current = Math.max(0, w - KNOB)
  }

  const move = useCallback((clientX) => {
    if (!dragRef.current || !rootRef.current) return
    const rect = rootRef.current.getBoundingClientRect()
    const nx = Math.max(0, Math.min(clientX - rect.left - KNOB / 2, maxRef.current))
    xRef.current = nx
    setX(nx)
  }, [])

  const winMove = useCallback((e) => move(e.clientX), [move])

  const winUp = useCallback(async () => {
    if (!dragRef.current) return
    dragRef.current = false
    setDragging(false)
    window.removeEventListener('pointermove', winMove)
    window.removeEventListener('pointerup', winUp)
    window.removeEventListener('pointercancel', winUp)
    const reached = xRef.current >= maxRef.current * 0.9     // forgiving end threshold
    if (!reached) { xRef.current = 0; setX(0); return }       // short slide → snap back
    xRef.current = maxRef.current; setX(maxRef.current)
    setBusy(true)
    const ok = await onUnlockRef.current?.()
    // Success → stay at the end; the card fades out and unmounts. Reset only on failure.
    if (ok === false && mounted.current) { setBusy(false); xRef.current = 0; setX(0) }
  }, [move, winMove])

  useEffect(() => () => {
    mounted.current = false
    window.removeEventListener('pointermove', winMove)
    window.removeEventListener('pointerup', winUp)
    window.removeEventListener('pointercancel', winUp)
  }, [winMove, winUp])

  const onDown = (e) => {
    if (busy) return
    e.preventDefault()
    measure()
    dragRef.current = true
    setDragging(true)
    window.addEventListener('pointermove', winMove)
    window.addEventListener('pointerup', winUp)
    window.addEventListener('pointercancel', winUp)
  }

  return (
    <div className="slide-unlock" ref={rootRef}>
      <span className="slide-track" aria-hidden="true">
        <span className="slide-fill" style={{ width: x > 0 ? x + KNOB / 2 : 0 }} />
        <span className="slide-label">{busy ? 'Unlocking…' : 'Slide to unlock'}</span>
      </span>
      <span
        className="slide-knob"
        role="button" tabIndex={0} aria-label={`Slide to unlock for ${priceLabel}`}
        style={{ transform: `translateX(${x}px)`, transition: dragging ? 'none' : 'transform .2s ease' }}
        onPointerDown={onDown}
      >
        {busy
          ? <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.8" strokeLinecap="round" strokeLinejoin="round"><path d="M20 6L9 17l-5-5" /></svg>
          : <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round"><path d="M7 6l6 6-6 6" /><path d="M13 6l6 6-6 6" /></svg>}
      </span>
    </div>
  )
}
