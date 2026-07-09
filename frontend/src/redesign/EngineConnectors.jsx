/* ── Engine scene connective illustration ─────────────────────────────────
 * Every coordinate here comes from `anchors` (measured DOM positions, see
 * useAnchors.js) — zero hardcoded positions. No filter/backdrop-filter and
 * nothing animates (see LandingPage.jsx's ghost-smear header comment for
 * why that matters in this codebase). */
import { useEffect } from 'react'

const SCREEN_ANCHORS = ['screen-row-technicals', 'screen-row-price', 'screen-row-fundamentals', 'screen-row-macro', 'screen-row-news']
const SCREEN_TINTS = {
  'screen-row-technicals': 'var(--blue-2)',
  'screen-row-price': 'var(--green)',
  'screen-row-fundamentals': '#c084fc',
  'screen-row-macro': '#22d3ee',
  'screen-row-news': 'var(--gold)',
}

const SCORE_ANCHORS = ['score-row-aapl', 'score-row-googl', 'score-row-tsla', 'score-row-nvda', 'score-row-amzn']
const SCORE_TINTS = {
  'score-row-aapl': 'var(--green)',
  'score-row-googl': 'var(--gold)',
  'score-row-tsla': 'var(--red)',
  'score-row-nvda': 'var(--green)',
  'score-row-amzn': 'var(--gold)',
}

const rightMid = (r) => ({ x: r.x + r.w, y: r.y + r.h / 2 })
const leftMid = (r) => ({ x: r.x, y: r.y + r.h / 2 })
// n evenly-spaced y positions within r's height, each centered in its own slot.
const distributeY = (r, n) => Array.from({ length: n }, (_, i) => r.y + (i + 0.5) * (r.h / n))

// `r` (netInput/netOutput) is the TIGHT bounding box of just that layer's node
// circles — its net-space height is (count-1)*NET_ROW_GAP + 2*radius, NOT the
// full 420-tall viewBox (that was the previous bug: dividing by 420 compressed
// every point toward the center, since the group's real span is much smaller).
// Node i's fraction of that actual span is (radius + i*NET_ROW_GAP) / span,
// mapped onto the measured r.h. Matches LandingPage.jsx's netColumn(): both
// layers have 5 real nodes now, radius 5.5 (input) / 4.5 (output), gap 56.
const NET_ROW_GAP = 56
const netNodeY = (r, i, count, radius) => {
  const span = (count - 1) * NET_ROW_GAP + 2 * radius
  return r.y + ((radius + i * NET_ROW_GAP) / span) * r.h
}

// Cubic bezier that leaves p1 and arrives at p2 horizontally — control points
// pulled horizontally by `pull` fraction of the horizontal span.
const bezierPath = (p1, p2, pull = 0.4) => {
  const dx = (p2.x - p1.x) * pull
  return `M ${p1.x} ${p1.y} C ${p1.x + dx} ${p1.y}, ${p2.x - dx} ${p2.y}, ${p2.x} ${p2.y}`
}

export function EngineConnectors({ anchors, containerSize, containerRef, debug = false }) {
  // Invariant: 1 SVG unit === 1 CSS px, i.e. the viewBox width just committed
  // must equal .eng-panels' actual current width. Checked post-commit (ref
  // reads aren't allowed during render) every time containerSize changes.
  useEffect(() => {
    if (!containerRef?.current || !containerSize) return
    const live = containerRef.current.offsetWidth
    if (Math.abs(live - containerSize.width) > 1) {
      console.warn('[EngineConnectors] viewBox width', containerSize.width, 'diverges from live .eng-panels offsetWidth', live)
    }
  }, [containerRef, containerSize])

  if (!anchors || !containerSize) return null

  const tickerCloud = anchors['ticker-cloud']
  const screenCard = anchors['screen-card']
  const netInput = anchors['net-input-layer']
  const netOutput = anchors['net-output-layer']
  const scoreCard = anchors['score-card']
  const trackCard = anchors['track-card']

  // B — funnel: horizontally centered ON screen-card's left border (its own
  // center x === screenCard.x, straddling the border, derived from the
  // anchors object — not a hardcoded offset), vertically on screen-card's
  // center. Wide left edge (the "mouth"), narrow right "spout" pointing
  // into the card. Opaque var(--panel) backing first (it overlaps the card
  // now, so the border must not show through), gradient fill on top.
  const funnel = (screenCard) ? (() => {
    const gapX = screenCard.x
    const cy = screenCard.y + screenCard.h / 2
    const halfW = 17, wideH = 24, narrowH = 4
    return {
      mouthX: gapX - halfW,
      mouthTop: cy - wideH,
      mouthBottom: cy + wideH,
      path: `M ${gapX - halfW} ${cy - wideH} L ${gapX + halfW} ${cy - narrowH} `
          + `L ${gapX + halfW} ${cy + narrowH} L ${gapX - halfW} ${cy + wideH} Z`,
    }
  })() : null

  // A — ticker-cloud -> funnel's mouth (its wide left edge). Origins are 4
  // FIXED points on the ticker-cloud anchor box's right edge (middle 60% of
  // its height) — not tied to any individual chip, since chips scroll
  // continuously and their measured positions would be stale within a
  // single frame. Destinations are 4 points spread across the mouth's
  // vertical span, so the curves visibly converge/taper the way the funnel
  // itself does. Nothing is drawn past the mouth — the funnel is the
  // terminus, not a waypoint on the way to the card.
  const flowA = (tickerCloud && funnel)
    ? (() => {
        const originYs = distributeY({ y: tickerCloud.y + tickerCloud.h * 0.2, h: tickerCloud.h * 0.6 }, 4)
        const destYs = distributeY({ y: funnel.mouthTop, h: funnel.mouthBottom - funnel.mouthTop }, 4)
        return originYs.map((y, i) => {
          const p1 = { x: tickerCloud.x + tickerCloud.w, y }
          const p2 = { x: funnel.mouthX, y: destYs[i] }
          return { id: `ecFlowA${i}`, p1, p2, d: bezierPath(p1, p2, 0.4) }
        })
      })()
    : []

  // C — screen-card's right edge (x) at each row's vertical center (y) -> net-input-layer's
  // RIGHT edge (not center) — the curves travel left-to-right, and the visible dot (node +
  // its glow halo) extends past the tightly-measured node-only bounding box, so stopping at
  // the center left a visible gap before the dot. Right edge carries the curve further into it.
  const flowC = (netInput && screenCard)
    ? (() => {
        const ys = Array.from({ length: 5 }, (_, i) => netNodeY(netInput, i, 5, 5.5))
        const cx = netInput.x + netInput.w / 5
        const originX = screenCard.x + screenCard.w
        return SCREEN_ANCHORS.map((name, i) => {
          const row = anchors[name]
          if (!row) return null
          const p1 = { x: originX, y: row.y + row.h / 2 }
          const p2 = { x: cx, y: ys[i] }
          return { key: name, color: SCREEN_TINTS[name], d: bezierPath(p1, p2, 0.4) }
        }).filter(Boolean)
      })()
    : []

  // D — net-output-layer's LEFT edge (mirrors C's reasoning: these curves travel further
  // right from here, so starting at the left edge instead of center carries them through
  // more of the dot, closing the same visible gap) -> score-card's left edge (x) at each
  // row's vertical center (y). Row anchors used only for .y.
  const flowD = (netOutput && scoreCard)
    ? (() => {
        const ys = Array.from({ length: 5 }, (_, i) => netNodeY(netOutput, i, 5, 4.5))
        const cx = netOutput.x + netOutput.w
        const destX = scoreCard.x 
        return SCORE_ANCHORS.map((name, i) => {
          const row = anchors[name]
          if (!row) return null
          const p1 = { x: cx, y: ys[i]  }
          const p2 = { x: destX, y: row.y + row.h / 2 }
          return { key: name, color: SCORE_TINTS[name], d: bezierPath(p1, p2, 0.4), end: p2 }
        }).filter(Boolean)
      })()
    : []

  // E — score-card -> track-card.
  const flowEEnd = (scoreCard && trackCard) ? leftMid(trackCard) : null
  const flowE = (scoreCard && trackCard) ? bezierPath(rightMid(scoreCard), flowEEnd, 0.5) : null

  return (
    <svg
      className="eng-connectors"
      width="100%"
      height="100%"
      viewBox={`0 0 ${containerSize.width} ${containerSize.height}`}
      aria-hidden="true"
    >
      <defs>
        {flowA.map((f) => (
          <linearGradient key={f.id} id={f.id} gradientUnits="userSpaceOnUse" x1={f.p1.x} y1={f.p1.y} x2={f.p2.x} y2={f.p2.y}>
            <stop offset="0%" stopColor="var(--blue-2)" stopOpacity="0.22" />
            <stop offset="100%" stopColor="var(--blue-2)" stopOpacity="0.65" />
          </linearGradient>
        ))}
        {funnel && (
          <linearGradient id="ecFunnelFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--blue-2)" stopOpacity="0.06" />
            <stop offset="100%" stopColor="var(--blue-2)" stopOpacity="0.01" />
          </linearGradient>
        )}
      </defs>

      {flowA.map((f) => (
        <path key={f.id} d={f.d} fill="none" stroke={`url(#${f.id})`} strokeWidth="1.2" />
      ))}

      {funnel && (
        <>
          {/* Opaque backing first — the funnel now overlaps screen-card's
              border, so the border must not show through its interior. */}
          <path d={funnel.path} fill="var(--panel)" stroke="none" />
          <path d={funnel.path} fill="url(#ecFunnelFill)" stroke="var(--blue-2)" strokeWidth="1" strokeOpacity="0.40" />
        </>
      )}

      {flowC.map((f) => (
        <path key={'c-' + f.key} d={f.d} fill="none" stroke={f.color} strokeWidth="1.5" strokeOpacity="0.35" />
      ))}

      {flowD.map((f) => (
        <g key={'d-' + f.key}>
          <path d={f.d} fill="none" stroke={f.color} strokeWidth="1.5" strokeOpacity="0.40" />
          <circle cx={f.end.x} cy={f.end.y} r="2.5" fill={f.color} opacity="0.8" />
        </g>
      ))}

      {flowE && (
        <g>
          <path d={flowE} fill="none" stroke="var(--blue-2)" strokeWidth="1.2" strokeOpacity="0.40" />
          <circle cx={flowEEnd.x} cy={flowEEnd.y} r="2.5" fill="var(--blue-2)" opacity="0.8" />
        </g>
      )}

      {/* Debug overlay — every measured anchor, independent of whether any
          connector above draws to it. Temporary; removed once the anchor
          system is fully trusted. */}
      {debug && Object.entries(anchors).map(([name, r]) => (
        <g key={'dbg-' + name}>
          <rect x={r.x} y={r.y} width={r.w} height={r.h} fill="none" stroke="magenta" strokeWidth="1" />
          <text x={r.x + 2} y={r.y + 9} fontSize="8" fill="magenta">{name}</text>
        </g>
      ))}
    </svg>
  )
}
