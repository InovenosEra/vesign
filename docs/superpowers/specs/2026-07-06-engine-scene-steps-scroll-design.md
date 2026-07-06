# Engine scene step integration + smoother section scroll — design

Status: approved by user 2026-07-06. Implementation not started.

## Problem

The landing page's "How it works" scene (`EngineScene` in
`frontend/src/redesign/LandingPage.jsx`) currently shows the neural-net
diagram, then — as a visually separate 4-column grid (`.ld-flow`) below it —
the same story retold as four numbered steps (Screen/Score/Signal/Track). The
diagram's own zone captions ("Reading the market" / "The engine" / "Signals,
live") say almost the same thing as the steps in different words, so a
visitor reads the concept twice: once as a diagram, once as a legend.

Separately, the landing page's section-by-section scroll
(`scroll-snap-type: y mandatory`, added in `ead61ee`) hands the settle
animation entirely to the browser, which reads as an abrupt jump rather than
a glide.

## Scope

**In scope:**
- Replace the 3 desktop `.eng-cap` zone captions with the actual step
  identity (number + title + short tag), so the diagram and the steps are
  one integrated read instead of diagram-then-legend.
- Give step 04 "Track" (which has no corresponding zone in the diagram — it
  describes what happens *after* a signal fires) its own slim strip under
  the diagram, distinct from the 3-zone caption treatment.
- Add a JS-driven eased scroll handler so section-to-section snapping glides
  on every input device, replacing reliance on the browser's native
  (uncontrollable) snap-settle animation.

**Out of scope (explicit):**
- No change to the diagram's visuals, animation, feeds, or output cards
  themselves — only the caption/label layer above them.
- No change to mobile (`≤780px`) rendering of the steps — `.eng-cap` is
  already `display:none` there and the diagram falls back to a stacked 1:1
  layout; the existing full-sentence `.ld-flow` 4-column block keeps
  rendering unchanged at that width, since there's no caption space to move
  the copy into.
- No change to any other landing-page section's scroll behavior beyond the
  snap mechanism itself (which is global to `.rd.ld main > *` already).

## Design

### 1. Desktop diagram captions (`>780px`)

`STEPS` (already defined, `LandingPage.jsx:313`) gains a short `tag` field
per step, used only in the diagram (the existing `d` sentence is kept as-is
for the mobile `.ld-flow` rendering):

```
01 Screen → tag: "1,800+ stocks, daily"
02 Score  → tag: "3 independent reads"
03 Signal → tag: "BUY / SELL, live"
```

The three `.eng-cap` elements (`left`/`center`/`right`) render `01 · Screen`
etc. as the primary line (replacing today's plain zone label) with the short
`tag` as a smaller sub-line underneath. This needs roughly two text lines of
vertical room in the top ~15-20% of the diagram, above where the first feed
line/node currently appears — no repositioning of the SVG content itself.

### 2. Step 04 · Track

Rendered as a new slim strip directly under `.ld-engine`, replacing the
current 4-column `.ld-flow` grid at desktop widths only: `04 · Track` +
its existing full sentence, styled as a one-line coda (small rule above it,
left-aligned), not as a 4th peer column.

### 3. Mobile (`≤780px`)

Unchanged. The existing `STEPS.map` → `.ld-flow` 4-column block (all four
steps, full `d` sentences) keeps rendering exactly as it does today. The new
desktop-only markup (diagram captions + Track strip) is hidden at this
breakpoint; the existing `.ld-flow` is hidden at desktop widths where the
diagram captions take over. Net effect: same `STEPS` data, two renderings
gated by the existing `780px` breakpoint already used for `.eng-cap`.

### 4. Scroll smoothing

Add a wheel/keyboard handler (mounted only while the landing page's
`ld-snap` class is active, same effect that currently toggles it) that:
- Detects a decisive scroll gesture (wheel delta or PageDown/arrow/space),
  `preventDefault()`s it, and animates `window.scrollTo` to the next/previous
  section's offset using a fixed eased duration (~500ms ease-out).
- Disables the CSS `scroll-snap-type` while mounted, so the browser's native
  snap never fights the JS-driven animation.
- Respects `prefers-reduced-motion: reduce` (falls back to the existing
  plain-scroll, non-snapping behavior — same as today's existing
  reduced-motion carve-out for `html.ld-snap`).
- Leaves in-page anchor links (`href="/#how"`) alone — they already rely on
  native anchor jump + `scroll-behavior: smooth` and aren't part of the
  wheel/keyboard gesture path.
- Touch/trackpad swipe on mobile is out of scope here since mobile doesn't
  use `ld-snap`/full-viewport sections at all (see `landing.css:176`, `.ld-scene`
  drops `min-height` at `≤780px`).

## Testing plan

- Manual: verify each of the 3 desktop captions renders inside the diagram
  panel without overlapping the SVG content at the min (`260px`) and max
  (`460px`) clamped diagram heights.
- Manual: verify mobile (`≤780px`) still shows the full 4-step `.ld-flow`
  block unchanged.
- Manual: scroll through the landing page with mouse wheel, trackpad, and
  keyboard (Page Down / arrow keys) and confirm each section-to-section
  transition glides rather than jumps.
- Manual: toggle OS-level "reduce motion" and confirm scroll falls back to
  plain (non-animated) scrolling, matching the existing reduced-motion
  behavior for the rest of the snap system.
