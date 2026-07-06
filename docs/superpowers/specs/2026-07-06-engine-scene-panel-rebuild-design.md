# Engine scene: rebuild as 4 bordered panels (reference-matched) — design

Status: approved by user 2026-07-06 ("I want everything to look exactly like this, as much as you can"). Implementation not started.

## Problem

The user reviewed the just-shipped engine-scene diagram (3 zone captions + a 4th "Track" zone of per-signal win/loss badges, all floating over one continuous canvas) and rejected it as "too simple," pointing to a reference infographic (4 bordered panel-cards, chevron-connected, each information-dense with icons, descriptions, and a real-feeling chart) as "the level I am looking for."

This is a full visual rebuild of `EngineScene`'s presentation layer, not an incremental extension: the continuous-canvas structure is replaced by 4 discrete bordered panels, and last session's Track-zone work (per-signal win/loss badges + connector lines) is removed and replaced with a chart+stats panel, per the user's explicit choice.

## Scope

**In scope:**
- Replace the single continuous `.ld-engine` canvas with 4 bordered panel cards (`Screen` / `Score` / `Signal` / `Track`), each with its own header bar (number + title) and a chevron arrow connecting to the next panel — matching the reference's layout.
- Each panel also gets a 2-line footer caption below its bordered box (bold tagline + smaller subtitle), matching the reference's per-column footer text: "Daily stock universe (1,800+)" / "Criteria-based filtering", "Data fusion & advanced modeling", "Daily signal decision (BUY/HOLD/SELL)", "Long-term accuracy & alpha tracking".
- **Panel 1 (Screen):** keeps the existing animated dashed feed lines (unchanged mechanism/timing), each feed gains a small color-coded icon badge + a one-line description under its label (e.g. "PRICE — trend, momentum, volatility"). Adds a decorative, static cluster of ~10-14 overlapping ticker-logo chips on the panel's left edge (reusing the site's self-hosted `/logos/{TICKER}.png`), suggesting the scale of the 1,800+ universe — purely decorative, no animation.
- **Panel 2 (Score):** the existing neural net (nodes/edges/pulses) unchanged, contained in its own bordered card. Adds small "Feature extraction" / "Pattern recognition" sub-captions near the net's left/right sides.
- **Panel 3 (Signal):** the existing ticker + BUY/HOLD/SELL cards, contained in a clean bordered list. No numeric score of any kind (confirmed: VQS/"Deep Score" stays internal-only, per the standing rule).
- **Panel 4 (Track):** **replaces** last session's per-signal win/loss badges + connector lines entirely. New content: a static, illustrative equity-curve-style line chart (a green "Alpha" line vs. a gray "Benchmark" line, with a few green BUY / red SELL dot markers along the alpha line, small legend beneath), plus 3 static stat lines below it (Win Rate / Alpha Generation / Sharpe Ratio). Numbers are deliberately generic/round (not matching any specific real figure) so this reads as illustration sitting one section above the real, live Proof section below it.
- Architecture: split `EngineScene` into one sub-component per panel (`ScreenPanel`/`ScorePanel`/`SignalPanel`/`TrackPanel`), given the added complexity — each panel is its own self-contained visual unit now, not one large continuous scene.

**Out of scope (explicit):**
- No live/backend data anywhere in this diagram — confirmed decorative/illustrative throughout (same convention as before), consistent with the file's existing "everything below is a fixed illustrative example, aria-hidden" header comment.
- No animation added to Panel 3's ticker list or Panel 4's chart — matching the reference's own static character for those two panels. Animation is preserved only where it already exists and reads as "powerful illustration": Panel 1's flowing feed lines and Panel 2's pulsing neural net.
- No change to mobile (`≤780px`) fallback beyond what's structurally necessary — the diagram still needs a stacked/simplified presentation there; exact mobile panel-stacking is an implementation detail for the plan, not a new requirement here.
- No change to the eased-scroll behavior, the Proof section, or any other part of the landing page outside `EngineScene`.

## Design

### 1. Outer layout: 4 panels + chevrons + footers

Each panel is a bordered card (dark background, subtle border, rounded corners — same visual language as the current `.ld-engine` panel) with:
- A header bar: `01 Screen` / `02 Score` / `03 Signal` / `04 Track` (number badge + title, same typographic treatment already established for captions).
- A chevron arrow between adjacent panels (small `›`-style glyph, connecting panel N to panel N+1).
- A 2-line footer caption below the bordered box (bold short phrase + smaller subtitle), per the content list in Scope above.

### 2. Panel 1 — Screen

Reuses `ENGINE_INPUTS`' existing 6 feeds (Price/Fundamentals/Financials/Analyst/News/Macro) and their existing animated dashed-line mechanic (`engDashIn` keyframe, per-feed color/duration), unchanged. Each feed row gains:
- A small square icon badge, color-matched to that feed's existing `FEED_COLORS` entry (simple monoline SVG icons, not emoji — matching this site's existing custom-icon convention).
- A one-line gray description under the feed's label (e.g. "RSI, moving averages, volatility" for Price/Technicals).

A decorative logo-chip cluster sits on the panel's left edge: 10-14 small ticker-logo tiles (reusing `/logos/{TICKER}.png`, e.g. MSFT/NVDA/AAPL/GOOGL/AMZN/TSLA/META/AVGO plus a few repeats), loosely overlapping with slight rotation via CSS transforms, entirely static (no animation) — pure "there are 1,800+ of these" scale flavor, `aria-hidden` like the rest of the scene.

### 3. Panel 2 — Score

The existing `NET_IN_NODES`/`NET_HID_NODES`/`NET_OUT_NODES`/`NET_EDGES`/`NET_PULSE_META` system is unchanged — same node counts, same wiring, same pulse animation. It's now contained inside its own bordered panel card instead of sitting on an open canvas. Two small static sub-captions appear near the net, one on the input side ("Feature extraction") and one on the output side ("Pattern recognition"), matching the reference's labeling.

### 4. Panel 3 — Signal

The existing `ENGINE_OUTPUTS` cards (logo + ticker + BUY/HOLD/SELL pill, cycling in/out via `engCardCycle`) are unchanged in content and animation, now presented as a clean bordered list rather than floating over open canvas. No numeric score anywhere in this panel.

### 5. Panel 4 — Track

Entirely new content, replacing last session's per-signal win/loss badges (`ENGINE_OUTPUTS[].track`, `.eng-track`, `.eng-track-line`, `TRACK_LINE_X0`/`TRACK_X` — all removed):
- A small static SVG line chart: a green "Alpha" polyline trending generally upward with some natural-looking noise, a flatter gray "Benchmark" polyline, and 3-4 small dot markers (green = BUY, red = SELL) placed along the alpha line at plausible points. A small text legend ("● BUY  ● SELL") beneath the chart.
- 3 static stat lines below the chart: `Win Rate: 64%`, `Alpha Generation: +11% vs. S&P`, `Sharpe Ratio: 1.7` — deliberately different from any specific real number the site has published elsewhere, so it can't be mistaken for a live claim.
- No animation on any of this (static, matching the reference and the "chart+stats, not per-signal cycling" decision).

### 6. Mobile fallback

The existing `≤780px` breakpoint already stacks/simplifies this scene (hides captions, forces a 1:1 aspect diagram, falls back to a full-text `.ld-flow` list). The 4-panel structure gets the equivalent treatment: the 4 panels stack vertically, full width, one per row, with the chevrons hidden (a vertical stack reads fine without a left-to-right connector). The existing full-text `.ld-flow` fallback list is removed at this breakpoint too, since each stacked panel now already carries its own descriptive content (this is a more capable mobile fallback than the old text-only list, not a regression).

## Testing plan

- Manual: confirm all 4 panels render with correct headers, chevrons, and footer captions at a wide desktop width.
- Manual: confirm Panel 1's feed lines still animate correctly with the new icon+description rows added, and the logo-chip cluster doesn't overlap or clip.
- Manual: confirm Panel 2's neural net still pulses correctly inside its new bordered container.
- Manual: confirm Panel 3 shows ticker+verdict only, no score number anywhere.
- Manual: confirm Panel 4's chart and stat lines render legibly and don't imply real/live data (no accidental match to real published figures).
- Manual: confirm mobile (`≤780px`) still conveys all 4 steps clearly in some stacked/simplified form.
- Manual: confirm `prefers-reduced-motion: reduce` still correctly freezes Panel 1/2's animations (Panel 3/4 have no animation to freeze).
