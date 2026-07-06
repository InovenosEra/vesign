# Engine scene: add a real 4th "Track" zone — design

Status: approved by user 2026-07-06 ("Just do it"). Implementation not started.

## Problem

The engine-scene diagram (`EngineScene` in `frontend/src/redesign/LandingPage.jsx`) illustrates 3 of the 4 "how it works" steps as one continuous visual: data feeds flow into a neural net (`01 Screen`/`02 Score`), which emits BUY/SELL/HOLD signal cards (`03 Signal`). Step `04 Track` ("every position is stop-managed; every closed trade — win or lose — is published") currently has no visual presence in the diagram at all — it only exists as a text strip (`.ld-track`) below the panel.

The user wants a real, illustrated 4th zone inside the diagram itself, directly continuing the left-to-right narrative: a signal fires, then it becomes a tracked position that eventually closes as a win or a loss.

## Scope

**In scope:**
- Widen the diagram's SVG canvas (`ENG_VB_W`) to make room for a 4th zone to the right of the existing signal cards, without altering zones 1–3's existing geometry, timing, or tuned pulse/animation values (`NET_LAYER_X`, `NET_*_NODES`, `NET_EDGES`, `NET_PULSE_META`, `ENGINE_INPUTS`, `bezierIn`, `bezierOut` — all unchanged).
- Reposition the existing signal cards (`.eng-card`, currently `right: 1.5%`) and the `03 Signal` caption leftward to make room, since the new zone claims the panel's true right edge.
- For each **BUY/SELL** signal only (not HOLD — a hold isn't a trade), add a connecting line and a small "closed trade" record in the new zone: ticker + a WIN/LOSS badge + a realized return %, animated on the same shared `dur`/`delay` as its signal card so the two can never drift out of sync, timed to visually follow the card's own reveal/hold/fade cycle.
- A mix of wins and losses among the 5 BUY/SELL records (not all wins), consistent with the page's existing "every trade, win or lose" ethos (see the Proof section below this one).
- A 4th `.eng-cap` caption ("`04 · Track`" + short tag) positioned above the new zone, matching the two-line title+tag style already established for the other 3 captions.
- Remove the desktop-only `.ld-track` text strip (now redundant — the diagram itself carries this content). Mobile's `.ld-flow` 4-item text list is unaffected (unchanged breakpoint/behavior).

**Out of scope (explicit):**
- No change to zones 1–3's actual illustrated content, node/edge geometry, or timing constants.
- No live data — this stays a fixed illustrative example like the rest of the diagram (existing file-header TODO comment already documents this).
- No change to the mobile (`≤780px`) fallback — captions stay hidden there, `.ld-flow`'s full 4-item text list is untouched.
- No change to the eased-scroll behavior shipped in the prior session's work.

## Design

### 1. Canvas geometry

`ENG_VB_W` grows from `1400` to `1850` (`ENG_VB_H` stays `620`; the panel's CSS `aspect-ratio` updates to match, `1850/620`). Zones 1–3 keep every existing x-coordinate exactly as-is (`NET_LAYER_X = { in: 550, hid: 700, out: 850 }`, feed line origin `x=40`, `bezierOut`'s `x0=1350` card-line endpoint) — the extra width is pure new space added to the right, reserved for zone 4. This means zones 1–3 end up occupying a smaller fraction of the panel than before (roughly the left ~76%), with zone 4 taking the remaining ~24% — a deliberate tradeoff to avoid re-tuning the carefully-calibrated existing pulse/animation timings in zones 1–3.

### 2. Repositioning cards + the `03 Signal` caption

- `.eng-card`'s `right: 1.5%` becomes `right: 25%` (cards still anchor near `bezierOut`'s unchanged `x=1350` endpoint, which is now further from the new right edge).
- The `03 Signal` caption (currently `.eng-cap.right { right: 1.5% }`) becomes `right: 25%` (same value as the cards, so caption and card column align).
- The `02 Score` caption (currently `.eng-cap.center { left: 50%; transform: translateX(-50%) }`, centered on the *panel*) must instead center on the *neural net's* new relative position: `left: 37.8%` (i.e. `700 / 1850`), keeping the same `translateX(-50%)` centering trick.
- The `01 Screen` caption (`.eng-cap.left { left: 2% }`) is unchanged — zone 1 still starts at the panel's left edge.

### 3. New zone 4: track records

New per-signal data, added only to the `ENGINE_OUTPUTS` entries whose `verdict` is `buy` or `sell` (not `hold`):

```
NVDA buy  → win,  +18.4%
XOM  sell → win,   +6.1%
AAPL buy  → loss,  -2.3%
TSLA sell → loss,  -4.8%
AVGO buy  → win,  +14.2%
```

(3 wins / 2 losses — MSFT and GOOGL, both `hold`, get no track record at all.)

Each record is a small HTML badge (`.eng-track`, sibling of `.eng-card`, same `top: %` positioning technique from the shared `cardY`) anchored at `right: 1.5%` (the new true right edge — the value freed up by moving `.eng-card` to `right: 25%`), showing: ticker (small/dim, de-emphasized relative to the live card since it's a downstream/settled result) + a colored WIN/LOSS pill + the return %.

A connecting line is drawn from each card's position (`x=1400`, just past the existing card anchor) to the new track record (`x=1820`, near the new right edge) at the *same* y as that signal's `cardY` (no vertical fan needed — a plain `<line>`, not a bezier, since both ends share the same y). Styled and animated the same way as `.eng-signal` (`stroke-dasharray`/`stroke-dashoffset` draw-in technique), colored green for `win` / red for `loss` (reusing `var(--green)` / `var(--red)`), using the *same* `dur`/`delay` as that signal's existing card and line so all three (card, its existing signal-line, its new track-line + record) stay perfectly in sync.

Timing: the existing card/line cycle is invisible 0–45%, visible 58–85%, fading 85–100% of each loop (`engCardCycle`/`engLineDraw`). The new track record's keyframe (`engTrackCycle`, applied to both the new line and the new badge) fades in starting at 80% (slightly overlapping the card's own fade-out, so it reads as "handing off" from live signal to closed result), holds through 96%, and fades out by 100% — landing back at empty exactly when the loop restarts and the card begins its own next fade-in.

### 4. New caption + removing the redundant text strip

A 4th `.eng-cap` at `right: 1.5%` (same alignment style as the old `03 Signal` caption: `align-items: flex-end; text-align: right`), reading `04 · Track` + a short tag (`"Win / loss, published"`, matching the existing tag style/length of the other 3).

The desktop `.ld-track` strip (added last session, showing `STEPS[3]`'s full sentence below the diagram) is removed at desktop widths — the diagram now carries this content directly, matching how `.ld-flow`'s full text list already gets superseded by in-diagram captions at desktop for zones 1–3. Mobile (`≤780px`) is unaffected: `.ld-flow`'s existing 4-item list (unchanged) is still the only rendering there, since `.eng-cap` (and now the new zone 4 markup) stays hidden below that breakpoint exactly like zones 1–3 already are.

## Testing plan

- Manual: verify all 4 captions render without overlapping the SVG content or each other, at both the diagram's min (`260px`) and max (`460px`) clamped heights.
- Manual: verify the 5 BUY/SELL signals each show a track record (with the correct win/loss color and %) in sync with their existing card, and that the 2 HOLD signals (MSFT, GOOGL) show no track record at all.
- Manual: verify the connecting line, card, and track record for a given signal never visually desync (same `dur`/`delay` driving all three).
- Manual: confirm mobile (`≤780px`) is pixel-identical to before this change — `.ld-flow`'s 4-item text list, unchanged.
- Manual: confirm the desktop `.ld-track` text strip is gone (superseded by the in-diagram zone 4).
