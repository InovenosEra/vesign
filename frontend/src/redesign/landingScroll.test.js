import { describe, it, expect } from 'vitest'
import { easeOutCubic, clampIndex, nextIndex, nearestIndex, scrollYAt, SCROLL_DURATION_MS } from './landingScroll'

describe('easeOutCubic', () => {
  it('starts at 0 and ends at 1', () => {
    expect(easeOutCubic(0)).toBe(0)
    expect(easeOutCubic(1)).toBe(1)
  })
  it('is past the midpoint at t=0.5 (fast start, soft landing)', () => {
    expect(easeOutCubic(0.5)).toBeGreaterThan(0.5)
  })
})

describe('clampIndex', () => {
  it('clamps below zero to zero', () => {
    expect(clampIndex(-1, 5)).toBe(0)
  })
  it('clamps above the last index to the last index', () => {
    expect(clampIndex(9, 5)).toBe(4)
  })
  it('passes through in-range values', () => {
    expect(clampIndex(2, 5)).toBe(2)
  })
})

describe('nextIndex', () => {
  it('moves forward one step', () => {
    expect(nextIndex(1, 1, 5)).toBe(2)
  })
  it('moves backward one step', () => {
    expect(nextIndex(1, -1, 5)).toBe(0)
  })
  it('stays put at the last section when moving forward', () => {
    expect(nextIndex(4, 1, 5)).toBe(4)
  })
  it('stays put at the first section when moving backward', () => {
    expect(nextIndex(0, -1, 5)).toBe(0)
  })
})

describe('nearestIndex', () => {
  it('picks the section whose top is closest to scrollY', () => {
    expect(nearestIndex([0, 800, 1600, 2400], 1650)).toBe(2)
  })
  it('picks index 0 when scrollY is above every top', () => {
    expect(nearestIndex([0, 800, 1600], -50)).toBe(0)
  })
})

describe('scrollYAt', () => {
  it('returns the start position at elapsed=0', () => {
    expect(scrollYAt(0, 1000, 0)).toBe(0)
  })
  it('returns the exact target once elapsed reaches the duration', () => {
    expect(scrollYAt(0, 1000, SCROLL_DURATION_MS)).toBe(1000)
  })
  it('is partway there mid-animation', () => {
    const y = scrollYAt(0, 1000, SCROLL_DURATION_MS / 2)
    expect(y).toBeGreaterThan(0)
    expect(y).toBeLessThan(1000)
  })
})
