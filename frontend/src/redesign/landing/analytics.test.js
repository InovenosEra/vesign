import { describe, it, expect, afterEach, vi } from 'vitest'
import { track } from './analytics'

describe('analytics track()', () => {
  const originalWindow = globalThis.window
  afterEach(() => {
    if (originalWindow === undefined) delete globalThis.window
    else globalThis.window = originalWindow
  })

  it('is a safe no-op when window is undefined (node/SSR/tests)', () => {
    delete globalThis.window
    expect(() => track('landing_view', {})).not.toThrow()
  })

  it('is a safe no-op when window.gtag is not a function (ad blockers)', () => {
    globalThis.window = {}
    expect(() => track('landing_view', {})).not.toThrow()
  })

  it('calls window.gtag("event", name, params) when gtag is present', () => {
    const gtag = vi.fn()
    globalThis.window = { gtag }
    track('landing_cta_click', { location: 'hero', label: 'sign_up' })
    expect(gtag).toHaveBeenCalledWith('event', 'landing_cta_click', { location: 'hero', label: 'sign_up' })
  })

  it('defaults params to an empty object', () => {
    const gtag = vi.fn()
    globalThis.window = { gtag }
    track('landing_view')
    expect(gtag).toHaveBeenCalledWith('event', 'landing_view', {})
  })
})
