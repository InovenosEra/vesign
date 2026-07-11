import { test, expect, vi, beforeEach } from 'vitest'
import * as api from './api'

beforeEach(() => {
  global.fetch = vi.fn(() =>
    Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve({}) })
  )
})

const lastUrl = () => global.fetch.mock.calls.at(-1)[0]

test('market fetchers hit the right /api paths', async () => {
  await api.getIndices();              expect(lastUrl()).toBe('/api/market/indices')
  await api.getMovers('gainers', 5);   expect(lastUrl()).toBe('/api/market/movers?type=gainers&limit=5')
  await api.getBreadth();              expect(lastUrl()).toBe('/api/market/breadth')
  await api.getValuation(6);           expect(lastUrl()).toBe('/api/market/valuation?limit=6')
  await api.getSectors();              expect(lastUrl()).toBe('/api/market/sectors')
  await api.getSectorDetail('Information Technology')
  expect(lastUrl()).toBe('/api/market/sector/Information%20Technology')
  await api.getTape();                 expect(lastUrl()).toBe('/api/market/tape')
  await api.getTopNews(5);             expect(lastUrl()).toBe('/api/market/news/top?limit=5')
  await api.getTopAnalyst(1, 5);       expect(lastUrl()).toBe('/api/market/analyst-changes/top?days=1&limit=5')
  await api.getEarningsWeek();         expect(lastUrl()).toBe('/api/market/earnings/week?days=7')
  await api.getEconomicCal(7);         expect(lastUrl()).toBe('/api/market/economic-calendar?days=7')
})
