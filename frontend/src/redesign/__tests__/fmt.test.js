import { test, expect } from 'vitest'
import { pct, num, dateFmt, ago, spark } from '../fmt'

test('pct adds sign and fixed decimals', () => {
  expect(pct(2)).toBe('+2.00%')
  expect(pct(-1.5)).toBe('-1.50%')
  expect(pct(0)).toBe('0.00%')
  expect(pct(null)).toBe('—')
  expect(pct(2, { fd: 1 })).toBe('+2.0%')
})

test('num formats with thousands + decimals, dash on null', () => {
  expect(num(1234.5)).toBe((1234.5).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }))
  expect(num(null)).toBe('—')
})

test('spark returns an SVG path, empty for <2 points', () => {
  expect(spark([1])).toBe('')
  expect(spark([])).toBe('')
  const d = spark([0, 5, 10])
  expect(d.startsWith('M')).toBe(true)
  expect(d).toContain('L')
})

test('dateFmt + ago handle empty input', () => {
  expect(dateFmt('')).toBe('')
  expect(ago('')).toBe('')
})
