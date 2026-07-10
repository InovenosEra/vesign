import { describe, it, expect } from 'vitest'
import { FAQ_IDS, generateFaqJsonLd } from './faqData'

describe('faqData', () => {
  it('FAQ_IDS lists 5 stable, unique identifiers', () => {
    expect(FAQ_IDS).toHaveLength(5)
    expect(new Set(FAQ_IDS).size).toBe(5)
  })

  describe('generateFaqJsonLd', () => {
    const faqs = [
      { q: 'Is this investment advice?', a: 'No, it is research only.' },
      { q: 'Is there a free plan?', a: 'Yes.' },
    ]

    it('produces a valid schema.org FAQPage shape', () => {
      const jsonLd = generateFaqJsonLd(faqs)
      expect(jsonLd['@context']).toBe('https://schema.org')
      expect(jsonLd['@type']).toBe('FAQPage')
      expect(jsonLd.mainEntity).toHaveLength(2)
    })

    it('maps each faq to a Question/Answer pair using the exact rendered text', () => {
      const jsonLd = generateFaqJsonLd(faqs)
      const [first] = jsonLd.mainEntity
      expect(first['@type']).toBe('Question')
      expect(first.name).toBe('Is this investment advice?')
      expect(first.acceptedAnswer['@type']).toBe('Answer')
      expect(first.acceptedAnswer.text).toBe('No, it is research only.')
    })

    it('is pure/serializable — round-trips through JSON.stringify unchanged', () => {
      const jsonLd = generateFaqJsonLd(faqs)
      expect(JSON.parse(JSON.stringify(jsonLd))).toEqual(jsonLd)
    })

    it('returns an empty mainEntity for an empty faq list rather than throwing', () => {
      expect(generateFaqJsonLd([]).mainEntity).toEqual([])
    })
  })
})
