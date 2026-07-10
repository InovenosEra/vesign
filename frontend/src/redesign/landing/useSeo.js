import { useEffect } from 'react'

function setMeta(selector, attr, content) {
  let el = document.querySelector(selector)
  if (!el) {
    el = document.createElement('meta')
    const [, attrName, attrValue] = selector.match(/\[([a-z]+)="([^"]+)"\]/) || []
    if (attrName) el.setAttribute(attrName, attrValue)
    document.head.appendChild(el)
  }
  const prev = el.getAttribute(attr)
  el.setAttribute(attr, content)
  return prev
}

// Sets document.title + description/OG/Twitter meta while a page is
// mounted, restoring whatever index.html shipped as soon as it unmounts —
// plain DOM, no head-management library. LandingPage is currently the only
// caller; safe for any future page to reuse the same way.
export function useSeo({ title, description }) {
  useEffect(() => {
    if (!title && !description) return
    const prevTitle = document.title
    const restores = []

    if (title) {
      document.title = title
      restores.push(() => { document.title = prevTitle })
    }
    if (description) {
      restores.push((prev => () => setMeta('meta[name="description"]', 'content', prev))(
        setMeta('meta[name="description"]', 'content', description)
      ))
      restores.push((prev => () => setMeta('meta[property="og:description"]', 'content', prev))(
        setMeta('meta[property="og:description"]', 'content', description)
      ))
      restores.push((prev => () => setMeta('meta[name="twitter:description"]', 'content', prev))(
        setMeta('meta[name="twitter:description"]', 'content', description)
      ))
    }
    if (title) {
      restores.push((prev => () => setMeta('meta[property="og:title"]', 'content', prev))(
        setMeta('meta[property="og:title"]', 'content', title)
      ))
      restores.push((prev => () => setMeta('meta[name="twitter:title"]', 'content', prev))(
        setMeta('meta[name="twitter:title"]', 'content', title)
      ))
    }

    return () => restores.forEach((fn) => fn())
  }, [title, description])
}
