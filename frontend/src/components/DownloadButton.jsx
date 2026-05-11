import { useState, useEffect, useRef } from 'react'

/** Authed download button with format dropdown (XLSX / CSV / ZIP).
 *
 *  Builds the URL by appending `?format=...` to the supplied base `url`.
 *  Default format is xlsx (matches the legacy single-button behavior).
 *  Performs an authenticated fetch (Clerk Bearer token), reads the response
 *  as a blob, and triggers a native download via an injected <a download>.
 *
 *  Props:
 *    - url:              base export URL (e.g. "/api/signals/export"). DON'T
 *                        include the format suffix — this component appends ?format=.
 *                        For backward compat, URLs ending in ".xlsx" are also accepted
 *                        (the suffix is stripped before adding ?format=).
 *    - filenameFallback: filename without extension if server doesn't send Content-Disposition
 *    - label:            tooltip text + main button label (default "Download")
 */
export default function DownloadButton({ url, filenameFallback = 'export', label = 'Download' }) {
  const [busy, setBusy] = useState(false)
  const [err,  setErr]  = useState(null)
  const [open, setOpen] = useState(false)
  const menuRef = useRef(null)

  // Close dropdown on outside click
  useEffect(() => {
    if (!open) return
    function onDocClick(e) {
      if (menuRef.current && !menuRef.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [open])

  const FORMATS = [
    { key: 'xlsx', label: 'XLSX (Excel)', mediaType: ['spreadsheetml', 'octet-stream'] },
    { key: 'csv',  label: 'CSV',          mediaType: ['text/csv', 'text/plain'] },
    { key: 'zip',  label: 'ZIP (csv compressed)', mediaType: ['application/zip', 'octet-stream'] },
  ]

  async function download(format) {
    if (busy) return
    setBusy(true); setErr(null); setOpen(false)
    try {
      const token = await window.Clerk?.session?.getToken()
      // Strip any legacy .xlsx suffix on the URL so we always hit the canonical endpoint
      const baseUrl = url.replace(/\.xlsx(\?|$)/, '$1')
      const sep = baseUrl.includes('?') ? '&' : '?'
      const fullUrl = `${baseUrl}${sep}format=${format}`

      const res = await fetch(fullUrl, {
        headers: {
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'ngrok-skip-browser-warning': 'true',
        },
        cache: 'no-store',
      })
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)

      // Validate content-type — nginx serves an HTML maintenance page if the
      // backend errors out, which would otherwise be saved as a corrupt file.
      const ct = (res.headers.get('content-type') || '').toLowerCase()
      const fmtMeta = FORMATS.find(f => f.key === format)
      const ok = fmtMeta.mediaType.some(m => ct.includes(m))
      if (!ok) {
        throw new Error(`server returned ${ct || 'unknown content type'} — try again or narrow the date range`)
      }

      // Prefer the filename the server told us; fall back to the prop + format extension
      const cd = res.headers.get('content-disposition') || ''
      const m = /filename="([^"]+)"/.exec(cd)
      const downloadName = m ? m[1] : `${filenameFallback}.${format}`

      const blob = await res.blob()
      const blobUrl = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = blobUrl
      a.download = downloadName
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(blobUrl)
    } catch (e) {
      setErr(e.message || 'download failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <span ref={menuRef} style={{ position: 'relative', display: 'inline-flex' }}>
      {/* Single button — clicking ALWAYS opens the format menu, never auto-downloads.
          Forces the user to consciously pick xlsx / csv / zip. */}
      <button
        onClick={() => setOpen(o => !o)}
        disabled={busy}
        title={err ? `Failed: ${err}` : `${label} — pick a format`}
        aria-haspopup="menu"
        aria-expanded={open}
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 6,
        }}
      >
        <span aria-hidden="true">⬇</span>
        <span>{busy ? 'Preparing…' : (err ? 'Retry' : label)}</span>
        <span aria-hidden="true" style={{ marginLeft: 2, opacity: 0.7 }}>▾</span>
      </button>

      {open && (
        <div
          role="menu"
          style={{
            position: 'absolute',
            top: 'calc(100% + 4px)',
            right: 0,
            minWidth: 180,
            background: 'var(--bg, #1a1d2e)',
            border: '1px solid var(--border, rgba(255,255,255,0.12))',
            borderRadius: 6,
            padding: 4,
            zIndex: 100,
            boxShadow: '0 6px 20px rgba(0,0,0,0.35)',
          }}
        >
          {FORMATS.map(f => (
            <button
              key={f.key}
              role="menuitem"
              onClick={() => download(f.key)}
              style={{
                display: 'block',
                width: '100%',
                textAlign: 'left',
                padding: '8px 10px',
                background: 'transparent',
                border: 0,
                color: 'inherit',
                cursor: 'pointer',
                borderRadius: 4,
                fontSize: 13,
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = 'rgba(255,255,255,0.06)' }}
              onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent' }}
            >
              {f.label}
            </button>
          ))}
        </div>
      )}
    </span>
  )
}
