import { useState } from 'react'

/** Authed XLSX download. Performs an authenticated fetch (Clerk Bearer token
 *  from `frontend/src/api.js` token getter), reads the response as a blob,
 *  and triggers a native browser download via an injected <a download>. */
export default function DownloadXLSXButton({ url, filenameFallback = 'export', label = 'Download XLSX' }) {
  const [busy, setBusy] = useState(false)
  const [err,  setErr]  = useState(null)

  async function handleClick() {
    if (busy) return
    setBusy(true); setErr(null)
    try {
      const token = await window.Clerk?.session?.getToken()
      const res = await fetch(url, {
        headers: {
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          'ngrok-skip-browser-warning': 'true',
        },
        cache: 'no-store',
      })
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)

      // Validate content-type — nginx serves an HTML maintenance page with status
      // 200 if the backend errors out (502/503/504), which would otherwise be
      // saved as a corrupt .xlsx that Excel refuses to open.
      const ct = (res.headers.get('content-type') || '').toLowerCase()
      const isXlsx = ct.includes('spreadsheetml') || ct.includes('octet-stream')
      if (!isXlsx) {
        throw new Error(`server returned ${ct || 'unknown content type'} — try again or narrow the date range`)
      }

      // Prefer the filename the server told us; fall back to the prop.
      const cd = res.headers.get('content-disposition') || ''
      const m = /filename="([^"]+)"/.exec(cd)
      const downloadName = m ? m[1] : `${filenameFallback}.xlsx`

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
    <button
      onClick={handleClick}
      disabled={busy}
      title={err ? `Failed: ${err}` : label}
      style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}
    >
      <span aria-hidden="true">⬇</span>
      <span>{busy ? 'Preparing…' : (err ? 'Retry' : 'Download')}</span>
    </button>
  )
}
