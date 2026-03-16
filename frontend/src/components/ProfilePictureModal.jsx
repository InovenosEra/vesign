import { useState, useRef } from 'react'
import { useUser } from '@clerk/react'

const UNSPLASH_KEY = import.meta.env.VITE_UNSPLASH_ACCESS_KEY

export default function ProfilePictureModal({ onClose }) {
  const { user } = useUser()
  const [tab, setTab] = useState('upload')   // 'upload' | 'search'
  const [query, setQuery] = useState('')
  const [results, setResults] = useState([])
  const [searching, setSearching] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)
  const [preview, setPreview] = useState(null)   // { url, file } for upload tab
  const fileRef = useRef(null)

  async function handleFileChange(e) {
    const file = e.target.files?.[0]
    if (!file) return
    setPreview({ url: URL.createObjectURL(file), file })
    setError(null)
  }

  async function handleSearch(e) {
    e.preventDefault()
    if (!query.trim()) return
    setSearching(true)
    setError(null)
    try {
      const res = await fetch(
        `https://api.unsplash.com/search/photos?query=${encodeURIComponent(query)}&per_page=15&orientation=squarish&client_id=${UNSPLASH_KEY}`
      )
      const data = await res.json()
      setResults(data.results || [])
      if ((data.results || []).length === 0) setError('No results found.')
    } catch {
      setError('Search failed.')
    } finally {
      setSearching(false)
    }
  }

  async function applyUpload() {
    if (!preview?.file) return
    setSaving(true)
    setError(null)
    try {
      await user.setProfileImage({ file: preview.file })
      onClose()
    } catch (e) {
      setError(e.message || 'Failed to save.')
    } finally {
      setSaving(false)
    }
  }

  async function applyUnsplash(imgUrl) {
    setSaving(true)
    setError(null)
    try {
      const res = await fetch(imgUrl)
      const blob = await res.blob()
      const file = new File([blob], 'profile.jpg', { type: blob.type })
      await user.setProfileImage({ file })
      onClose()
    } catch (e) {
      setError(e.message || 'Failed to save.')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-box" style={{ maxWidth: 540, width: '90vw' }} onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
          <span style={{ fontSize: 16, fontWeight: 700 }}>Edit Profile Picture</span>
          <button className="modal-close" onClick={onClose} style={{ position: 'static' }}>✕</button>
        </div>

        {/* Tabs */}
        <div style={{ display: 'flex', gap: 4, marginBottom: 20, borderBottom: '1px solid var(--border)', paddingBottom: 0 }}>
          {['upload', 'search'].map(t => (
            <button
              key={t}
              onClick={() => { setTab(t); setError(null) }}
              style={{
                padding: '8px 18px', fontSize: 13, fontWeight: tab === t ? 700 : 400,
                background: 'transparent', border: 'none', cursor: 'pointer',
                color: tab === t ? 'var(--accent)' : 'var(--muted)',
                borderBottom: tab === t ? '2px solid var(--accent)' : '2px solid transparent',
                marginBottom: -1,
              }}
            >{t === 'upload' ? 'Upload from Computer' : 'Search Online'}</button>
          ))}
        </div>

        {/* Upload tab */}
        {tab === 'upload' && (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16 }}>
            {preview
              ? <img src={preview.url} alt="" style={{ width: 120, height: 120, borderRadius: '50%', objectFit: 'cover', border: '2px solid var(--accent)' }} />
              : user?.imageUrl
                ? <img src={user.imageUrl} alt="" style={{ width: 120, height: 120, borderRadius: '50%', objectFit: 'cover', border: '2px solid var(--border)' }} />
                : <div style={{ width: 120, height: 120, borderRadius: '50%', background: 'var(--surface)', border: '2px dashed var(--border)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--muted)', fontSize: 13 }}>No image</div>
            }
            <input ref={fileRef} type="file" accept="image/*" style={{ display: 'none' }} onChange={handleFileChange} />
            <button onClick={() => fileRef.current?.click()} style={{ padding: '8px 20px', fontSize: 13 }}>
              Choose Image…
            </button>
            {preview && (
              <button className="primary" onClick={applyUpload} disabled={saving} style={{ padding: '8px 28px' }}>
                {saving ? 'Saving…' : 'Save'}
              </button>
            )}
          </div>
        )}

        {/* Search tab */}
        {tab === 'search' && (
          <div>
            <form onSubmit={handleSearch} style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
              <input
                placeholder="Search for an image (e.g. lion, mountain, abstract…)"
                value={query}
                onChange={e => setQuery(e.target.value)}
                style={{ flex: 1 }}
                autoFocus
              />
              <button className="primary" type="submit" disabled={searching || !query.trim()}>
                {searching ? '…' : 'Search'}
              </button>
            </form>

            {results.length > 0 && (
              <div style={{
                display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 8,
                maxHeight: 300, overflowY: 'auto',
              }}>
                {results.map(img => (
                  <div key={img.id} style={{ position: 'relative', cursor: 'pointer' }}
                    onClick={() => !saving && applyUnsplash(img.urls.regular)}>
                    <img
                      src={img.urls.thumb}
                      alt={img.alt_description}
                      style={{ width: '100%', aspectRatio: '1', objectFit: 'cover', borderRadius: 6, display: 'block',
                        opacity: saving ? 0.5 : 1, transition: 'opacity 0.15s' }}
                    />
                    {saving && (
                      <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center',
                        background: 'rgba(0,0,0,0.4)', borderRadius: 6, fontSize: 11, color: '#fff' }}>Saving…</div>
                    )}
                  </div>
                ))}
              </div>
            )}
            {!UNSPLASH_KEY && (
              <p style={{ color: 'var(--red)', fontSize: 12 }}>
                Missing VITE_UNSPLASH_ACCESS_KEY in .env.local
              </p>
            )}
          </div>
        )}

        {error && <p style={{ color: 'var(--red, #e74c3c)', fontSize: 12, marginTop: 12, textAlign: 'center' }}>{error}</p>}
      </div>
    </div>
  )
}
