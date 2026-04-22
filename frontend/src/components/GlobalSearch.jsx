import { useState, useRef, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { searchTickers } from '../api'
import { useCurrency } from '../context/CurrencyContext'
import SignalModal from './SignalModal'

export default function GlobalSearch() {
  const { t } = useTranslation()
  const { fmtPrice } = useCurrency()
  const [query, setQuery]       = useState('')
  const [results, setResults]   = useState([])
  const [open, setOpen]         = useState(false)
  const [activeIdx, setActiveIdx] = useState(-1)
  const [selected, setSelected] = useState(null)
  const inputRef    = useRef(null)
  const dropdownRef = useRef(null)
  const debounceRef = useRef(null)

  function handleChange(e) {
    const val = e.target.value
    setQuery(val)
    setActiveIdx(-1)
    if (debounceRef.current) clearTimeout(debounceRef.current)
    if (!val.trim()) { setResults([]); setOpen(false); return }
    debounceRef.current = setTimeout(async () => {
      try {
        const data = await searchTickers(val.trim())
        setResults(data)
        setOpen(true)
      } catch {
        setResults([])
      }
    }, 300)
  }

  useEffect(() => {
    function onMouseDown(e) {
      if (
        dropdownRef.current && !dropdownRef.current.contains(e.target) &&
        inputRef.current    && !inputRef.current.contains(e.target)
      ) setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    return () => document.removeEventListener('mousedown', onMouseDown)
  }, [])

  function handleKeyDown(e) {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setOpen(true)
      setActiveIdx(i => Math.min(i + 1, results.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActiveIdx(i => Math.max(i - 1, 0))
    } else if (e.key === 'Enter' && activeIdx >= 0) {
      e.preventDefault()
      handleSelect(results[activeIdx])
    } else if (e.key === 'Escape') {
      setOpen(false)
      inputRef.current?.blur()
    }
  }

  function handleSelect(row) {
    setSelected(row)
    setOpen(false)
    setQuery('')
    setResults([])
  }

  return (
    <>
      <div style={{ position: 'relative' }}>
        <input
          ref={inputRef}
          value={query}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onFocus={() => results.length > 0 && setOpen(true)}
          placeholder={t('table.searchGlobal')}
          style={{
            width: 240,
            padding: '6px 14px',
            borderRadius: 20,
            border: '1px solid var(--border)',
            background: 'var(--surface)',
            color: 'var(--text)',
            fontSize: 13,
            outline: 'none',
            transition: 'border-color 0.15s',
          }}
          onFocusCapture={e => e.target.style.borderColor = 'var(--accent)'}
          onBlurCapture={e => e.target.style.borderColor = 'var(--border)'}
        />
        {open && results.length > 0 && (
          <div
            ref={dropdownRef}
            style={{
              position: 'absolute',
              top: 'calc(100% + 8px)',
              left: '50%',
              transform: 'translateX(-50%)',
              width: 340,
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              borderRadius: 10,
              zIndex: 1000,
              boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
              overflow: 'hidden',
            }}
          >
            {results.map((r, i) => (
              <div
                key={r.ticker}
                onMouseDown={() => handleSelect(r)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 10,
                  padding: '8px 14px',
                  cursor: 'pointer',
                  background: i === activeIdx ? 'rgba(79,142,247,0.15)' : 'transparent',
                  borderBottom: i < results.length - 1 ? '1px solid var(--border)' : 'none',
                  transition: 'background 0.1s',
                }}
                onMouseEnter={() => setActiveIdx(i)}
                onMouseLeave={() => setActiveIdx(-1)}
              >
                {r.logo_url
                  ? <img src={r.logo_url} alt="" style={{ width: 30, height: 30, borderRadius: 6, objectFit: 'contain', flexShrink: 0 }} onError={e => e.target.style.display = 'none'} />
                  : <div style={{ width: 30, height: 30, borderRadius: 6, background: 'var(--border)', flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 9, fontWeight: 'bold', color: 'var(--muted)' }}>{r.ticker.slice(0, 4)}</div>
                }
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 700, fontSize: 13 }}>{r.ticker}</div>
                  <div style={{ fontSize: 11, color: 'var(--muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.company}</div>
                </div>
                {r.signal && (
                  <span className={`badge badge-${r.signal}`} style={{ flexShrink: 0, fontSize: 10 }}>{r.signal}</span>
                )}
                {r.close != null && (
                  <span style={{ fontSize: 12, color: 'var(--muted)', flexShrink: 0 }}>{fmtPrice(r.close)}</span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
      {selected && <SignalModal row={selected} onClose={() => setSelected(null)} />}
    </>
  )
}
