import { useState, useMemo } from 'react'

/**
 * Client-side sort for small in-memory arrays.
 * Returns { sorted, sort, toggle } where toggle(col) cycles asc/desc.
 */
export function useSort(data, defaultKey = null, defaultDir = 'desc') {
  const [sort, setSort] = useState({ key: defaultKey, dir: defaultDir })

  const sorted = useMemo(() => {
    if (!data || !sort.key) return data ?? []
    return [...data].sort((a, b) => {
      const av = a[sort.key]
      const bv = b[sort.key]
      if (av == null && bv == null) return 0
      if (av == null) return 1
      if (bv == null) return -1
      const cmp = typeof av === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv))
      return sort.dir === 'asc' ? cmp : -cmp
    })
  }, [data, sort])

  function toggle(key) {
    setSort(s =>
      s.key === key
        ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' }
        : { key, dir: 'asc' }
    )
  }

  return { sorted, sort, toggle }
}
