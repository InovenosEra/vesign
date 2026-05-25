/* Lets any redesign section open the shared SignalModal by ticker.
 * Provided by AppShell, consumed by Tape + every Market section. */
import { createContext, useContext } from 'react'

// Default null so consumers OUTSIDE the redesign (e.g. GlobalSearch on the old
// parked pages) can detect there's no redesign modal and fall back.
export const TickerModalContext = createContext(null)
export const useTickerModal = () => useContext(TickerModalContext) || (() => {})
