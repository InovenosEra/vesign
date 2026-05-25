/* Lets any redesign section open the shared SignalModal by ticker.
 * Provided by AppShell, consumed by Tape + every Market section. */
import { createContext, useContext } from 'react'

export const TickerModalContext = createContext(() => {})
export const useTickerModal = () => useContext(TickerModalContext)
