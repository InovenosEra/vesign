// holdingForm.js — pure validation for the add-holding form.
export function validateHolding({ ticker, shares, price, date }) {
  if (!ticker || !String(ticker).trim()) return 'Pick a ticker'
  if (!(Number(shares) > 0)) return 'Shares must be greater than 0'
  if (price === '' || price == null || !(Number(price) >= 0)) return 'Enter a valid price'
  if (!date) return 'Pick a buy date'
  if (date > new Date().toISOString().slice(0, 10)) return 'Buy date cannot be in the future'
  return null
}
