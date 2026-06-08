/* Account page — React port of the account-v1.html mockup. Renders inside
 * <AppShell> (.rd scope). Sidebar nav switches panes client-side.
 *
 * Wired to REAL data where it exists: identity (Clerk), currency (useCurrency),
 * language (i18n), plan + wallet balance (useMe), password/sign-out (Clerk).
 * Billing, payment, API keys, integrations, 2FA, sessions, notifications and
 * trading toggles are mock UI (no backend yet) — interactive but not persisted. */
import { useState } from 'react'
import { NavLink } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import { useUser, useClerk } from '@clerk/react'
import { useCurrency } from '../context/CurrencyContext'
import { useMe } from '../context/MeContext'
import { fmtCents } from './signals/gating'
import { LOGO } from './fmt'
import './account.css'

const PLAN_LABELS = { free: 'Free', pro: 'Pro', max: 'Max', pro_plus: 'Pro+' }

const ICONS = {
  profile: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="8" r="4" /><path d="M20 21a8 8 0 10-16 0" /></svg>,
  plan: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="2" y="6" width="20" height="12" rx="2" /><path d="M2 11h20" /></svg>,
  wallet: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M21 12V7a2 2 0 00-2-2H5a2 2 0 100 4h14a2 2 0 012 2zm0 0v5a2 2 0 01-2 2H5a2 2 0 01-2-2V7" /><circle cx="17" cy="13" r="1.4" fill="currentColor" /></svg>,
  notifications: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 8a6 6 0 10-12 0c0 7-3 9-3 9h18s-3-2-3-9 M13.7 21a2 2 0 01-3.4 0" /></svg>,
  trading: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 3v18h18 M7 14l4-4 4 4 5-5" /></svg>,
  security: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 2l9 4v6c0 5.5-3.8 10.5-9 12-5.2-1.5-9-6.5-9-12V6l9-4z" /></svg>,
  api: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M16 18l6-6-6-6 M8 6l-6 6 6 6 M14 4l-4 16" /></svg>,
  data: <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M3 5v6c0 1.7 4 3 9 3s9-1.3 9-3V5 M3 11v6c0 1.7 4 3 9 3s9-1.3 9-3v-6" /></svg>,
}

const NAV = [
  { id: 'profile', label: 'Profile' },
  { id: 'plan', label: 'Plan & billing' },
  { id: 'wallet', label: 'Wallet' },
  { id: 'notifications', label: 'Notifications' },
  { id: 'trading', label: 'Trading preferences' },
  { id: 'security', label: 'Security' },
  { id: 'api', label: 'API & integrations', soon: true },
  { id: 'data', label: 'Data & privacy' },
]

/* ---- small interactive primitives (local state only) ---- */
function Toggle({ defaultOn = false }) {
  const [on, setOn] = useState(defaultOn)
  return (
    <label className="toggle">
      <input type="checkbox" checked={on} onChange={e => setOn(e.target.checked)} />
      <span className="slider" />
    </label>
  )
}
function Range({ min, max, value, fmt }) {
  const [v, setV] = useState(value)
  return (
    <div className="range-input">
      <input type="range" min={min} max={max} value={v} onChange={e => setV(+e.target.value)} />
      <span className="range-display">{fmt(v)}</span>
    </div>
  )
}
function Card({ title, hint, badge, children, bodyPad }) {
  return (
    <div className="setting-card">
      <div className="setting-card-head">
        <h3>{title}</h3>
        {hint && <span className="hint">{hint}</span>}
        {badge && <span className={'badge' + (badge.cls ? ' ' + badge.cls : '')}>{badge.label}</span>}
      </div>
      <div className="setting-card-body" style={bodyPad}>{children}</div>
    </div>
  )
}

/* ---- panes ---- */
function ProfilePane({ user, openUserProfile, currency, setCurrency, i18n }) {
  return (
    <>
      <div className="acc-pane-head"><h2>Profile</h2><span className="sub">Personal information &amp; display preferences</span></div>
      <Card title="Identity">
        <div className="field-row">
          <div className="field-label">Display name<small>Shown on the platform and in exports</small></div>
          <div className="field-value"><input className="input medium" defaultValue={user?.fullName || ''} /></div>
          <button className="btn sm" onClick={() => openUserProfile()}>Update</button>
        </div>
        <div className="field-row">
          <div className="field-label">Email address<small>Used for sign-in and signal alerts</small></div>
          <div className="field-value">
            <input className="input medium" defaultValue={user?.primaryEmailAddress?.emailAddress || ''} readOnly />
            {user?.primaryEmailAddress?.verification?.status === 'verified' && <span className="verified-tag">✓ Verified</span>}
          </div>
          <button className="btn sm" onClick={() => openUserProfile()}>Change</button>
        </div>
        <div className="field-row">
          <div className="field-label">Phone (optional)<small>For SMS alerts on critical signals</small></div>
          <div className="field-value"><input className="input medium" placeholder="+972 50 123 4567" /></div>
          <button className="btn sm" onClick={() => openUserProfile()}>Add</button>
        </div>
      </Card>
      <Card title="Display">
        <div className="field-row">
          <div className="field-label">Currency<small>Used across the platform for monetary values</small></div>
          <div className="field-value">
            <select className="acc-select" value={currency} onChange={e => setCurrency(e.target.value)}>
              <option value="USD">USD — US Dollar</option>
              <option value="EUR">EUR — Euro</option>
              <option value="ILS">ILS — Israeli Shekel</option>
            </select>
          </div>
          <span />
        </div>
        <div className="field-row">
          <div className="field-label">Timezone<small>Tickers and signals are timestamped in this zone</small></div>
          <div className="field-value">
            <select className="acc-select" defaultValue="il"><option value="il">Asia/Jerusalem · IDT (UTC+3)</option><option value="ny">America/New_York · EDT (UTC−4)</option><option value="utc">UTC</option></select>
          </div>
          <span />
        </div>
        <div className="field-row">
          <div className="field-label">Language<small>Interface language</small></div>
          <div className="field-value">
            <select className="acc-select" value={(i18n.language || 'en').slice(0, 2)}
              onChange={e => { i18n.changeLanguage(e.target.value); try { localStorage.setItem('lang', e.target.value) } catch { /* ignore */ } }}>
              <option value="en">English (US)</option>
              <option value="he">עברית (Hebrew)</option>
              <option value="es">Español</option>
              <option value="fr">Français</option>
              <option value="de">Deutsch</option>
              <option value="it">Italiano</option>
            </select>
          </div>
          <span />
        </div>
        <div className="field-row">
          <div className="field-label">Theme<small>Dark by default · light mode coming soon</small></div>
          <div className="field-value">
            <select className="acc-select" defaultValue="dark"><option value="dark">Dark</option><option value="light" disabled>Light (soon)</option></select>
          </div>
          <span />
        </div>
      </Card>
    </>
  )
}

function PlanPane({ planLabel }) {
  const INVOICES = [
    ['May 19, 2026', 'INV-2026-05-1934'], ['Apr 19, 2026', 'INV-2026-04-1934'],
    ['Mar 19, 2026', 'INV-2026-03-1934'], ['Feb 19, 2026', 'INV-2026-02-1934'],
    ['Jan 19, 2026', 'INV-2026-01-1934'], ['Dec 19, 2025', 'INV-2025-12-1934'],
  ]
  return (
    <>
      <div className="acc-pane-head"><h2>Plan &amp; billing</h2><span className="sub">Manage your subscription &amp; payment method</span></div>
      <div className="plan-card">
        <div className="info">
          <span className="tier">★ Current plan</span>
          <div className="name">{planLabel}</div>
          <div className="desc">Unlimited signals · Real-time data · ML predictions · 5 watchlists · API access · Priority support</div>
          <div className="meta">
            <span><span className="v">Renews</span> Jun 19, 2026</span>
            <span><span className="v">Billed</span> Monthly</span>
            <span><span className="v">Member since</span> Aug 14, 2024</span>
          </div>
        </div>
        <div className="price">
          <div className="amt"><span className="s">$</span>15</div>
          <div className="unit">per month</div>
          <div className="actions">
            <NavLink className="btn sm" to="/market">Change plan</NavLink>
            <button className="btn sm danger">Cancel</button>
          </div>
        </div>
      </div>
      <Card title="Payment method" hint="Updated 4 months ago">
        <div className="field-row">
          <div className="field-label">Card on file<small>Charged on the 19th of each month</small></div>
          <div className="field-value">
            <svg width="36" height="24" viewBox="0 0 36 24" fill="none"><rect x="0.5" y="0.5" width="35" height="23" rx="3.5" stroke="rgba(255,255,255,0.12)" /><circle cx="14" cy="12" r="6" fill="#eb001b" opacity="0.85" /><circle cx="22" cy="12" r="6" fill="#f79e1b" opacity="0.85" /></svg>
            <span style={{ fontFamily: 'var(--mono)', fontSize: 12.5, color: 'var(--ink)' }}>Mastercard •••• 4831</span>
            <span style={{ fontFamily: 'var(--mono)', fontSize: 11, color: 'var(--ink-3)' }}>Exp 03 / 27</span>
          </div>
          <button className="btn sm">Update</button>
        </div>
      </Card>
      <div className="setting-card">
        <div className="setting-card-head"><h3>Billing history</h3><span className="hint">Last 6 invoices</span></div>
        <div className="setting-card-body" style={{ padding: 0 }}>
          <table className="invoices-table">
            <thead><tr><th>Date</th><th>Invoice</th><th>Amount</th><th>Status</th><th className="r">Download</th></tr></thead>
            <tbody>
              {INVOICES.map(([d, inv]) => (
                <tr key={inv}><td>{d}</td><td>{inv}</td><td>$15.00</td><td><span className="status-pill paid">Paid</span></td><td className="r"><span className="dl">PDF ↓</span></td></tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </>
  )
}

function WalletPane({ balanceCents, unlockCents }) {
  const unlocks = unlockCents ? Math.floor(balanceCents / unlockCents) : 0
  const TXN = [
    { ico: 'unlock', ttl: 'BUY signal unlocked', tk: 'MTD', sub: 'Mettler-Toledo · VQS 9', date: 'May 19, 14:32', amt: '−$0.10', cls: 'debit', run: '$8.40' },
    { ico: 'unlock', ttl: 'BUY signal unlocked', tk: 'YELP', sub: 'Yelp · VQS 9', date: 'May 19, 14:32', amt: '−$0.10', cls: 'debit', run: '$8.50' },
    { ico: 'deep', ttl: 'Deep-dive unlocked', tk: 'NVDA', sub: 'NVIDIA · full report', date: 'May 18, 09:14', amt: '−$0.50', cls: 'debit', run: '$8.60' },
    { ico: 'unlock', ttl: 'BUY signal unlocked', tk: 'PANW', sub: 'Palo Alto Networks · VQS 8', date: 'May 16, 11:08', amt: '−$0.10', cls: 'debit', run: '$9.10' },
    { ico: 'topup', ttl: 'Auto-reload', tk: null, sub: 'Mastercard •••• 4831', date: 'May 12, 09:00', amt: '+$10.00', cls: 'credit', run: '$9.30' },
  ]
  const TopupIco = <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round"><path d="M12 5v14M5 12h14" /></svg>
  const UnlockIco = <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="3" y="11" width="18" height="11" rx="2" /><path d="M7 11V7a5 5 0 0110 0v4" /></svg>
  const DeepIco = <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="11" cy="11" r="7" /><path d="M21 21l-4.35-4.35" /></svg>
  const icoFor = (k) => k === 'topup' ? TopupIco : k === 'deep' ? DeepIco : UnlockIco
  return (
    <>
      <div className="acc-pane-head"><h2>Wallet</h2><span className="sub">Top up · pay-per-signal credit · transaction history</span></div>
      <div className="wallet-hero">
        <div className="info">
          <div className="label"><span className="dot" /> Available balance</div>
          <div className="balance"><span className="s">$</span>{(balanceCents / 100).toFixed(2)} <small>≈ {unlocks} BUY unlocks</small></div>
          <div className="meta">Pay-per-signal credit · {fmtCents(unlockCents)} per BUY unlock</div>
        </div>
        <div className="actions">
          <button className="btn-topup">{TopupIco} Top up wallet</button>
          <a className="btn-history">View full history →</a>
        </div>
      </div>
      <div className="setting-card">
        <div className="setting-card-head"><h3>Quick top-up</h3><span className="hint">Paid with your card on file · Mastercard •••• 4831</span></div>
        <div className="setting-card-body" style={{ padding: '0 22px 16px' }}>
          <div className="quick-topup">
            {[['5', '50'], ['10', '100', true], ['25', '250'], ['50', '500']].map(([a, u, rec]) => (
              <div key={a} className={'amt-pill' + (rec ? ' recommended' : '')}>
                <div className="amt"><span className="s">$</span>{a}</div>
                <div className="sig-est">{u} unlocks</div>
              </div>
            ))}
            <div className="custom-amt"><span className="s">$</span><input type="text" placeholder="Custom" /></div>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', paddingTop: 4 }}>
            <span style={{ fontFamily: 'var(--mono)', fontSize: 11, color: 'var(--ink-3)' }}>No fees · Refundable · 5% bonus on $50+ top-ups</span>
            <button className="btn primary sm">Top up $10</button>
          </div>
        </div>
      </div>
      <Card title="Auto-reload" badge={{ label: 'On' }}>
        <div className="field-row">
          <div className="field-label">Auto top-up when balance falls below<small>We'll never let you miss a signal because of an empty wallet</small></div>
          <div className="field-value"><div className="spending-cap"><div className="input-wrap"><span className="s">$</span><input type="text" defaultValue="2.00" /></div></div></div>
          <Toggle defaultOn />
        </div>
        <div className="field-row">
          <div className="field-label">Reload amount<small>Charged to your card on file</small></div>
          <div className="field-value"><select className="acc-select" defaultValue="10"><option value="10">$10.00</option><option value="5">$5.00</option><option value="25">$25.00</option><option value="50">$50.00</option></select></div>
          <span />
        </div>
        <div className="field-row">
          <div className="field-label">Monthly spending cap<small>Hard limit · no spend above this</small></div>
          <div className="field-value"><div className="spending-cap"><div className="input-wrap"><span className="s">$</span><input type="text" defaultValue="20.00" /></div><span className="per">/ month</span></div></div>
          <Toggle defaultOn />
        </div>
      </Card>
      <div className="setting-card">
        <div className="setting-card-head"><h3>Recent activity</h3><span className="hint">Last 30 days</span></div>
        <div className="setting-card-body" style={{ padding: 0 }}>
          <table className="txn-table">
            <thead><tr><th>Activity</th><th>Ticker</th><th>Date</th><th className="r">Amount</th><th className="r">Balance</th></tr></thead>
            <tbody>
              {TXN.map((t, i) => (
                <tr key={i}>
                  <td><div className="txn-cell"><div className={'txn-ico ' + t.ico}>{icoFor(t.ico)}</div><div className="info"><div className="ttl">{t.ttl}</div><div className="sub">{t.tk && <span className="tk">{t.tk}</span>}{t.sub}</div></div></div></td>
                  <td>{t.tk ? <div className="ticker-mini"><img src={LOGO(t.tk)} alt="" /></div> : '—'}</td>
                  <td>{t.date}</td>
                  <td className={'r amt-cell ' + t.cls}>{t.amt}</td>
                  <td className="r running">{t.run}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="show-more"><a>Show all transactions →</a></div>
        </div>
      </div>
    </>
  )
}

function NotificationsPane() {
  const sig = [
    ['New BUY signals', 'Every BUY that passes your filters', true],
    ['New SELL signals', 'Stops triggered or take-profit fired', true],
    ['Daily summary email', 'End-of-day digest at 16:30 ET', false],
    ['Weekly performance report', 'Sundays · portfolio + Vesign benchmark', true],
  ]
  const act = [
    ['Watchlist big moves', 'Ticker moves > 5% in a day', true],
    ['Earnings reminders', 'Day-before alert for watchlist tickers', true],
    ['Analyst upgrades / downgrades', 'For tickers you hold', false],
    ['Product & release news', 'New features, model updates, blog posts', true],
  ]
  const rows = (arr) => arr.map(([l, s, on]) => (
    <div className="field-row" key={l}>
      <div className="field-label">{l}<small>{s}</small></div>
      <span />
      <Toggle defaultOn={on} />
    </div>
  ))
  return (
    <>
      <div className="acc-pane-head"><h2>Notifications</h2><span className="sub">Choose when and how Vesign reaches out</span></div>
      <Card title="Signal alerts" hint="Emailed within 60s of signal fire">{rows(sig)}</Card>
      <Card title="Price & activity alerts">{rows(act)}</Card>
    </>
  )
}

function TradingPane() {
  return (
    <>
      <div className="acc-pane-head"><h2>Trading preferences</h2><span className="sub">Defaults applied to your signals &amp; portfolio simulation</span></div>
      <Card title="Position sizing">
        <div className="field-row">
          <div className="field-label">Default position size<small>Used to simulate yield on $/trade basis</small></div>
          <div className="field-value"><div className="input-group"><span style={{ paddingLeft: 12, fontFamily: 'var(--mono)', color: 'var(--ink-3)' }}>$</span><input className="input mono short" defaultValue="1,000" style={{ textAlign: 'right' }} /></div></div>
          <button className="btn sm">Update</button>
        </div>
        <div className="field-row">
          <div className="field-label">Max single-position size<small>As % of total portfolio</small></div>
          <div className="field-value"><Range min={2} max={20} value={10} fmt={v => `${v}%`} /></div>
          <span />
        </div>
      </Card>
      <Card title="Risk rules">
        <div className="field-row">
          <div className="field-label">Trailing stop %<small>Triggers SELL when price falls this much from peak</small></div>
          <div className="field-value"><Range min={10} max={50} value={25} fmt={v => `${v}%`} /></div>
          <span />
        </div>
        <div className="field-row">
          <div className="field-label">Min VQS score<small>Filter out signals below this conviction</small></div>
          <div className="field-value"><Range min={1} max={10} value={6} fmt={v => `${v} / 10`} /></div>
          <span />
        </div>
        <div className="field-row">
          <div className="field-label">Min predicted upside<small>Require this gap to analyst mean target</small></div>
          <div className="field-value"><Range min={0} max={50} value={15} fmt={v => `+${v}%`} /></div>
          <span />
        </div>
        <div className="field-row">
          <div className="field-label">Default watchlist<small>Where new BUY signals get tagged</small></div>
          <div className="field-value"><select className="acc-select" defaultValue="core"><option value="core">Core Tech</option><option value="growth">Growth</option><option value="dividend">Dividend</option><option value="income">Income</option><option value="ml">ML Picks</option></select></div>
          <span />
        </div>
      </Card>
    </>
  )
}

function SecurityPane({ openUserProfile }) {
  return (
    <>
      <div className="acc-pane-head"><h2>Security</h2><span className="sub">Password, two-factor authentication, and sessions</span></div>
      <Card title="Password" hint="Manage via your account portal">
        <div className="field-row">
          <div className="field-label">Password<small>Change your sign-in password</small></div>
          <div className="field-value"><span style={{ fontFamily: 'var(--mono)', fontSize: 12, color: 'var(--ink-2)' }}>••••••••••••</span></div>
          <button className="btn sm primary" onClick={() => openUserProfile()}>Change password</button>
        </div>
      </Card>
      <Card title="Two-factor authentication" badge={{ label: 'Enabled' }}>
        <div className="field-row">
          <div className="field-label">Authenticator app<small>Time-based one-time password (TOTP)</small></div>
          <div className="field-value"><span style={{ fontFamily: 'var(--mono)', fontSize: 12, color: 'var(--ink-2)' }}>Configured · 2024-09-12</span></div>
          <button className="btn sm" onClick={() => openUserProfile()}>Reconfigure</button>
        </div>
        <div className="field-row">
          <div className="field-label">Backup codes<small>Used if you lose your authenticator</small></div>
          <div className="field-value"><span style={{ fontFamily: 'var(--mono)', fontSize: 12, color: 'var(--ink-3)' }}>8 of 10 unused</span></div>
          <button className="btn sm" onClick={() => openUserProfile()}>Generate new</button>
        </div>
      </Card>
      <div className="setting-card">
        <div className="setting-card-head"><h3>Active sessions</h3><span className="hint">Manage in account portal</span></div>
        <div className="setting-card-body" style={{ padding: 0 }}>
          <div className="session-row">
            <div className="ico"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="2" y="4" width="20" height="14" rx="2" /><path d="M2 18l3 4h14l3-4" /></svg></div>
            <div className="info"><div className="where">This device <span className="current">Current</span></div><div className="meta">Active now</div></div>
            <span />
          </div>
        </div>
      </div>
    </>
  )
}

function ApiPane() {
  return (
    <>
      <div className="acc-pane-head"><h2>API &amp; integrations</h2><span className="sub">Programmatic access &amp; third-party connections</span></div>
      <div className="setting-card">
        <div className="setting-card-head"><h3>API key</h3><span className="hint">Keep this secret — anyone with the key can read your data</span></div>
        <div className="setting-card-body" style={{ padding: '18px 22px 20px' }}>
          <div className="api-key-row">
            <div className="key">vk_live_2NzJqP9aR4mF••••••••••••3oXn8eW5vT</div>
            <button className="reveal-btn"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" /><circle cx="12" cy="12" r="3" /></svg>Show</button>
            <button className="copy-btn"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="9" y="9" width="13" height="13" rx="2" /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" /></svg>Copy</button>
          </div>
          <div style={{ marginTop: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span style={{ fontFamily: 'var(--mono)', fontSize: 11, color: 'var(--ink-3)' }}>Created Sep 12, 2024 · 14,832 requests this month · 73% of 20k quota used</span>
            <button className="btn sm danger">Regenerate</button>
          </div>
        </div>
      </div>
      <Card title="Integrations">
        <div className="int-grid">
          <div className="int-card"><div className="head"><div className="ico slack">#</div><div className="nm">Slack</div><div className="status on">Connected</div></div><div className="desc">Receive signal alerts in your team channel. Posting to <code style={{ color: 'var(--ink-2)' }}>#vesign-signals</code></div></div>
          <div className="int-card"><div className="head"><div className="ico discord">◐</div><div className="nm">Discord</div><div className="status">Connect</div></div><div className="desc">Send Vesign signals to a Discord webhook or bot in your server.</div></div>
          <div className="int-card"><div className="head"><div className="ico webhook">{'{ }'}</div><div className="nm">Webhooks</div><div className="status on">2 active</div></div><div className="desc">POST signals to your own endpoint. Configure URL, retries, and signing secret.</div></div>
        </div>
      </Card>
    </>
  )
}

function DataPane() {
  return (
    <>
      <div className="acc-pane-head"><h2>Data &amp; privacy</h2><span className="sub">Export your data or close your account</span></div>
      <Card title="Export">
        <div className="field-row">
          <div className="field-label">Trade history<small>All closed + open positions · CSV</small></div>
          <div className="field-value"><span style={{ fontFamily: 'var(--mono)', fontSize: 11.5, color: 'var(--ink-3)' }}>Full trade log since 2024-08-14</span></div>
          <button className="btn sm">Download CSV</button>
        </div>
        <div className="field-row">
          <div className="field-label">Portfolio snapshot<small>Current holdings &amp; cost basis · XLSX</small></div>
          <div className="field-value"><span style={{ fontFamily: 'var(--mono)', fontSize: 11.5, color: 'var(--ink-3)' }}>Holdings across your watchlists</span></div>
          <button className="btn sm">Download XLSX</button>
        </div>
        <div className="field-row">
          <div className="field-label">Full account data<small>Everything we have on you · JSON, GDPR-compliant</small></div>
          <div className="field-value"><span style={{ fontFamily: 'var(--mono)', fontSize: 11.5, color: 'var(--ink-3)' }}>Sent to your email, takes up to 24h</span></div>
          <button className="btn sm">Request export</button>
        </div>
      </Card>
      <div className="setting-card danger-zone">
        <div className="setting-card-head"><h3 style={{ color: 'var(--red)' }}>Danger zone</h3></div>
        <div className="danger-card-body">
          <div className="text">
            <div className="ttl">Delete account</div>
            <div className="desc">Permanently delete your Vesign account, all watchlists, trade history, and personal data. This cannot be undone. Your billing will be cancelled on the next cycle.</div>
          </div>
          <button className="btn danger">Delete account</button>
        </div>
      </div>
    </>
  )
}

export default function AccountPage() {
  const [pane, setPane] = useState('profile')
  const { user } = useUser()
  const { signOut, openUserProfile } = useClerk()
  const { i18n } = useTranslation()
  const { currency, setCurrency } = useCurrency()
  const me = useMe()
  const planLabel = PLAN_LABELS[me.plan] || (me.plan ? me.plan[0].toUpperCase() + me.plan.slice(1) : 'Free')
  const initials = ((user?.firstName?.[0] || '') + (user?.lastName?.[0] || '')) || 'IL'
  const crumb = NAV.find(n => n.id === pane)?.label || 'Profile'

  return (
    <>
      <div className="acc-head">
        <NavLink className="acc-back" to="/market">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M15 18l-6-6 6-6" /></svg>
          Back
        </NavLink>
        <div className="acc-crumbs">
          <span className="crumb">Account</span><span className="sep">/</span><span className="crumb active">{crumb}</span>
        </div>
      </div>

      <div className="account-layout">
        <aside className="acc-side">
          <div className="acc-user-card">
            <div className="ava">
              {user?.imageUrl ? <img src={user.imageUrl} alt={initials} /> : initials}
              <div className="edit" title="Change picture" onClick={() => openUserProfile()}>
                <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M12 19l7-7 3 3-7 7-3-3z M18 13l-1.5-7.5L2 2l3.5 14.5L13 18l5-5z" /></svg>
              </div>
            </div>
            <div className="name">{user?.fullName || 'Account'}</div>
            <div className="email">{user?.primaryEmailAddress?.emailAddress || ''}</div>
            <div className="plan-badge">★ {planLabel}</div>
          </div>
          <nav className="acc-nav">
            {NAV.map(n => (
              n.soon ? (
                <a key={n.id} className="soon" aria-disabled="true" title="Coming soon">
                  {ICONS[n.id]}{n.label}<span className="soon-tag">Soon</span>
                </a>
              ) : (
                <a key={n.id} className={pane === n.id ? 'active' : ''} onClick={() => { setPane(n.id); window.scrollTo({ top: 0 }) }}>
                  {ICONS[n.id]}{n.label}
                </a>
              )
            ))}
            <a className="logout" onClick={() => signOut({ redirectUrl: '/sign-in' })}>
              <svg className="ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4 M16 17l5-5-5-5 M21 12H9" /></svg>
              Log out
            </a>
          </nav>
        </aside>

        <main className="acc-main">
          <div className="acc-pane">
            {pane === 'profile' && <ProfilePane user={user} openUserProfile={openUserProfile} currency={currency} setCurrency={setCurrency} i18n={i18n} />}
            {pane === 'plan' && <PlanPane planLabel={planLabel} />}
            {pane === 'wallet' && <WalletPane balanceCents={me.balance_cents} unlockCents={me.per_row_price_cents} />}
            {pane === 'notifications' && <NotificationsPane />}
            {pane === 'trading' && <TradingPane />}
            {pane === 'security' && <SecurityPane openUserProfile={openUserProfile} />}
            {pane === 'api' && <ApiPane />}
            {pane === 'data' && <DataPane />}
          </div>
        </main>
      </div>
    </>
  )
}
