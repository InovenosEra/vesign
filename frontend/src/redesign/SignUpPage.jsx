/* VeSign self-serve sign-up — Clerk's prebuilt <SignUp /> (handles email
 * verification + social providers enabled in the Clerk dashboard) in a
 * two-column layout: plan explanation left, form right. Public route /sign-up.
 * New users carry NO entitlement record, so backend/entitlements.get_plan()
 * resolves them to the FREE tier — paid features stay behind the paywall. */
import { SignUp } from '@clerk/react'
import AuthLayout, { CLERK_APPEARANCE } from './AuthLayout'
import { TIERS } from './tiers'

/* Informational only — no plan selection or payment here. New accounts always
 * start on Free; upgrades happen later behind the in-app paywall. */
function PlanExplainer() {
  return (
    <aside className="ld-auth-plans">
      <h2>Start free — upgrade when the signals earn it.</h2>
      <p className="ld-auth-plans-sub">
        Every account starts on the Free tier. No card, no trial clock.{' '}
        {/* TODO: wire to backend — tier copy/prices are placeholders */}
        <span className="ld-placeholder-tag">Placeholder pricing</span>
      </p>
      {TIERS.map(t => (
        <div className={'ld-auth-tier' + (t.featured ? ' featured' : '')} key={t.name}>
          <div className="ld-auth-tier-head">
            <span className="nm">{t.name}</span>
            <span className="pr">{t.price}<em>{t.period}</em></span>
          </div>
          <div className="bl">{t.blurb}</div>
          <ul>
            {t.features.map(f => <li key={f}>{f}</li>)}
          </ul>
        </div>
      ))}
    </aside>
  )
}

export default function SignUpPage() {
  return (
    <AuthLayout hint="Already have an account?" ctaLabel="Log in" ctaTo="/sign-in">
      <div className="ld-auth-head">
        <h1>Create your free account</h1>
        <p>Free tier, no credit card. Upgrade only if the signals earn it.</p>
      </div>
      <div className="ld-auth-grid">
        <PlanExplainer />
        <div className="ld-auth-form">
          {/* forceRedirectUrl (Clerk v6): always land on /market after the full
              sign-up flow, including the built-in email verification step. */}
          <SignUp forceRedirectUrl="/market" signInUrl="/sign-in" appearance={CLERK_APPEARANCE} />
        </div>
      </div>
    </AuthLayout>
  )
}
