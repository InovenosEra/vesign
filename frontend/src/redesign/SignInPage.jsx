/* VeSign sign-in — Clerk's prebuilt <SignIn /> in the shared dark auth shell.
 * Replaces the legacy custom LoginPage (hand-rolled form + request-access
 * flow), which self-serve sign-up superseded. Public route /sign-in — also the
 * target of the app's auth gate (ClerkProvider signInUrl). */
import { SignIn } from '@clerk/react'
import AuthLayout, { CLERK_APPEARANCE } from './AuthLayout'

export default function SignInPage() {
  return (
    <AuthLayout hint="New to VeSign?" ctaLabel="Sign up free" ctaTo="/sign-up">
      <div className="ld-auth-head">
        <h1>Welcome back</h1>
        <p>Sign in to your VeSign account.</p>
      </div>
      {/* forceRedirectUrl (Clerk v6): land on /market after sign-in. */}
      <SignIn forceRedirectUrl="/market" signUpUrl="/sign-up" appearance={CLERK_APPEARANCE} />
    </AuthLayout>
  )
}
