"""Clerk JWT verification for Vesign."""
import base64
import os
import time

import requests as _requests
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwk, jwt
from sqlalchemy import text

from data.loaders import engine

load_dotenv()


# ---------------------------------------------------------------------------
# Blocked-users table — admin-controlled denylist that survives sessions
# ---------------------------------------------------------------------------
# Why a table (not a config var or Clerk's "Lock"):
#   - Clerk's free-tier "Lock" only holds for the Attack Protection lockout
#     duration (typically ~60 min) and Ban requires the Pro plan.
#   - We want indefinite, admin-controlled blocking that survives Clerk's
#     auto-unlock and doesn't require a redeploy to add/remove users.
#
# Block a user (one SQL line on the production server):
#   INSERT INTO blocked_users (user_id, reason) VALUES ('user_xxx', 'reason');
#
# Unblock:
#   DELETE FROM blocked_users WHERE user_id = 'user_xxx';

def _ensure_blocked_users_table() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS blocked_users (
                user_id    TEXT PRIMARY KEY,
                email      TEXT,
                reason     TEXT,
                blocked_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))


_ensure_blocked_users_table()


# Tiny in-process cache to avoid hitting the DB on every request. The TTL is
# short because we want admin blocks to take effect within ~60s of the
# INSERT — that's plenty fast for "kick this user out".
_blocked_cache: dict = {"ids": None, "fetched_at": 0.0}
_BLOCKED_TTL = 60  # seconds


def _is_blocked(user_id: str) -> "tuple[bool, str | None]":
    """Return (blocked, reason). Reason is the admin-supplied message or None."""
    now = time.time()
    if _blocked_cache["ids"] is None or now - _blocked_cache["fetched_at"] > _BLOCKED_TTL:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT user_id, reason FROM blocked_users")).fetchall()
        _blocked_cache["ids"] = {r[0]: r[1] for r in rows}
        _blocked_cache["fetched_at"] = now
    if user_id in _blocked_cache["ids"]:
        return True, _blocked_cache["ids"][user_id]
    return False, None

# ---------------------------------------------------------------------------
# JWKS helpers
# ---------------------------------------------------------------------------

def _clerk_jwks_url() -> str:
    """Derive the JWKS URL from the Clerk publishable key."""
    key = os.getenv("CLERK_PUBLISHABLE_KEY", "")
    try:
        b64 = key.split("_", 2)[2]
        padded = b64 + "=" * ((4 - len(b64) % 4) % 4)
        domain = base64.b64decode(padded).decode().rstrip("$")
        return f"https://{domain}/.well-known/jwks.json"
    except Exception:
        raise RuntimeError("Invalid CLERK_PUBLISHABLE_KEY — check your .env")


_jwks_cache: dict = {"keys": None, "fetched_at": 0.0}
_JWKS_TTL = 3600  # refresh every hour


def _get_jwks() -> dict:
    now = time.time()
    if _jwks_cache["keys"] is None or now - _jwks_cache["fetched_at"] > _JWKS_TTL:
        url = _clerk_jwks_url()
        resp = _requests.get(url, timeout=10)
        resp.raise_for_status()
        _jwks_cache["keys"] = resp.json()
        _jwks_cache["fetched_at"] = now
    return _jwks_cache["keys"]


# ---------------------------------------------------------------------------
# Token verification
# ---------------------------------------------------------------------------

def _verify_token(token: str) -> dict:
    jwks = _get_jwks()
    header = jwt.get_unverified_header(token)
    kid = header.get("kid")

    key_data = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)

    if key_data is None:
        # Key not found — Clerk may have rotated keys, force refresh once
        _jwks_cache["fetched_at"] = 0.0
        jwks = _get_jwks()
        key_data = next((k for k in jwks.get("keys", []) if k.get("kid") == kid), None)

    if key_data is None:
        raise JWTError("Signing key not found in JWKS")

    public_key = jwk.construct(key_data, algorithm="RS256")
    return jwt.decode(token, public_key, algorithms=["RS256"], options={"verify_aud": False})


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


def get_current_user(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)):
    # Dev-only bypass for local UI/UX mockup work. Never set on prod.
    # BYPASS_USER_ID lets local dev impersonate a real Clerk account so the
    # portfolio/watchlist endpoints return real seeded data; defaults to a
    # synthetic id when unset.
    if os.getenv("BYPASS_AUTH") == "1":
        return {
            "id": os.getenv("BYPASS_USER_ID", "dev-bypass"),
            "email": "laufer.israel@gmail.com",
        }

    if credentials is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing token")

    try:
        payload = _verify_token(credentials.credentials)
        user_id: str = payload.get("sub")
        if not user_id:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    # Denylist gate — admin-controlled, survives Clerk's auto-unlock window.
    blocked, reason = _is_blocked(user_id)
    if blocked:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "ACCOUNT_DISABLED", "reason": reason or "Your account has been disabled."},
        )

    return {"id": user_id, "email": payload.get("email", "")}
