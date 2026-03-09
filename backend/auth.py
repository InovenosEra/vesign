"""Clerk JWT verification for Vesign."""
import base64
import os
import time

import requests as _requests
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwk, jwt

load_dotenv()

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

_bearer = HTTPBearer()


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(_bearer)):
    try:
        payload = _verify_token(credentials.credentials)
        user_id: str = payload.get("sub")
        if not user_id:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
        return {"id": user_id, "email": payload.get("email", "")}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
