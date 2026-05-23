"""Tests for BYPASS_AUTH=1 dev short-circuit in backend.auth.get_current_user."""
import os
import pytest
from unittest.mock import patch


def test_bypass_returns_dev_user_by_default():
    """BYPASS_AUTH=1 with no BYPASS_USER_ID → synthetic dev-bypass id."""
    from backend.auth import get_current_user
    with patch.object(os, "getenv") as mock_getenv:
        # BYPASS_AUTH=1; BYPASS_USER_ID unset → falls back to its default arg.
        mock_getenv.side_effect = lambda key, default=None: (
            "1" if key == "BYPASS_AUTH" else
            default if key == "BYPASS_USER_ID" else
            os.environ.get(key, default)
        )
        user = get_current_user(credentials=None)
        assert user == {"id": "dev-bypass", "email": "laufer.israel@gmail.com"}


def test_bypass_user_id_impersonates_real_account():
    """BYPASS_AUTH=1 with BYPASS_USER_ID set → that id is returned."""
    from backend.auth import get_current_user
    with patch.object(os, "getenv") as mock_getenv:
        mock_getenv.side_effect = lambda key, default=None: (
            "1" if key == "BYPASS_AUTH" else
            "user_abc123" if key == "BYPASS_USER_ID" else
            os.environ.get(key, default)
        )
        user = get_current_user(credentials=None)
        assert user["id"] == "user_abc123"


def test_no_bypass_requires_credentials():
    """Without BYPASS_AUTH set, get_current_user requires valid credentials (raises 401 for None)."""
    from fastapi import HTTPException
    from backend.auth import get_current_user
    with patch.object(os, "getenv") as mock_getenv:
        # When called with "BYPASS_AUTH", return None; for other keys, return the real value
        mock_getenv.side_effect = lambda key, default=None: None if key == "BYPASS_AUTH" else os.environ.get(key, default)
        with pytest.raises(HTTPException) as exc:
            get_current_user(credentials=None)
        assert exc.value.status_code == 401
