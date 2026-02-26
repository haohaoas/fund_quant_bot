from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import Header, HTTPException

from backend.auth_service import get_user_by_token


def _extract_bearer_token(authorization: Optional[str]) -> str:
    raw = str(authorization or "").strip()
    if not raw:
        raise HTTPException(status_code=401, detail="missing authorization")

    parts = raw.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="invalid authorization format")

    token = parts[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="empty token")
    return token


def get_current_token(authorization: Optional[str] = Header(default=None, alias="Authorization")) -> str:
    return _extract_bearer_token(authorization)


def get_current_user(authorization: Optional[str] = Header(default=None, alias="Authorization")) -> Dict[str, Any]:
    token = _extract_bearer_token(authorization)
    user = get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid or expired session")
    return user
