from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth import get_current_token, get_current_user
from backend.auth_service import create_session, login_user, register_user, revoke_session

router = APIRouter()


class AuthPayload(BaseModel):
    email: str = Field(..., min_length=3)
    password: str = Field(..., min_length=6)


@router.post("/api/auth/register")
def register(payload: AuthPayload):
    try:
        user = register_user(payload.email, payload.password)
        session = create_session(int(user["id"]))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "ok": True,
        "token": session["token"],
        "expires_at": session["expires_at"],
        "user": user,
    }


@router.post("/api/auth/login")
def login(payload: AuthPayload):
    try:
        result = login_user(payload.email, payload.password)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "ok": True,
        "token": result["token"],
        "expires_at": result["expires_at"],
        "user": result["user"],
    }


@router.get("/api/auth/me")
def me(user: Dict[str, Any] = Depends(get_current_user)):
    return {"ok": True, "user": user}


@router.post("/api/auth/logout")
def logout(token: str = Depends(get_current_token)):
    revoke_session(token)
    return {"ok": True}
