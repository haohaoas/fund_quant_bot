# backend/routers/account.py
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend import portfolio_service as ps
from backend.auth import get_current_user

router = APIRouter()


class CashPayload(BaseModel):
    cash: float = Field(..., description="Account cash")
    account_id: Optional[int] = Field(
        default=None, description="Account ID; if omitted, use active account"
    )


class CreateAccountPayload(BaseModel):
    name: str = Field(..., min_length=1, description="Account name")
    cash: float = Field(default=0.0, description="Initial cash")
    avatar: str = Field(default="", description="Avatar text or URL")


class UpdateAccountPayload(BaseModel):
    name: Optional[str] = Field(default=None, description="Account name")
    avatar: Optional[str] = Field(default=None, description="Avatar text or URL")


@router.get("/api/accounts")
def get_accounts(user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    return {"items": ps.list_accounts(user_id=uid)}


@router.post("/api/accounts")
def create_account(payload: CreateAccountPayload, user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    try:
        account = ps.create_account(payload.name, payload.cash, payload.avatar, user_id=uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "account": account}


@router.patch("/api/accounts/{account_id}")
def update_account(account_id: int, payload: UpdateAccountPayload, user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    try:
        account = ps.update_account(
            account_id,
            name=payload.name,
            avatar=payload.avatar,
            user_id=uid,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "account": account}


@router.get("/api/account")
def get_account(account_id: Optional[int] = None, user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(account_id, uid)
        account = ps.get_account(aid, user_id=uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not account:
        raise HTTPException(status_code=404, detail="account not found")
    return account


@router.post("/api/account/cash")
def update_cash(payload: CashPayload, user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(payload.account_id, uid)
        ps.set_account_cash(float(payload.cash), account_id=aid, user_id=uid)
        account = ps.get_account(account_id=aid, user_id=uid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    if not account:
        raise HTTPException(status_code=404, detail="account not found")
    return {
        "ok": True,
        "account_id": int(account["id"]),
        "cash": float(account["cash"]),
    }
