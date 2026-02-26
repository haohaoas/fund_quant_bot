# backend/routers/trades.py
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from backend import portfolio_service as ps
from backend.auth import get_current_user

router = APIRouter()


class TradePayload(BaseModel):
    account_id: Optional[int] = None
    code: str
    action: str
    price: float
    amount: Optional[float] = None
    shares: Optional[float] = None
    note: str = ""


@router.get("/api/trades")
def trades(
    limit: int = 50,
    account_id: Optional[int] = None,
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(account_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"account_id": aid, "items": ps.list_trades(limit=limit, account_id=aid)}


@router.post("/api/trades")
def create_trade(payload: TradePayload, user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(payload.account_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    code = str(payload.code)
    action = str(payload.action).upper()
    price = float(payload.price)

    trade_id = ps.add_trade(
        account_id=aid,
        code=code,
        action=action,
        amount=payload.amount,
        price=price,
        shares=payload.shares,
        note=payload.note,
    )
    applied = ps.apply_trade_to_portfolio(
        account_id=aid,
        code=code,
        action=action,
        price=price,
        amount=payload.amount,
        shares=payload.shares,
    )
    if not applied.get("ok"):
        raise HTTPException(status_code=400, detail=applied)

    return {"ok": True, "account_id": aid, "trade_id": trade_id, "applied": applied}
