from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth import get_current_user
from backend import watchlist_service as ws

router = APIRouter()


class WatchlistPayload(BaseModel):
    code: str = Field(..., min_length=1)
    name: str = Field(default="")


@router.get("/api/watchlist")
def get_watchlist(
    quote_source: str = "auto",
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    return {"items": ws.list_watchlist(uid, quote_source=quote_source)}


@router.post("/api/watchlist")
def put_watchlist(payload: WatchlistPayload, user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    try:
        item = ws.upsert_watchlist(uid, payload.code, payload.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "item": item}


@router.delete("/api/watchlist/{code}")
def delete_watchlist(code: str, user: Dict[str, Any] = Depends(get_current_user)):
    uid = int(user["id"])
    try:
        ok = ws.remove_watchlist(uid, code)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not ok:
        raise HTTPException(status_code=404, detail="watchlist item not found")
    return {"ok": True, "code": code}


@router.get("/api/watchlist/analyze")
def analyze_watchlist_fund(
    code: str,
    name: Optional[str] = None,
    quote_source: str = "auto",
    user: Dict[str, Any] = Depends(get_current_user),
):
    _ = user
    try:
        result = ws.analyze_fund(
            code=code,
            name=name or "",
            quote_source=quote_source,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"analyze failed: {type(e).__name__}: {e}")
    return result
