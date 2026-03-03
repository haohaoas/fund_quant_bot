from __future__ import annotations

from typing import Any, Dict, Optional
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend.auth import get_current_user
from backend import watchlist_service as ws

router = APIRouter()
_WATCHLIST_ROUTE_TIMEOUT_SECONDS = float(os.getenv("WATCHLIST_ROUTE_TIMEOUT_SECONDS", "10"))


class WatchlistPayload(BaseModel):
    code: str = Field(..., min_length=1)
    name: str = Field(default="")


class WatchlistSectorPayload(BaseModel):
    code: str = Field(..., min_length=1)
    sector: str = Field(default="")
    name: str = Field(default="")


@router.get("/api/watchlist")
def get_watchlist(
    quote_source: str = "auto",
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(ws.list_watchlist, uid, quote_source)
            items = fut.result(timeout=_WATCHLIST_ROUTE_TIMEOUT_SECONDS)
    except FuturesTimeoutError:
        items = []
    except Exception:
        items = []
    return {"items": items}


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


@router.post("/api/watchlist/sector")
def set_watchlist_sector(
    payload: WatchlistSectorPayload,
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        result = ws.set_watchlist_sector(
            uid,
            payload.code,
            payload.sector,
            payload.name,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, **result}


@router.get("/api/watchlist/analyze")
def analyze_watchlist_fund(
    code: str,
    name: Optional[str] = None,
    quote_source: str = "auto",
    include_ai: bool = True,
    user: Dict[str, Any] = Depends(get_current_user),
):
    _ = user
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(
                ws.analyze_fund,
                code=code,
                name=name or "",
                quote_source=quote_source,
                include_ai=include_ai,
            )
            result = fut.result(timeout=_WATCHLIST_ROUTE_TIMEOUT_SECONDS)
    except FuturesTimeoutError:
        # Degrade to no-AI variant to keep UX responsive.
        try:
            result = ws.analyze_fund(
                code=code,
                name=name or "",
                quote_source=quote_source,
                include_ai=False,
            )
            ai = result.get("ai_decision") or {}
            ai["reason"] = "分析超时，已返回无AI快速结果。"
            result["ai_decision"] = ai
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"analyze timeout: {type(e).__name__}: {e}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"analyze failed: {type(e).__name__}: {e}")
    return result
