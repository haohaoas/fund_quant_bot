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
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            fut = pool.submit(ws.list_watchlist, uid, quote_source)
            items = fut.result(timeout=_WATCHLIST_ROUTE_TIMEOUT_SECONDS)
        finally:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
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
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            fut = pool.submit(
                ws.analyze_fund,
                code=code,
                name=name or "",
                quote_source=quote_source,
                include_ai=include_ai,
            )
            result = fut.result(timeout=_WATCHLIST_ROUTE_TIMEOUT_SECONDS)
        finally:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
    except FuturesTimeoutError:
        # Degrade to no-AI variant to keep UX responsive.
        try:
            pool2 = ThreadPoolExecutor(max_workers=1)
            try:
                fut2 = pool2.submit(
                    ws.analyze_fund,
                    code=code,
                    name=name or "",
                    quote_source=quote_source,
                    include_ai=False,
                )
                result = fut2.result(timeout=3.0)
            finally:
                try:
                    pool2.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
            ai = result.get("ai_decision") or {}
            ai["reason"] = "分析超时，已返回无AI快速结果。"
            result["ai_decision"] = ai
        except Exception:
            # Last-resort lightweight payload (keep schema compatible).
            result = {
                "generated_at": "",
                "code": str(code or ""),
                "name": str(name or code or ""),
                "latest": {"price": None, "pct": None, "time": "", "source": ""},
                "signal": {
                    "action": "HOLD",
                    "position_hint": "KEEP",
                    "hit_level": None,
                    "price_vs_base_pct": None,
                    "reason": "分析服务繁忙，请稍后重试。",
                    "base_price": None,
                    "grids": [],
                },
                "sector": {"name": "未知板块", "score": 50, "flow_pct": None, "level": "中性", "comment": ""},
                "ai_decision": {"action": "HOLD", "reason": "分析超时，已返回占位结果。"},
            }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"analyze failed: {type(e).__name__}: {e}")
    return result
