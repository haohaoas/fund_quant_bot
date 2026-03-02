from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from backend import portfolio_service as ps
from backend.auth import get_current_user

router = APIRouter()


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class InvestmentPayload(BaseModel):
    code: str = Field(..., min_length=1, description="Fund code")
    amount: float = Field(..., description="Amount of money; BUY positive, SELL negative")
    action: Optional[str] = Field(None, description="BUY/SELL/SIP/REDEEM (optional)")
    nav: Optional[float] = Field(None, description="Optional price/NAV; if omitted, try realtime quote")
    shares: Optional[float] = Field(None, description="Optional shares; if omitted, shares = abs(amount)/nav")
    account_id: Optional[int] = Field(
        default=None, description="Account ID; if omitted, use active account"
    )


class SectorOverrideIn(BaseModel):
    code: str
    sector: str = ""  # 传空=删除覆盖


@router.get("/api/portfolio")
def portfolio(
    force_refresh: bool = False,
    account_id: Optional[int] = None,
    quote_source: str = "auto",
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(account_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if force_refresh:
        clear_fn = getattr(ps, "clear_fund_gz_cache", None)
        source_mode = str(quote_source or "").strip().lower()
        # estimate mode is latency-sensitive; keep short-lived cache to avoid
        # serial network calls timing out on portfolio page.
        if callable(clear_fn) and source_mode != "estimate":
            clear_fn()
    classic_positions = ps.list_positions(
        account_id=aid,
        quote_source=quote_source,
    )

    cashflow_fn = getattr(ps, "get_cashflow_summary", None)
    if callable(cashflow_fn):
        cashflow = cashflow_fn(days=3650, account_id=aid)
    else:
        inflow = 0.0
        outflow = 0.0
        for t in ps.list_trades(limit=2000, account_id=aid):
            a = str(t.get("action") or "").upper()
            amt = t.get("amount")
            price = t.get("price")
            sh = t.get("shares")
            v = None
            try:
                if amt is not None:
                    v = float(amt)
                elif price is not None and sh is not None:
                    v = float(price) * float(sh)
            except Exception:
                v = None
            if v is None:
                continue
            if a in ("BUY", "SIP"):
                outflow += v
            elif a in ("SELL", "REDEEM"):
                inflow += v
        cashflow = {
            "inflow": round(inflow, 2),
            "outflow": round(outflow, 2),
            "net": round(inflow - outflow, 2),
            "trades": float(len(ps.list_trades(limit=2000, account_id=aid))),
        }

    total_mv = 0.0
    total_daily_profit = 0.0
    total_holding_profit = 0.0
    for p in classic_positions:
        mv = p.get("market_value")
        dp = p.get("daily_profit")
        hp = p.get("holding_profit")
        try:
            if mv is not None:
                total_mv += float(mv)
            if dp is not None:
                total_daily_profit += float(dp)
            if hp is not None:
                total_holding_profit += float(hp)
        except Exception:
            pass

    cash = ps.get_account_cash(account_id=aid, user_id=uid)

    return {
        "account_id": aid,
        "generated_at": _now_str(),
        "cash": round(float(cash), 2),
        "positions": classic_positions,
        "account": {
            "cash": round(float(cash), 2),
            "total_market_value": round(float(total_mv), 2),
            "daily_profit": round(float(total_daily_profit), 2),
            "holding_profit": round(float(total_holding_profit), 2),
            "total_asset": round(float(cash) + float(total_mv), 2),
        },
        "b_mode": {
            "trades": int(cashflow.get("trades", 0) or 0),
            "cashflow": cashflow,
            "positions": classic_positions,
            "by_sector": [],
        },
    }


@router.post("/api/investments")
def create_investment(
    payload: InvestmentPayload,
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(payload.account_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    code = payload.code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="code required")

    amount = float(payload.amount)
    action = (payload.action or ("BUY" if amount >= 0 else "SELL")).strip().upper()
    if action not in ("BUY", "SELL", "SIP", "REDEEM"):
        raise HTTPException(status_code=400, detail="action must be BUY/SELL/SIP/REDEEM")

    trade_value = abs(amount)

    price = float(payload.nav) if payload.nav is not None else None
    if price is None:
        fetch_fn = getattr(ps, "fetch_fund_gz", None)
        gz = fetch_fn(code) if callable(fetch_fn) else None
        if isinstance(gz, dict) and gz.get("ok"):
            try:
                price = float(gz.get("prev_nav") or gz.get("nav") or 0.0)
            except Exception:
                price = None

    if price is None:
        q = ps.get_latest_quote(code)
        if q and q.get("nav"):
            price = float(q["nav"])

    if price is None or price <= 0:
        raise HTTPException(
            status_code=400,
            detail="nav/price not available. Provide payload.nav or add a quote via /api/quotes, or ensure fundgz is reachable.",
        )

    shares = float(payload.shares) if payload.shares is not None else None
    if shares is None:
        shares = trade_value / price
    if shares <= 0:
        raise HTTPException(status_code=400, detail="shares must be > 0")

    if action in ("BUY", "SIP"):
        spend = shares * price
        cash = ps.get_account_cash(account_id=aid, user_id=uid)
        if cash < spend:
            ps.set_account_cash(cash + (spend - cash), account_id=aid, user_id=uid)

    ts = _now_str()
    ps.add_trade(
        account_id=aid,
        code=code,
        action=action,
        amount=trade_value,
        price=price,
        shares=shares,
        note="from /api/investments",
        ts=ts,
    )

    applied = ps.apply_trade_to_portfolio(
        account_id=aid,
        code=code,
        action=action,
        price=price,
        amount=trade_value,
        shares=shares,
    )
    if not applied.get("ok"):
        raise HTTPException(status_code=400, detail=applied)

    return {
        "ok": True,
        "account_id": aid,
        "trade": {
            "ts": ts,
            "code": code,
            "action": action,
            "amount": round(float(trade_value), 2),
            "price": round(float(price), 6),
            "shares": round(float(shares), 6),
        },
        "position": ps.get_position(code, account_id=aid)
        if hasattr(ps, "get_position")
        else None,
    }


@router.get("/api/investments")
def list_investments(
    limit: int = 200,
    account_id: Optional[int] = None,
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(account_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    limit = max(1, min(int(limit), 2000))
    items = ps.list_trades(limit=limit, account_id=aid)
    return {"generated_at": _now_str(), "account_id": aid, "items": items}


@router.delete("/api/positions/{code}")
def delete_position(
    code: str,
    account_id: Optional[int] = None,
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(account_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    code = str(code).strip()
    if not code:
        raise HTTPException(status_code=400, detail="code required")

    remove_fn = getattr(ps, "remove_position", None)
    if not callable(remove_fn):
        raise HTTPException(status_code=500, detail="remove_position not implemented")

    result = remove_fn(code, account_id=aid)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "position not found"))
    return {"ok": True, "account_id": aid, "deleted": result}


@router.get("/api/investments/summary")
def investments_summary(
    account_id: Optional[int] = None,
    user: Dict[str, Any] = Depends(get_current_user),
):
    uid = int(user["id"])
    try:
        aid = ps.resolve_account_id_for_user(account_id, uid)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    cashflow_fn = getattr(ps, "get_cashflow_summary", None)
    cashflow = (
        cashflow_fn(days=3650, account_id=aid)
        if callable(cashflow_fn)
        else {"inflow": 0.0, "outflow": 0.0, "net": 0.0, "trades": 0}
    )
    return {"generated_at": _now_str(), "account_id": aid, "cashflow": cashflow}


@router.get("/api/sector_override")
def api_get_sector_override(code: str, user: Dict[str, Any] = Depends(get_current_user)):
    _ = user
    get_fn = getattr(ps, "get_sector_override", None)
    if not callable(get_fn):
        return {"code": code, "sector": ""}
    return {"code": code, "sector": get_fn(code) or ""}


@router.post("/api/sector_override")
def api_set_sector_override(payload: SectorOverrideIn, user: Dict[str, Any] = Depends(get_current_user)):
    _ = user
    set_fn = getattr(ps, "set_sector_override", None)
    if not callable(set_fn):
        raise HTTPException(status_code=400, detail="portfolio_service.set_sector_override not implemented")
    set_fn(payload.code, payload.sector)
    return {"ok": True, "code": payload.code, "sector": payload.sector}
