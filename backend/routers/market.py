# backend/routers/market.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

import time
import threading

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from backend import portfolio_service as ps
from backend.services.sector_flow_service import sector_fund_flow_core, diagnostics_providers
router = APIRouter()

# =========================
# In-memory cache to avoid spamming upstream providers (e.g., akshare)
# Cache key is (indicator, sector_type, provider)
# We cache the largest requested top_n and serve subsets for smaller top_n.
# =========================
_FLOW_CACHE_TTL_SECONDS = 60
_flow_cache_lock = threading.Lock()
_flow_cache: Dict[tuple, Dict[str, Any]] = {}


def _flow_cache_key(indicator: str, sector_type: str, provider: Optional[str]) -> tuple:
    return (str(indicator or ''), str(sector_type or ''), str(provider or 'auto'))


def _get_sector_fund_flow_cached(
    *,
    indicator: str,
    sector_type: str,
    top_n: int,
    provider: Optional[str],
) -> Dict[str, Any]:
    """Return cached sector fund flow data with TTL; reduce duplicate upstream calls."""
    key = _flow_cache_key(indicator, sector_type, provider)
    now = time.time()

    with _flow_cache_lock:
        cached = _flow_cache.get(key)
        if cached:
            ts = float(cached.get("_ts", 0.0))
            cached_top_n = int(cached.get("_top_n", 0))
            if (now - ts) < _FLOW_CACHE_TTL_SECONDS and cached_top_n >= int(top_n):
                # Serve subset
                out = dict(cached.get("data") or {})
                items = (out.get("items") or [])
                if isinstance(items, list):
                    out["items"] = items[: int(top_n)]
                out["top_n"] = int(top_n)
                out["cached"] = True
                out["cache_age_seconds"] = int(now - ts)
                return out

    # Cache miss / expired / need larger top_n: fetch fresh
    res = sector_fund_flow_core(indicator=indicator, sector_type=sector_type, top_n=top_n, provider=provider)

    # Store fresh (cache the largest top_n we fetched)
    try:
        with _flow_cache_lock:
            _flow_cache[key] = {"_ts": now, "_top_n": int(top_n), "data": res}
    except Exception:
        pass

    # Annotate
    if isinstance(res, dict):
        res.setdefault("cached", False)
        res.setdefault("cache_age_seconds", None)
    return res


@router.get("/api/sector_fund_flow")
def sector_fund_flow(
    indicator: str = "今日",
    sector_type: str = "行业资金流",
    top_n: int = 30,
    provider: Optional[str] = None,
):
    res = _get_sector_fund_flow_cached(indicator=indicator, sector_type=sector_type, top_n=top_n, provider=provider)
    return JSONResponse(content=res, headers={"Content-Type": "application/json; charset=utf-8"})


@router.get("/api/dashboard")
def dashboard(
    indicator: str = "今日",
    sector_type: str = "行业资金流",
    top_n: int = 20,
    provider: Optional[str] = None,
):
    data = _get_sector_fund_flow_cached(indicator=indicator, sector_type=sector_type, top_n=top_n, provider=provider)
    cash = ps.get_account_cash()
    positions = ps.list_positions()

    return JSONResponse(
        content={
            "generated_at": data.get("generated_at", ""),
            "fetched_at": data.get("fetched_at", ""),
            "stale": bool(data.get("stale", False)),
            "warning": data.get("warning", ""),
            "indicator": data.get("indicator", indicator),
            "sector_type": data.get("sector_type", sector_type),
            "top_n": data.get("top_n", top_n),
            "account": {"cash": cash, "positions_count": len(positions)},
            "sectors": data.get("items", []),
            "provider": data.get("provider", ""),
            "debug_columns": data.get("debug_columns", []),
        },
        headers={"Content-Type": "application/json; charset=utf-8"},
    )


@router.get("/api/diagnostics/providers")
def diagnostics():
    return diagnostics_providers()


# =========================
# Strategy (Aggressive MVP)
# =========================

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _fund_to_sector(code: str) -> str:
    """
    Resolve fund -> sector.
    Priority:
    1) ps.get_sector_override (if implemented)
    2) sector.get_sector_by_fund (持仓加权/静态映射)
    3) '其他'
    """
    code = (code or "").strip()
    if not code:
        return "其他"

    get_override = getattr(ps, "get_sector_override", None)
    if callable(get_override):
        try:
            s = (get_override(code) or "").strip()
            if s:
                return s
        except Exception:
            pass

    try:
        from sector import get_sector_by_fund  # type: ignore

        s = (get_sector_by_fund(code) or "").strip()
        if s and s != "未知板块":
            return s
    except Exception:
        pass

    return "其他"


def _reverse_sector_map() -> Dict[str, List[str]]:
    """Reverse mapping: sector -> [fund_codes]."""
    mp: Dict[str, List[str]] = {}
    try:
        from sector import FUND_TO_SECTOR  # type: ignore
        for c, s in (FUND_TO_SECTOR or {}).items():
            if not s:
                continue
            mp.setdefault(str(s), []).append(str(c))
    except Exception:
        pass
    return mp


class StrategyPlanIn(BaseModel):
    indicator: str = Field("5日", description="资金流周期：今日/5日/10日")
    sector_type: str = Field("行业资金流", description="行业资金流/概念资金流/地域资金流")
    top_n: int = Field(20, ge=5, le=200)
    provider: Optional[str] = Field(None, description="tushare/akshare/auto")

    # Execution constraints
    budget_today: float = Field(0.0, ge=0.0, description="今日新增投入预算（0=不额外投入，仅给出结构性建议）")
    max_single_trade: float = Field(1000.0, ge=0.0, description="单笔最大金额")
    max_trades_per_day: int = Field(5, ge=1, le=20, description="最多生成多少笔交易")

    # Aggressive tilt params
    tilt_top1: float = Field(0.08, ge=0.0, le=0.5)
    tilt_top2: float = Field(0.05, ge=0.0, le=0.5)
    tilt_top3: float = Field(0.03, ge=0.0, le=0.5)
    tilt_cap_per_day: float = Field(0.12, ge=0.0, le=0.8, description="单日总倾斜上限")

    # Risk controls
    min_cash_ratio: float = Field(0.15, ge=0.0, le=0.9, description="最低现金比例")
    max_position_per_fund: float = Field(0.40, ge=0.05, le=0.95, description="单基金最大仓位")
    sell_only_if_over_by: float = Field(0.06, ge=0.0, le=0.5, description="只有超配超过这个比例才建议卖出")

    # Base targets (by sector). If omitted, use a default.
    base_target_by_sector: Optional[Dict[str, float]] = Field(
        None,
        description="基准目标权重（按板块）。例：{\"半导体\":0.30,\"先进制造\":0.30,\"机器人\":0.20,\"CASH\":0.20}",
    )


def _normalize_weights(w: Dict[str, float]) -> Dict[str, float]:
    cleaned: Dict[str, float] = {}
    for k, v in (w or {}).items():
        kk = str(k).strip() or "其他"
        cleaned[kk] = max(0.0, _safe_float(v, 0.0))

    s = sum(cleaned.values())
    if s <= 0:
        return cleaned
    return {k: v / s for k, v in cleaned.items()}


def _default_base_targets() -> Dict[str, float]:
    # Aggressive-friendly baseline; override via request if needed.
    return {
        "半导体": 0.30,
        "先进制造": 0.30,
        "机器人": 0.20,
        "CASH": 0.20,
    }


def _portfolio_snapshot() -> Dict[str, Any]:
    cash = _safe_float(ps.get_account_cash(), 0.0)
    positions = ps.list_positions() or []

    total_mv = 0.0
    by_code_mv: Dict[str, float] = {}
    for p in positions:
        code = str(p.get("code") or p.get("fund_code") or "").strip()
        mv = p.get("market_value")
        if mv is None:
            mv = p.get("mv")
        if mv is None:
            mv = p.get("cost")
        v = max(0.0, _safe_float(mv, 0.0))
        if code:
            by_code_mv[code] = by_code_mv.get(code, 0.0) + v
        total_mv += v

    total_asset = cash + total_mv
    return {
        "cash": cash,
        "positions": positions,
        "by_code_mv": by_code_mv,
        "total_market_value": total_mv,
        "total_asset": total_asset,
    }


def _current_weights_by_sector(snapshot: Dict[str, Any]) -> Dict[str, float]:
    by_code_mv: Dict[str, float] = snapshot.get("by_code_mv", {})
    total_asset = max(1e-9, _safe_float(snapshot.get("total_asset"), 0.0))

    by_sector: Dict[str, float] = {}
    for code, mv in (by_code_mv or {}).items():
        sec = _fund_to_sector(code)
        by_sector[sec] = by_sector.get(sec, 0.0) + _safe_float(mv, 0.0)

    return {k: v / total_asset for k, v in by_sector.items()}


def _apply_aggressive_tilt(
    base: Dict[str, float],
    top_sectors: List[str],
    tilt_top1: float,
    tilt_top2: float,
    tilt_top3: float,
    tilt_cap_per_day: float,
    min_cash_ratio: float,
) -> Dict[str, float]:
    base = dict(base or {})
    base.setdefault("CASH", 0.0)
    base = _normalize_weights(base)

    tilts = [tilt_top1, tilt_top2, tilt_top3]
    desired: Dict[str, float] = {}
    for i, sec in enumerate(top_sectors[:3]):
        if not sec or sec == "CASH":
            continue
        desired[sec] = desired.get(sec, 0.0) + max(0.0, tilts[i])

    total_tilt = sum(desired.values())
    if total_tilt <= 0:
        return base

    if total_tilt > tilt_cap_per_day:
        scale = tilt_cap_per_day / total_tilt
        desired = {k: v * scale for k, v in desired.items()}
        total_tilt = sum(desired.values())

    cash = base.get("CASH", 0.0)
    take_from_cash = min(total_tilt, max(0.0, cash - min_cash_ratio))
    cash -= take_from_cash
    remaining = total_tilt - take_from_cash

    if remaining > 1e-9:
        donors = {k: v for k, v in base.items() if k not in set(top_sectors[:3]) and k != "CASH" and v > 0}
        donors_sum = sum(donors.values())
        if donors_sum > 1e-9:
            for k, v in donors.items():
                cut = remaining * (v / donors_sum)
                base[k] = max(0.0, base[k] - cut)

    base["CASH"] = cash
    for sec, add in desired.items():
        base[sec] = base.get(sec, 0.0) + add

    base = _normalize_weights(base)

    if base.get("CASH", 0.0) + 1e-9 < min_cash_ratio:
        need = min_cash_ratio - base.get("CASH", 0.0)
        non_cash = {k: v for k, v in base.items() if k != "CASH"}
        if non_cash:
            k_max = max(non_cash.keys(), key=lambda k: non_cash[k])
            base[k_max] = max(0.0, base[k_max] - need)
            base["CASH"] = min_cash_ratio
            base = _normalize_weights(base)

    return base


def _allocate_buys_to_funds(sector: str, amount: float, snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    by_code_mv: Dict[str, float] = snapshot.get("by_code_mv", {})

    held = [(c, mv) for c, mv in by_code_mv.items() if _fund_to_sector(c) == sector]
    if held:
        held.sort(key=lambda x: x[1], reverse=True)
        return {"code": held[0][0], "amount": amount}

    rev = _reverse_sector_map()
    codes = rev.get(sector) or []
    if codes:
        return {"code": codes[0], "amount": amount}

    return None


@router.post("/api/strategy/plan")
def strategy_plan(payload: StrategyPlanIn):
    snapshot = _portfolio_snapshot()
    total_asset = _safe_float(snapshot.get("total_asset"), 0.0)
    cash = _safe_float(snapshot.get("cash"), 0.0)

    flow = _get_sector_fund_flow_cached(
        indicator=payload.indicator,
        sector_type=payload.sector_type,
        top_n=payload.top_n,
        provider=payload.provider,
    )

    items = flow.get("items", []) or []
    top_sectors = [str(x.get("name") or "").strip() for x in items[:3] if isinstance(x, dict)]

    base = payload.base_target_by_sector or _default_base_targets()
    base = _normalize_weights(base)
    if "CASH" not in base:
        base["CASH"] = 0.0
        base = _normalize_weights(base)

    dynamic = _apply_aggressive_tilt(
        base=base,
        top_sectors=top_sectors,
        tilt_top1=payload.tilt_top1,
        tilt_top2=payload.tilt_top2,
        tilt_top3=payload.tilt_top3,
        tilt_cap_per_day=payload.tilt_cap_per_day,
        min_cash_ratio=payload.min_cash_ratio,
    )

    current_by_sector = _current_weights_by_sector(snapshot)
    current_cash_ratio = (cash / total_asset) if total_asset > 1e-9 else 1.0

    signals: List[Dict[str, Any]] = []
    for i, sec in enumerate(top_sectors[:3]):
        raw = items[i] if i < len(items) else {}
        signals.append(
            {
                "id": f"sector_flow_top{i+1}",
                "level": "info",
                "title": f"{sec} 资金流强势 (Top{i+1})",
                "detail": {
                    "main_net_yi": raw.get("main_net"),
                    "main_net_pct": raw.get("main_net_pct"),
                    "chg_pct": raw.get("chg_pct"),
                    "indicator": payload.indicator,
                    "sector_type": payload.sector_type,
                },
            }
        )

    signals.append(
        {
            "id": "aggressive_tilt",
            "level": "warn",
            "title": "偏进攻模式：对强势板块进行权重倾斜",
            "detail": {
                "base_targets": base,
                "dynamic_targets": dynamic,
                "tilt_params": {
                    "top1": payload.tilt_top1,
                    "top2": payload.tilt_top2,
                    "top3": payload.tilt_top3,
                    "cap": payload.tilt_cap_per_day,
                    "min_cash_ratio": payload.min_cash_ratio,
                },
            },
        }
    )

    signals.append(
        {
            "id": "risk_controls",
            "level": "info",
            "title": "风控约束",
            "detail": {
                "min_cash_ratio": payload.min_cash_ratio,
                "max_position_per_fund": payload.max_position_per_fund,
                "max_trades_per_day": payload.max_trades_per_day,
                "max_single_trade": payload.max_single_trade,
                "sell_only_if_over_by": payload.sell_only_if_over_by,
            },
        }
    )

    plan: List[Dict[str, Any]] = []

    budget_today = _safe_float(payload.budget_today, 0.0)
    cash_floor_amt = payload.min_cash_ratio * total_asset
    deployable_cash = max(0.0, cash - cash_floor_amt)
    budget_cap = min(deployable_cash, budget_today) if budget_today > 0 else 0.0

    buy_candidates: List[Dict[str, Any]] = []
    for sec, tgt_w in dynamic.items():
        if sec == "CASH":
            continue
        cur_w = current_by_sector.get(sec, 0.0)
        delta_w = tgt_w - cur_w
        if delta_w > 1e-6:
            buy_candidates.append({"sector": sec, "delta_w": delta_w})

    buy_candidates.sort(key=lambda x: x["delta_w"], reverse=True)

    remaining_budget = budget_cap
    skipped: List[str] = []

    for cand in buy_candidates:
        if len(plan) >= payload.max_trades_per_day:
            break
        if remaining_budget <= 1e-6:
            break

        sec = cand["sector"]
        proposed = min(remaining_budget, payload.max_single_trade)

        pick = _allocate_buys_to_funds(sec, proposed, snapshot)
        if not pick:
            skipped.append(sec)
            continue

        plan.append(
            {
                "code": pick["code"],
                "action": "BUY",
                "amount": round(_safe_float(pick["amount"], 0.0), 2),
                "reason": f"进攻倾斜：{sec} 目标权重提升（资金流强势）",
                "sector": sec,
                "priority": len(plan) + 1,
            }
        )
        remaining_budget -= proposed

    # SELL minimal in aggressive mode
    sell_candidates: List[Dict[str, Any]] = []
    for sec, cur_w in current_by_sector.items():
        tgt_w = dynamic.get(sec, 0.0)
        over = cur_w - tgt_w
        if over > payload.sell_only_if_over_by:
            sell_candidates.append({"sector": sec, "over": over})

    if sell_candidates and len(plan) < payload.max_trades_per_day:
        sell_candidates.sort(key=lambda x: x["over"], reverse=True)
        for cand in sell_candidates:
            if len(plan) >= payload.max_trades_per_day:
                break
            sec = cand["sector"]

            amount = min(payload.max_single_trade, max(200.0, payload.max_single_trade * 0.5))

            by_code_mv: Dict[str, float] = snapshot.get("by_code_mv", {})
            held = [(c, mv) for c, mv in by_code_mv.items() if _fund_to_sector(c) == sec]
            if not held:
                continue
            held.sort(key=lambda x: x[1], reverse=True)
            code = held[0][0]

            plan.append(
                {
                    "code": code,
                    "action": "SELL",
                    "amount": round(_safe_float(amount, 0.0), 2),
                    "reason": f"超配回收：{sec} 超过动态目标权重（进攻模式下仅做轻量去风险）",
                    "sector": sec,
                    "priority": len(plan) + 1,
                }
            )

    if budget_today > 0 and budget_cap <= 1e-6:
        signals.append(
            {
                "id": "budget_blocked_by_cash_floor",
                "level": "warn",
                "title": "预算未能投入：现金安全垫限制",
                "detail": {
                    "cash": cash,
                    "cash_floor": cash_floor_amt,
                    "min_cash_ratio": payload.min_cash_ratio,
                    "current_cash_ratio": round(current_cash_ratio, 4),
                },
            }
        )

    if skipped:
        signals.append(
            {
                "id": "missing_sector_mapping",
                "level": "warn",
                "title": "部分强势板块未能生成买入建议：缺少板块-基金映射",
                "detail": {
                    "sectors": skipped,
                    "hint": "可在 sector.py 的 FUND_TO_SECTOR 添加映射，或用 sector_override 覆盖。",
                },
            }
        )

    return {
        "ok": True,
        "generated_at": flow.get("generated_at", ""),
        "meta": {
            "provider": flow.get("provider", ""),
            "indicator": payload.indicator,
            "sector_type": payload.sector_type,
            "top_n": payload.top_n,
        },
        "portfolio_snapshot": {
            "cash": round(cash, 2),
            "total_asset": round(total_asset, 2),
            "cash_ratio": round(current_cash_ratio, 4),
        },
        "market": {
            "top_sectors": items[:5],
            "fetched_at": flow.get("fetched_at", ""),
            "stale": bool(flow.get("stale", False)),
            "warning": flow.get("warning", ""),
        },
        "targets": {
            "base": base,
            "dynamic": dynamic,
        },
        "signals": signals,
        "plan": plan,
    }
