from __future__ import annotations

from datetime import datetime
import os
from typing import Any, Dict, List, Optional

from backend.db import get_conn, init_db

init_db()


def _norm_user_id(user_id: int) -> int:
    try:
        uid = int(user_id)
    except Exception:
        uid = 0
    if uid <= 0:
        raise ValueError("invalid user id")
    return uid


def _norm_code(code: str) -> str:
    c = str(code or "").strip()
    if not c:
        raise ValueError("code required")
    return c


def _item_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "user_id": int(row["user_id"]),
        "code": str(row["code"] or ""),
        "name": str(row["name"] or ""),
        "latest_price": None,
        "latest_pct": None,
        "sector_name": "",
        "sector_pct": None,
        "latest_time": "",
        "latest_source": "",
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def list_watchlist(user_id: int) -> List[Dict[str, Any]]:
    uid = _norm_user_id(user_id)
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, user_id, code, name, created_at, updated_at
            FROM watchlist_funds
            WHERE user_id = ?
            ORDER BY updated_at DESC, id DESC
            """,
            (uid,),
        ).fetchall()

    items = [_item_from_row(dict(r)) for r in rows]

    # Enrich watchlist with latest quote and fallback name from config.
    try:
        from config import WATCH_FUNDS
    except Exception:
        WATCH_FUNDS = {}

    try:
        from data import get_fund_latest_price, get_fund_name
    except Exception:
        get_fund_latest_price = None
        get_fund_name = None
    try:
        from sector import get_sector_by_fund, get_sector_sentiment
    except Exception:
        get_sector_by_fund = None
        get_sector_sentiment = None
    try:
        from backend.fund_sector_service import (
            get_cached_fund_sector,
            resolve_and_cache_fund_sector,
        )
    except Exception:
        get_cached_fund_sector = None
        resolve_and_cache_fund_sector = None
    try:
        from backend.portfolio_service import fetch_fund_gz
    except Exception:
        fetch_fund_gz = None

    for item in items:
        code = str(item["code"] or "").strip()
        if not item["name"]:
            cfg = WATCH_FUNDS.get(code, {})
            if isinstance(cfg, dict):
                item["name"] = str(cfg.get("name") or "").strip()

        # First priority: reuse the same quote channel as holdings page.
        if callable(fetch_fund_gz):
            try:
                gz = fetch_fund_gz(code) or {}
            except Exception:
                gz = {}
            if gz.get("ok"):
                if not item["name"]:
                    item["name"] = str(gz.get("name") or "").strip()
                try:
                    nav = gz.get("nav")
                    item["latest_price"] = float(nav) if nav is not None else None
                except Exception:
                    item["latest_price"] = None
                try:
                    pct = gz.get("daily_change_pct")
                    item["latest_pct"] = float(pct) if pct is not None else None
                except Exception:
                    item["latest_pct"] = None
                item["latest_time"] = str(gz.get("gztime") or gz.get("jzrq") or "")
                item["latest_source"] = "fundgz"

        need_quote_fallback = not (
            item["latest_price"] is not None
            and item["latest_pct"] is not None
            and item["name"]
        )

        if callable(get_fund_latest_price) and need_quote_fallback:
            try:
                latest = get_fund_latest_price(code) or {}
            except Exception:
                latest = {}

            if not item["name"]:
                item["name"] = str(latest.get("name") or "").strip()

            price = latest.get("price")
            pct = latest.get("pct")
            try:
                item["latest_price"] = float(price) if price is not None else None
            except Exception:
                item["latest_price"] = None
            try:
                item["latest_pct"] = float(pct) if pct is not None else None
            except Exception:
                item["latest_pct"] = None
            item["latest_time"] = str(latest.get("time") or item["latest_time"] or "")
            item["latest_source"] = str(latest.get("source") or item["latest_source"] or "")

        if not item["name"] and callable(get_fund_name):
            try:
                item["name"] = str(get_fund_name(code) or "").strip()
            except Exception:
                pass

        # Enrich sector info for watchlist table.
        sector_name = ""
        if callable(get_cached_fund_sector):
            try:
                cached = get_cached_fund_sector(code) or {}
                sector_name = str(cached.get("sector") or "").strip()
                if sector_name == "未知板块":
                    sector_name = ""
            except Exception:
                sector_name = ""
        if (not sector_name) and callable(resolve_and_cache_fund_sector):
            try:
                sector_name = str(
                    resolve_and_cache_fund_sector(
                        code,
                        fund_name=item["name"],
                        force_refresh=True,
                    )
                    or ""
                ).strip()
            except Exception:
                sector_name = ""
        if (not sector_name) and callable(get_sector_by_fund):
            try:
                sector_name = str(get_sector_by_fund(code) or "").strip()
            except Exception:
                sector_name = ""
        item["sector_name"] = sector_name or "未知板块"

        if item["sector_name"] and item["sector_name"] != "未知板块" and callable(get_sector_sentiment):
            try:
                senti = get_sector_sentiment(item["sector_name"]) or {}
            except Exception:
                senti = {}
            pct = senti.get("flow_pct")
            try:
                item["sector_pct"] = float(pct) if pct is not None else None
            except Exception:
                item["sector_pct"] = None

    return items


def upsert_watchlist(user_id: int, code: str, name: str = "") -> Dict[str, Any]:
    uid = _norm_user_id(user_id)
    c = _norm_code(code)
    nm = str(name or "").strip()

    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO watchlist_funds (user_id, code, name, created_at, updated_at)
            VALUES (?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
            ON CONFLICT(user_id, code) DO UPDATE SET
                name = CASE WHEN excluded.name = '' THEN watchlist_funds.name ELSE excluded.name END,
                updated_at = datetime('now','localtime')
            """,
            (uid, c, nm),
        )
        row = conn.execute(
            """
            SELECT id, user_id, code, name, created_at, updated_at
            FROM watchlist_funds
            WHERE user_id = ? AND code = ?
            """,
            (uid, c),
        ).fetchone()
    if not row:
        raise ValueError("save watchlist failed")
    item = _item_from_row(dict(row))

    # Warm sector cache once on add/update, then future reads can use DB cache directly.
    try:
        from backend.fund_sector_service import resolve_and_cache_fund_sector

        resolve_and_cache_fund_sector(
            c,
            fund_name=item.get("name") or nm,
        )
    except Exception:
        pass

    return item


def remove_watchlist(user_id: int, code: str) -> bool:
    uid = _norm_user_id(user_id)
    c = _norm_code(code)
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM watchlist_funds WHERE user_id = ? AND code = ?",
            (uid, c),
        )
    return int(cur.rowcount or 0) > 0


def analyze_fund(code: str, name: str = "") -> Dict[str, Any]:
    c = _norm_code(code)
    display_name = str(name or "").strip()

    # Lazy imports to avoid heavy init at module import time.
    try:
        from config import WATCH_FUNDS
    except Exception:
        WATCH_FUNDS = {}
    try:
        from backend.portfolio_service import fetch_fund_gz
    except Exception:
        fetch_fund_gz = None
    try:
        from strategy import generate_today_signal
    except Exception:
        generate_today_signal = None
    try:
        from sector import get_sector_by_fund, get_sector_sentiment
    except Exception:
        get_sector_by_fund = None
        get_sector_sentiment = None
    try:
        from ai_advisor import ask_deepseek_fund_decision
    except Exception:
        ask_deepseek_fund_decision = None

    if not display_name:
        cfg = WATCH_FUNDS.get(c, {})
        if isinstance(cfg, dict):
            display_name = str(cfg.get("name") or "").strip()

    latest: Dict[str, Any] = {}
    price = None
    pct = None

    if callable(fetch_fund_gz):
        try:
            gz = fetch_fund_gz(c) or {}
        except Exception:
            gz = {}
    else:
        gz = {}

    if gz.get("ok"):
        if not display_name:
            display_name = str(gz.get("name") or "").strip()
        latest = {
            "price": gz.get("nav"),
            "pct": gz.get("daily_change_pct"),
            "time": gz.get("gztime") or gz.get("jzrq"),
            "source": "fundgz",
        }
        price = latest.get("price")
        pct = latest.get("pct")
    else:
        # Keep analyze endpoint responsive: avoid heavy history fallback path here.
        latest = {
            "price": None,
            "pct": None,
            "time": "",
            "source": "unavailable",
        }
        price = latest.get("price")
        pct = latest.get("pct")

    price_f = _to_float_or_none(price)
    pct_f = _to_float_or_none(pct)

    signal: Dict[str, Any]
    if price_f is None:
        signal = {
            "action": "HOLD",
            "position_hint": "KEEP",
            "hit_level": None,
            "price_vs_base_pct": None,
            "reason": "暂时无法获取实时价格，建议观望",
            "grids": [],
            "base_price": None,
        }
    else:
        if callable(generate_today_signal):
            try:
                signal = generate_today_signal(c, price_f)
            except Exception as e:
                signal = {
                    "action": "HOLD",
                    "position_hint": "KEEP",
                    "hit_level": None,
                    "price_vs_base_pct": None,
                    "reason": f"策略计算失败: {type(e).__name__}",
                    "grids": [],
                    "base_price": None,
                }
        else:
            signal = {
                "action": "HOLD",
                "position_hint": "KEEP",
                "hit_level": None,
                "price_vs_base_pct": None,
                "reason": "策略模块未就绪，使用默认观望建议",
                "grids": [],
                "base_price": None,
            }

    if callable(get_sector_by_fund):
        try:
            sector_name = str(get_sector_by_fund(c) or "").strip()
        except Exception:
            sector_name = ""
    else:
        sector_name = ""

    sector_info: Dict[str, Any] = {
        "sector": sector_name or "未知板块",
        "score": 50,
        "level": "中性",
        "comment": "暂未获取到板块情绪数据。",
    }
    sector_live_enabled = (
        os.getenv("WATCHLIST_ANALYZE_SECTOR_LIVE", "0").strip() == "1"
    )
    if sector_live_enabled and callable(get_sector_sentiment):
        try:
            raw_sector_info = get_sector_sentiment(sector_name) or {}
            if isinstance(raw_sector_info, dict):
                sector_info = {
                    "sector": str(
                        raw_sector_info.get("sector") or sector_name or "未知板块"
                    ),
                    "score": _to_float_or_none(raw_sector_info.get("score")) or 50,
                    "level": str(raw_sector_info.get("level") or "中性"),
                    "comment": str(raw_sector_info.get("comment") or ""),
                }
        except Exception:
            pass

    ai_enabled = os.getenv("WATCHLIST_ANALYZE_USE_AI", "0").strip() == "1"
    ai: Dict[str, Any] = {
        "action": str(signal.get("action") or "HOLD"),
        "reason": "AI 分析未开启，已采用策略信号。",
    }
    if ai_enabled and callable(ask_deepseek_fund_decision):
        try:
            ai_resp = ask_deepseek_fund_decision(
                fund_name=display_name or c,
                code=c,
                latest={
                    "price": price_f,
                    "pct": pct_f,
                    "time": latest.get("time"),
                    "source": latest.get("source"),
                },
                quant_signal=signal,
                sector_info=sector_info,
                fund_profile=None,
            )
            if isinstance(ai_resp, dict):
                ai = {
                    "action": str(
                        ai_resp.get("action") or signal.get("action") or "HOLD"
                    ),
                    "reason": str(ai_resp.get("reason") or ""),
                }
        except Exception:
            ai = {
                "action": str(signal.get("action") or "HOLD"),
                "reason": "AI 分析暂不可用，已采用策略信号。",
            }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "code": c,
        "name": display_name or c,
        "latest": {
            "price": price_f,
            "pct": pct_f,
            "time": str(latest.get("time") or ""),
            "source": str(latest.get("source") or ""),
        },
        "signal": {
            "action": str(signal.get("action") or "HOLD"),
            "position_hint": str(signal.get("position_hint") or "KEEP"),
            "hit_level": signal.get("hit_level"),
            "price_vs_base_pct": signal.get("price_vs_base_pct"),
            "reason": str(signal.get("reason") or ""),
            "base_price": signal.get("base_price"),
            "grids": signal.get("grids") or [],
        },
        "sector": {
            "name": str(sector_info.get("sector") or sector_name or "未知板块"),
            "score": _to_float_or_none(sector_info.get("score")) or 50,
            "level": str(sector_info.get("level") or "中性"),
            "comment": str(sector_info.get("comment") or ""),
        },
        "ai_decision": {
            "action": str(ai.get("action") or signal.get("action") or "HOLD"),
            "reason": str(ai.get("reason") or ""),
        },
    }
