from __future__ import annotations

from datetime import datetime
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
        if callable(get_sector_by_fund):
            try:
                sector_name = str(get_sector_by_fund(code) or "").strip()
            except Exception:
                sector_name = ""
        else:
            sector_name = ""
        if sector_name and sector_name != "未知板块":
            item["sector_name"] = sector_name
        else:
            item["sector_name"] = ""

        if item["sector_name"] and callable(get_sector_sentiment):
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
    return _item_from_row(dict(row))


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
    from config import WATCH_FUNDS
    from data import get_fund_latest_price
    from backend.portfolio_service import fetch_fund_gz
    from strategy import generate_today_signal
    from sector import get_sector_by_fund, get_sector_sentiment
    from ai_advisor import ask_deepseek_fund_decision

    if not display_name:
        cfg = WATCH_FUNDS.get(c, {})
        if isinstance(cfg, dict):
            display_name = str(cfg.get("name") or "").strip()

    latest: Dict[str, Any] = {}
    price = None
    pct = None

    try:
        gz = fetch_fund_gz(c) or {}
    except Exception:
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
        latest = get_fund_latest_price(c) or {}
        price = latest.get("price")
        pct = latest.get("pct")

    signal: Dict[str, Any]
    if price is None:
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
        try:
            signal = generate_today_signal(c, float(price))
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

    sector_name = get_sector_by_fund(c)
    sector_info = get_sector_sentiment(sector_name)

    ai = ask_deepseek_fund_decision(
        fund_name=display_name or c,
        code=c,
        latest={
            "price": price,
            "pct": pct,
            "time": latest.get("time"),
            "source": latest.get("source"),
        },
        quant_signal=signal,
        sector_info=sector_info,
        fund_profile=None,
    )

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "code": c,
        "name": display_name or c,
        "latest": {
            "price": float(price) if price is not None else None,
            "pct": float(pct) if pct is not None else None,
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
            "score": float(sector_info.get("score") or 50),
            "level": str(sector_info.get("level") or "中性"),
            "comment": str(sector_info.get("comment") or ""),
        },
        "ai_decision": {
            "action": str(ai.get("action") or signal.get("action") or "HOLD"),
            "reason": str(ai.get("reason") or ""),
        },
    }
