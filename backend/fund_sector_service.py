from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from backend.db import get_conn, init_db

try:
    import akshare as ak  # type: ignore
except Exception:
    ak = None


init_db()

_ENABLE_HOLDING_INFER = os.getenv("FUND_SECTOR_BY_HOLDINGS", "1").strip() == "1"
_PROFILE_TTL_SECONDS = int(os.getenv("FUND_SECTOR_PROFILE_TTL_SECONDS", "86400"))
_STOCK_SECTOR_TTL_SECONDS = int(os.getenv("STOCK_SECTOR_TTL_SECONDS", "2592000"))
_FUND_SECTOR_CACHE_TTL_SECONDS = int(
    os.getenv("FUND_SECTOR_CACHE_TTL_SECONDS", "2592000")
)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _norm_fund_code(code: str) -> str:
    return str(code or "").strip()


def _norm_stock_code(code: str) -> str:
    c = str(code or "").strip()
    if not c:
        return ""
    digits = "".join(ch for ch in c if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return c


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if not s:
            return default
        if s.endswith("%"):
            s = s[:-1].strip()
        return float(s)
    except Exception:
        return default


def _pick_col(df: Any, keys: List[str]) -> Optional[str]:
    cols = list(getattr(df, "columns", []) or [])
    for c in cols:
        text = str(c)
        for k in keys:
            if k in text:
                return text
    return None


def _parse_ts(text: str) -> Optional[datetime]:
    s = str(text or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def _is_fresh(ts: str, ttl_seconds: int) -> bool:
    dt = _parse_ts(ts)
    if dt is None:
        return False
    age = (datetime.now() - dt).total_seconds()
    return age <= max(0, int(ttl_seconds))


def _load_stock_sector(stock_code: str) -> Optional[Tuple[str, str]]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT sector, updated_at
            FROM stock_sector_map
            WHERE stock_code = ?
            """,
            (stock_code,),
        ).fetchone()
    if not row:
        return None
    return (str(row["sector"] or "").strip(), str(row["updated_at"] or "").strip())


def _save_stock_sector(stock_code: str, sector: str, source: str) -> None:
    sec = str(sector or "").strip()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO stock_sector_map (stock_code, sector, source, updated_at)
            VALUES (?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(stock_code) DO UPDATE SET
                sector = excluded.sector,
                source = excluded.source,
                updated_at = datetime('now','localtime')
            """,
            (stock_code, sec, str(source or "").strip()),
        )


def _load_fund_profile(fund_code: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT fund_code, dominant_sector, sector_weights_json, holdings_json, source, updated_at
            FROM fund_sector_profile
            WHERE fund_code = ?
            """,
            (fund_code,),
        ).fetchone()
    if not row:
        return None

    def _loads_safe(raw: Any, fallback: Any) -> Any:
        try:
            return json.loads(str(raw or ""))
        except Exception:
            return fallback

    return {
        "fund_code": str(row["fund_code"] or "").strip(),
        "dominant_sector": str(row["dominant_sector"] or "").strip(),
        "sector_weights": _loads_safe(row["sector_weights_json"], {}),
        "holdings": _loads_safe(row["holdings_json"], []),
        "source": str(row["source"] or "").strip(),
        "updated_at": str(row["updated_at"] or "").strip(),
    }


def get_cached_fund_sector(code: str) -> Optional[Dict[str, str]]:
    c = _norm_fund_code(code)
    if not c:
        return None
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT fund_code, sector, source, updated_at
            FROM fund_sector_cache
            WHERE fund_code = ?
            """,
            (c,),
        ).fetchone()
    if not row:
        return None
    return {
        "fund_code": str(row["fund_code"] or "").strip(),
        "sector": str(row["sector"] or "").strip(),
        "source": str(row["source"] or "").strip(),
        "updated_at": str(row["updated_at"] or "").strip(),
    }


def set_cached_fund_sector(code: str, sector: str, source: str) -> None:
    c = _norm_fund_code(code)
    s = str(sector or "").strip()
    if not c:
        return
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO fund_sector_cache (fund_code, sector, source, updated_at)
            VALUES (?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(fund_code) DO UPDATE SET
                sector = excluded.sector,
                source = excluded.source,
                updated_at = datetime('now','localtime')
            """,
            (c, s, str(source or "").strip()),
        )


def _save_fund_profile(
    fund_code: str,
    dominant_sector: str,
    sector_weights: Dict[str, float],
    holdings: List[Dict[str, Any]],
    source: str,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO fund_sector_profile (
                fund_code, dominant_sector, sector_weights_json, holdings_json, source, updated_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(fund_code) DO UPDATE SET
                dominant_sector = excluded.dominant_sector,
                sector_weights_json = excluded.sector_weights_json,
                holdings_json = excluded.holdings_json,
                source = excluded.source,
                updated_at = datetime('now','localtime')
            """,
            (
                fund_code,
                str(dominant_sector or "").strip(),
                json.dumps(sector_weights or {}, ensure_ascii=False),
                json.dumps(holdings or [], ensure_ascii=False),
                str(source or "").strip(),
            ),
        )


def _fetch_fund_top_holdings(code: str, top_n: int = 10) -> List[Dict[str, Any]]:
    if ak is None:
        return []

    fn = getattr(ak, "fund_portfolio_hold_em", None)
    if not callable(fn):
        return []

    current_year = datetime.now().year
    years = [str(current_year), str(current_year - 1), str(current_year - 2)]
    frames: List[Any] = []

    for y in years:
        df = None
        for kwargs in (
            {"symbol": code, "date": y},
            {"fund": code, "date": y},
            {"symbol": code},
            {"fund": code},
        ):
            try:
                df = fn(**kwargs)
            except Exception:
                continue
            if df is not None and len(df) > 0:
                frames.append(df)
                break
        if frames:
            break

    if not frames:
        return []
    df = frames[0]

    code_col = _pick_col(df, ["股票代码", "证券代码", "成分券代码", "代码"])
    name_col = _pick_col(df, ["股票名称", "证券名称", "成分券名称", "名称"])
    weight_col = _pick_col(df, ["占净值比例", "占净值比", "持仓占比", "占基金净值比"])
    period_col = _pick_col(df, ["季度", "报告期", "截止日期", "披露日期"])

    if not code_col or not weight_col:
        return []

    # Use latest disclosed period first.
    latest_period = ""
    if period_col:
        for _, r in df.iterrows():
            p = str(r.get(period_col, "")).strip()
            if p and p > latest_period:
                latest_period = p

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        if period_col and latest_period:
            p = str(r.get(period_col, "")).strip()
            if p != latest_period:
                continue

        stock_code = _norm_stock_code(r.get(code_col))
        if not stock_code:
            continue
        weight_pct = _safe_float(r.get(weight_col), 0.0)
        if weight_pct <= 0:
            continue
        stock_name = str(r.get(name_col, "")).strip() if name_col else ""
        rows.append(
            {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "weight_pct": weight_pct,
            }
        )

    rows.sort(key=lambda x: float(x.get("weight_pct") or 0.0), reverse=True)
    return rows[: max(1, int(top_n))]


def _fetch_stock_sector_from_ak(stock_code: str) -> Tuple[str, str]:
    if ak is None:
        return ("", "")

    fn = getattr(ak, "stock_individual_info_em", None)
    if not callable(fn):
        return ("", "")

    try:
        df = fn(symbol=stock_code)
    except Exception:
        return ("", "")

    if df is None or len(df) == 0:
        return ("", "")

    # Expected shape: item/value
    key_col = _pick_col(df, ["item", "项目", "字段"])
    val_col = _pick_col(df, ["value", "值", "内容"])
    if not key_col or not val_col:
        cols = list(getattr(df, "columns", []) or [])
        if len(cols) >= 2:
            key_col, val_col = str(cols[0]), str(cols[1])
    if not key_col or not val_col:
        return ("", "")

    for _, r in df.iterrows():
        k = str(r.get(key_col, "")).strip()
        if ("行业" in k) or ("所属行业" in k):
            v = str(r.get(val_col, "")).strip()
            if v and v != "--":
                return (v, "ak.stock_individual_info_em")
    return ("", "")


def get_stock_sector(stock_code: str) -> str:
    c = _norm_stock_code(stock_code)
    if not c:
        return ""

    row = _load_stock_sector(c)
    if row:
        sector, updated_at = row
        if sector and _is_fresh(updated_at, _STOCK_SECTOR_TTL_SECONDS):
            return sector

    sector, source = _fetch_stock_sector_from_ak(c)
    if sector:
        _save_stock_sector(c, sector, source or "ak")
        return sector

    if row:
        # Use stale mapping when online refresh failed.
        return row[0]
    return ""


def get_fund_sector_profile(code: str, refresh: bool = False) -> Dict[str, Any]:
    c = _norm_fund_code(code)
    if not c:
        return {
            "fund_code": "",
            "dominant_sector": "",
            "sector_weights": {},
            "holdings": [],
            "source": "",
            "updated_at": "",
        }

    cached = _load_fund_profile(c)
    if (
        cached
        and not refresh
        and _is_fresh(str(cached.get("updated_at") or ""), _PROFILE_TTL_SECONDS)
    ):
        return cached

    if not _ENABLE_HOLDING_INFER:
        return cached or {
            "fund_code": c,
            "dominant_sector": "",
            "sector_weights": {},
            "holdings": [],
            "source": "disabled",
            "updated_at": _now_str(),
        }

    holdings = _fetch_fund_top_holdings(c, top_n=10)
    if not holdings:
        return cached or {
            "fund_code": c,
            "dominant_sector": "",
            "sector_weights": {},
            "holdings": [],
            "source": "no_holdings",
            "updated_at": _now_str(),
        }

    weights: Dict[str, float] = {}
    enriched_holdings: List[Dict[str, Any]] = []
    for h in holdings:
        sc = _norm_stock_code(h.get("stock_code"))
        sec = get_stock_sector(sc) or "其他"
        wp = _safe_float(h.get("weight_pct"), 0.0)
        if wp <= 0:
            continue
        weights[sec] = weights.get(sec, 0.0) + wp
        enriched_holdings.append(
            {
                "stock_code": sc,
                "stock_name": str(h.get("stock_name") or "").strip(),
                "weight_pct": round(wp, 4),
                "sector": sec,
            }
        )

    if not weights:
        return cached or {
            "fund_code": c,
            "dominant_sector": "",
            "sector_weights": {},
            "holdings": enriched_holdings,
            "source": "no_sector_mapping",
            "updated_at": _now_str(),
        }

    dominant_sector = max(weights.keys(), key=lambda s: float(weights.get(s, 0.0)))
    rounded_weights = {
        k: round(float(v), 4)
        for k, v in sorted(weights.items(), key=lambda x: x[1], reverse=True)
    }

    profile = {
        "fund_code": c,
        "dominant_sector": dominant_sector,
        "sector_weights": rounded_weights,
        "holdings": enriched_holdings,
        "source": "top10_holdings_weighted",
        "updated_at": _now_str(),
    }

    try:
        _save_fund_profile(
            fund_code=c,
            dominant_sector=dominant_sector,
            sector_weights=rounded_weights,
            holdings=enriched_holdings,
            source="top10_holdings_weighted",
        )
    except Exception:
        pass

    return profile


def _infer_sector_from_fund_name(name: str) -> str:
    n = str(name or "").strip()
    if not n:
        return ""
    kw_map = [
        ("半导体", "半导体"),
        ("芯片", "半导体"),
        ("光伏", "新能源"),
        ("锂电", "新能源"),
        ("新能源", "新能源"),
        ("机器人", "机器人"),
        ("人工智能", "AI应用"),
        ("AI", "AI应用"),
        ("算力", "AI应用"),
        ("传媒", "中证传媒"),
        ("通信", "5G通信"),
        ("油气", "油气产业"),
        ("军工", "商业航天"),
        ("航天", "商业航天"),
        ("有色", "有色金属"),
        ("黄金", "沪港深黄金"),
        ("纳指", "纳指100"),
        ("中证1000", "中证1000"),
        ("创业板", "创业板"),
        ("沪深300", "沪深300"),
    ]
    upper_n = n.upper()
    for kw, sector in kw_map:
        if kw.isupper():
            if kw in upper_n:
                return sector
        else:
            if kw in n:
                return sector
    return ""


def _get_fund_name_quick(code: str, fallback_name: str = "") -> str:
    name = str(fallback_name or "").strip()
    if name:
        return name
    c = _norm_fund_code(code)
    if not c:
        return ""
    try:
        from backend.portfolio_service import fetch_fund_gz

        gz = fetch_fund_gz(c) or {}
        if gz.get("ok"):
            nm = str(gz.get("name") or "").strip()
            if nm:
                return nm
    except Exception:
        pass
    try:
        from data import get_fund_name

        nm = str(get_fund_name(c) or "").strip()
        if nm:
            return nm
    except Exception:
        pass
    return ""


def resolve_and_cache_fund_sector(
    code: str,
    *,
    fund_name: str = "",
    static_fallback: str = "",
    force_refresh: bool = False,
) -> str:
    c = _norm_fund_code(code)
    if not c:
        return ""

    cached = get_cached_fund_sector(c)
    if cached and not force_refresh:
        cached_sector = str(cached.get("sector") or "").strip()
        # "未知板块" is treated as unresolved and should be retried.
        if (
            cached_sector
            and cached_sector != "未知板块"
            and _is_fresh(
                str(cached.get("updated_at") or ""),
                _FUND_SECTOR_CACHE_TTL_SECONDS,
            )
        ):
            return cached_sector

    # Step 1: weighted holdings.
    sector = infer_fund_sector(c, refresh=force_refresh)
    if sector:
        set_cached_fund_sector(c, sector, "top10_holdings_weighted")
        return sector

    # Step 2: fallback static map.
    sf = str(static_fallback or "").strip()
    if sf:
        set_cached_fund_sector(c, sf, "static_mapping")
        return sf

    # Step 3: fallback fund name keyword.
    nm = _get_fund_name_quick(c, fallback_name=fund_name)
    by_name = _infer_sector_from_fund_name(nm)
    if by_name:
        set_cached_fund_sector(c, by_name, "fund_name_keyword")
        return by_name

    # Preserve stale cached value when fresh resolve failed.
    if cached:
        return str(cached.get("sector") or "").strip()
    set_cached_fund_sector(c, "未知板块", "unresolved")
    return "未知板块"


def infer_fund_sector(code: str, refresh: bool = False) -> str:
    profile = get_fund_sector_profile(code, refresh=refresh)
    sector = str(profile.get("dominant_sector") or "").strip()
    return sector
