# sector.py
"""
板块信息与情绪评分模块（工程优化版）

定位（非常重要）：
- 提供【板块背景/倾向】信息（低频、可解释）
- 用于：日志解释 / LLM 上下文
- ❌ 不作为交易信号
- ❌ 不参与择时或仓位判断
"""

from typing import Dict, Any, Optional
import time

try:
    import akshare as ak
except Exception:
    ak = None


# === 基金 -> 板块映射（最小维护集） ===
FUND_TO_SECTOR: Dict[str, str] = {
    "008888": "半导体",
    "014881": "机器人",
    "018125": "先进制造",
    "013238": "通信设备",
    "015790": "航空航天",
}

def get_sector_by_fund(code: str) -> str:
    c = str(code or "").strip()
    if not c:
        return "未知板块"

    # Highest priority: manual override.
    try:
        from backend.portfolio_service import get_sector_override

        ov = str(get_sector_override(c) or "").strip()
        if ov:
            return ov
    except Exception:
        pass

    static_sector = str(FUND_TO_SECTOR.get(c) or "").strip()

    # First priority: cached table (and miss->resolve once->write cache).
    try:
        from backend.fund_sector_service import (
            get_cached_fund_sector,
            resolve_and_cache_fund_sector,
        )

        cached = get_cached_fund_sector(c) or {}
        cached_sector = str(cached.get("sector") or "").strip()
        if cached_sector:
            return cached_sector

        inferred = str(
            resolve_and_cache_fund_sector(c, static_fallback=static_sector) or ""
        ).strip()
        if inferred:
            return inferred
    except Exception:
        pass

    # Fallback: static mapping.
    if static_sector:
        return static_sector

    return "未知板块"


# === 行业资金流缓存（低频） ===
_FLOW_CACHE = {"ts": 0.0, "df": None}
_FLOW_CACHE_TTL = 120  # 秒
_BOARD_PCT_CACHE = {"ts": 0.0, "map": {}}
_BOARD_PCT_CACHE_TTL = 120  # 秒


def _safe_float(x, default: float = 0.0) -> float:
    try:
        s = str(x).strip().replace(",", "")
        if not s or s in {"--", "-", "nan", "None"}:
            return default
        if s.endswith("亿"):
            return float(s[:-1]) * 1e8
        if s.endswith("万"):
            return float(s[:-1]) * 1e4
        return float(s)
    except Exception:
        return default


def _norm_sector_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for token in ("板块", "概念", "行业", "主题", "产业", "赛道", "指数"):
        text = text.replace(token, "")
    return text.strip()


def _pick_col(df, keys):
    for c in getattr(df, "columns", []):
        for k in keys:
            if k in str(c):
                return c
    return None


def _get_sector_flow_df():
    if ak is None:
        return None

    now = time.time()
    if _FLOW_CACHE["df"] is not None and (now - _FLOW_CACHE["ts"]) <= _FLOW_CACHE_TTL:
        return _FLOW_CACHE["df"]

    for fn_name in ("stock_sector_fund_flow_rank", "stock_sector_fund_flow_summary"):
        fn = getattr(ak, fn_name, None)
        if not fn:
            continue
        try:
            df = fn()
            if df is not None and len(df) > 0:
                _FLOW_CACHE["ts"] = now
                _FLOW_CACHE["df"] = df
                return df
        except Exception:
            pass

    return None


def _get_sector_board_pct_map() -> Dict[str, float]:
    if ak is None:
        return {}

    now = time.time()
    cached = _BOARD_PCT_CACHE.get("map")
    if isinstance(cached, dict) and cached and (now - _BOARD_PCT_CACHE.get("ts", 0.0)) <= _BOARD_PCT_CACHE_TTL:
        return cached

    try:
        from backend.services.sector_flow_service import akshare_no_proxy
    except Exception:
        akshare_no_proxy = None

    def _load_df(fn_name: str):
        fn = getattr(ak, fn_name, None)
        if not callable(fn):
            return None
        try:
            if callable(akshare_no_proxy):
                with akshare_no_proxy():
                    return fn()
            return fn()
        except Exception:
            return None

    result: Dict[str, float] = {}
    for fn_name in ("stock_board_industry_name_em", "stock_board_concept_name_em"):
        df = _load_df(fn_name)
        if df is None or len(df) == 0:
            continue
        for _, row in df.iterrows():
            name = str(row.get("板块名称") or row.get("名称") or row.get("行业") or row.get("概念") or "").strip()
            if not name:
                continue
            pct = _safe_float(row.get("涨跌幅") if "涨跌幅" in df.columns else row.get("涨跌"), default=None)
            if pct is None:
                continue
            result[name] = float(pct)
            norm = _norm_sector_text(name)
            if norm and norm not in result:
                result[norm] = float(pct)

    _BOARD_PCT_CACHE["ts"] = now
    _BOARD_PCT_CACHE["map"] = result
    return result


def _lookup_sector_board_pct(board_map: Dict[str, float], sector: str) -> Optional[float]:
    key = str(sector or "").strip()
    if not key:
        return None
    if key in board_map:
        return board_map[key]
    norm = _norm_sector_text(key)
    if norm in board_map:
        return board_map[norm]
    for cand, pct in board_map.items():
        if key in cand or cand in key:
            return pct
        c_norm = _norm_sector_text(cand)
        if norm and c_norm and (norm in c_norm or c_norm in norm):
            return pct
    return None


def _lookup_sector_flow(df, sector: str) -> Optional[Dict[str, Any]]:
    if df is None:
        return None

    name_col = _pick_col(df, ["行业", "板块", "概念", "名称"])
    inflow_col = _pick_col(df, ["主力净流入", "净流入"])
    pct_col = _pick_col(df, ["涨跌幅", "涨跌"])

    if not name_col or not inflow_col:
        return None

    target = sector.strip()
    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if target and name and (target in name or name in target):
            return {
                "name": name,
                "inflow": _safe_float(row.get(inflow_col)),
                "pct": _safe_float(row.get(pct_col)) if pct_col else 0.0,
            }
    return None


def _flow_to_score(inflow: float, pct: float) -> int:
    """
    低频、背景型评分（刻意钝化）
    不是择时指标，只是“偏强 / 偏弱”的解释器
    """
    score = 50.0
    yi = inflow / 1e8

    if yi >= 30:
        score += 18
    elif yi >= 10:
        score += 10
    elif yi <= -30:
        score -= 18
    elif yi <= -10:
        score -= 10

    if pct >= 3:
        score += 6
    elif pct <= -3:
        score -= 6

    return int(max(0, min(100, round(score))))


def get_sector_sentiment(sector: str) -> Dict[str, Any]:
    sector = str(sector).strip()
    board_map = _get_sector_board_pct_map()

    df = _get_sector_flow_df()
    alias_map = {
        "AI应用": ["人工智能", "AIGC", "ChatGPT", "算力"],
        "影视院线": ["影视传媒", "文化传媒"],
    }
    candidates = [sector]
    candidates.extend(alias_map.get(sector, []))

    hit = None
    board_pct = None
    for cand in candidates:
        if board_pct is None:
            board_pct = _lookup_sector_board_pct(board_map, cand)
        hit = _lookup_sector_flow(df, cand)
        if hit:
            break

    flow_inflow = None
    flow_pct = None
    if hit:
        flow_inflow = float(hit["inflow"])
        flow_pct = float(hit["pct"]) if hit.get("pct") is not None else board_pct
        score = _flow_to_score(flow_inflow, flow_pct)
    elif board_pct is not None:
        flow_inflow = 0.0
        flow_pct = float(board_pct)
        score = _flow_to_score(flow_inflow, flow_pct)
    else:
        base_scores = {
            "半导体": 48,
            "机器人": 55,
            "通信设备": 52,
            "先进制造": 50,
            "航空航天": 52,
        }
        score = base_scores.get(sector, 50)

    if score >= 70:
        level = "强"
        comment = "板块背景偏强，但仍需结合价格与资金判断。"
    elif score >= 60:
        level = "偏强"
        comment = "板块背景向好，适合顺势关注。"
    elif score >= 50:
        level = "中性"
        comment = "多空均衡，作为背景参考。"
    elif score >= 40:
        level = "偏弱"
        comment = "资金情绪偏谨慎，背景略弱。"
    else:
        level = "弱"
        comment = "板块背景承压，需谨慎。"

    if hit:
        comment += f"（{hit['name']}：主力净流入 {flow_inflow/1e8:.1f} 亿，涨跌幅 {flow_pct:.2f}%）"
    elif board_pct is not None:
        comment += f"（板块涨跌幅 {board_pct:.2f}%）"

    return {
        "sector": sector,
        "score": score,
        "level": level,
        "comment": comment,
        "flow_inflow": flow_inflow,
        "flow_pct": flow_pct,
    }
