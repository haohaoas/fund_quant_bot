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


def _get_fund_name_quick(code: str) -> str:
    c = str(code or "").strip()
    if not c:
        return ""
    # First try fast quote path.
    try:
        from backend.portfolio_service import fetch_fund_gz

        gz = fetch_fund_gz(c) or {}
        if gz.get("ok"):
            nm = str(gz.get("name") or "").strip()
            if nm:
                return nm
    except Exception:
        pass

    # Fallback: local name map.
    try:
        from data import get_fund_name

        nm = str(get_fund_name(c) or "").strip()
        if nm:
            return nm
    except Exception:
        pass
    return ""


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

    # First priority: infer by top holdings (stock-sector weighted).
    try:
        from backend.fund_sector_service import infer_fund_sector

        inferred = str(infer_fund_sector(c) or "").strip()
        if inferred:
            return inferred
    except Exception:
        pass

    # Fallback #1: static mapping.
    static_sector = str(FUND_TO_SECTOR.get(c) or "").strip()
    if static_sector:
        return static_sector

    # Fallback #2: infer from fund name keywords.
    try:
        name = _get_fund_name_quick(c)
        inferred_from_name = _infer_sector_from_fund_name(name)
        if inferred_from_name:
            return inferred_from_name
    except Exception:
        pass

    return "未知板块"


# === 行业资金流缓存（低频） ===
_FLOW_CACHE = {"ts": 0.0, "df": None}
_FLOW_CACHE_TTL = 120  # 秒


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

    df = _get_sector_flow_df()
    hit = _lookup_sector_flow(df, sector)

    flow_inflow = None
    flow_pct = None
    if hit:
        flow_inflow = float(hit["inflow"])
        flow_pct = float(hit["pct"])
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

    return {
        "sector": sector,
        "score": score,
        "level": level,
        "comment": comment,
        "flow_inflow": flow_inflow,
        "flow_pct": flow_pct,
    }
