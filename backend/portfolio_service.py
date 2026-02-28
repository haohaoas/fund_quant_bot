# backend/portfolio_service.py
"""组合 / 账本服务（完整版）：
- 账户现金
- 当前持仓
- 交易流水
- 交易应用到持仓
- 手动录入基金净值（NAV）

说明：
- 本文件不依赖 `backend/sector.py`。
- 板块默认使用本地静态映射 FUND_TO_SECTOR；如果未配置则为“未知板块”。
- 基金名称/净值/涨幅默认使用 Eastmoney fundgz（1234567）接口；
  夜间优先切换到真实净值快照（akshare 开放式基金净值）并带多级兜底。
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import time
import sqlite3

import requests

from backend.db import get_conn, init_db

# ===== 板块映射（本地静态配置） =====
# 你可以按需把常用基金补到这里。
FUND_TO_SECTOR: Dict[str, str] = {
    # 示例："008888": "半导体",
    # 示例："015790": "半导体",
}


def get_sector_by_fund(code: str) -> str:
    c = str(code).strip()
    return FUND_TO_SECTOR.get(c, "未知板块")


# 确保数据库已初始化
init_db()

# ============================
# 基金实时估值/净值（Eastmoney 1234567 fundgz）
# ============================
# Example: https://fundgz.1234567.com.cn/js/015790.js -> jsonpgz({...});

_FUNDGZ_TTL_SECONDS = 30
_FUNDGZ_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_SETTLED_NAV_TTL_SECONDS = 900
_SETTLED_NAV_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_OPEN_FUND_DAILY_TTL_SECONDS = 600
_OPEN_FUND_DAILY_CACHE: Dict[str, Any] = {"ts": 0.0, "data": {}}
DEFAULT_ACCOUNT_ID = 1


def clear_fund_gz_cache(code: Optional[str] = None) -> None:
    c = str(code or "").strip()
    if c:
        _FUNDGZ_CACHE.pop(c, None)
        return
    _FUNDGZ_CACHE.clear()


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s in ("", "--", "-", "None", "nan", "NaN"):
            return None
        return float(s)
    except Exception:
        return None


def _norm_code6(code: str) -> str:
    digits = "".join(ch for ch in str(code or "").strip() if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return str(code or "").strip()


def _pick_col_contains(df: Any, keys: List[str]) -> Optional[str]:
    cols_obj = getattr(df, "columns", None)
    cols = list(cols_obj) if cols_obj is not None else []
    for c in cols:
        text = str(c)
        for k in keys:
            if k in text:
                return text
    return None


def _pick_row_value(row: Any, keys: List[str]) -> Any:
    for k in keys:
        try:
            v = row.get(k)
        except Exception:
            v = None
        if v is None:
            continue
        s = str(v).strip()
        if s in ("", "--", "-", "None", "nan", "NaN"):
            continue
        return v
    return None


def _parse_jsonp_obj(text: str) -> Dict[str, Any]:
    s = (text or "").strip()
    if not s:
        return {}
    # jsonpgz({...});
    if "(" in s and s.endswith(");"):
        try:
            s = s[s.find("(") + 1 : -2]
        except Exception:
            pass
    try:
        import json
        return json.loads(s)
    except Exception:
        return {}


def _parse_local_date(value: Any):
    s = str(value or "").strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def _is_nav_settled(gz: Dict[str, Any]) -> bool:
    jzrq_date = _parse_local_date(gz.get("jzrq"))
    if jzrq_date is None:
        return False

    def _prev_trade_day(d):
        cur = d - timedelta(days=1)
        while cur.weekday() >= 5:
            cur -= timedelta(days=1)
        return cur

    def _latest_expected_settled_date(now_dt: datetime):
        today = now_dt.date()
        # 周末/休市日：最新净值应至少到最近交易日
        if today.weekday() >= 5:
            cur = today
            while cur.weekday() >= 5:
                cur -= timedelta(days=1)
            return cur
        # 交易日晚间（默认20:00后）才期望“T日净值”可见
        if now_dt.hour >= 20:
            return today
        # 白天/傍晚：只期望到上一交易日
        return _prev_trade_day(today)

    expected = _latest_expected_settled_date(datetime.now())
    return jzrq_date >= expected


def _fetch_settled_nav_snapshot(code: str, jzrq: str) -> Dict[str, Any]:
    """
    对于“已结算净值”的基金，额外读取历史净值，拿到真实的 prev_nav 与日涨幅。
    返回:
      {
        "nav": float,
        "prev_nav": float|None,
        "daily_change_pct": float|None,
        "jzrq": "YYYY-MM-DD",
      }
    """
    c = str(code or "").strip()
    if not c:
        return {}
    target = str(jzrq or "").strip()
    key = f"{c}:{target or '-'}"
    now = time.time()
    cached = _SETTLED_NAV_CACHE.get(key)
    if cached and (now - cached[0]) <= _SETTLED_NAV_TTL_SECONDS:
        return cached[1]

    out: Dict[str, Any] = {}
    # 1) First try open-fund daily snapshot: typically refreshed after close.
    try:
        daily = _fetch_open_fund_daily_snapshot(c)
        d_nav = _safe_float((daily or {}).get("nav"))
        d_prev = _safe_float((daily or {}).get("prev_nav"))
        d_pct = _safe_float((daily or {}).get("daily_change_pct"))
        d_jzrq = str((daily or {}).get("jzrq") or "").strip()

        if d_nav is not None:
            if d_prev is None and d_pct is not None and d_pct > -100:
                try:
                    d_prev = d_nav / (1.0 + d_pct / 100.0)
                except Exception:
                    d_prev = None
            if d_pct is None and d_prev not in (None, 0):
                try:
                    d_pct = (float(d_nav) - float(d_prev)) / float(d_prev) * 100.0
                except Exception:
                    d_pct = None
            out = {
                "nav": float(d_nav),
                "prev_nav": float(d_prev) if d_prev is not None else None,
                "daily_change_pct": float(d_pct) if d_pct is not None else None,
                "jzrq": d_jzrq or target,
            }
            _SETTLED_NAV_CACHE[key] = (now, out)
            return out
    except Exception:
        pass

    # 2) Fallback to history series (stable but less timely).
    try:
        from data import get_fund_history  # 延迟导入，避免启动时重依赖

        df = get_fund_history(c, lookback_days=120)
        if df is None or getattr(df, "empty", True):
            _SETTLED_NAV_CACHE[key] = (now, out)
            return out
        if "date" not in df.columns or "close" not in df.columns:
            _SETTLED_NAV_CACHE[key] = (now, out)
            return out

        series: List[Tuple[Any, float]] = []
        for _, row in df.iterrows():
            raw_d = row.get("date")
            raw_v = row.get("close")
            d = None
            if hasattr(raw_d, "date"):
                try:
                    d = raw_d.date()
                except Exception:
                    d = None
            if d is None:
                d = _parse_local_date(raw_d)
            v = _safe_float(raw_v)
            if d is None or v is None:
                continue
            series.append((d, float(v)))

        if not series:
            _SETTLED_NAV_CACHE[key] = (now, out)
            return out
        series.sort(key=lambda x: x[0])

        target_d = _parse_local_date(target)
        idx = len(series) - 1
        if target_d is not None:
            found = False
            for i in range(len(series) - 1, -1, -1):
                if series[i][0] <= target_d:
                    idx = i
                    found = True
                    break
            if not found:
                idx = 0

        nav_date, nav_val = series[idx]
        prev_val = series[idx - 1][1] if idx > 0 else None
        pct = None
        if prev_val is not None and prev_val != 0:
            pct = (float(nav_val) - float(prev_val)) / float(prev_val) * 100.0

        out = {
            "nav": float(nav_val),
            "prev_nav": float(prev_val) if prev_val is not None else None,
            "daily_change_pct": float(pct) if pct is not None else None,
            "jzrq": str(nav_date),
        }
    except Exception:
        out = {}

    _SETTLED_NAV_CACHE[key] = (now, out)
    return out


def _fetch_open_fund_daily_snapshot(code: str) -> Dict[str, Any]:
    """
    Fetch settled NAV snapshot from akshare open-fund daily table.
    Returns:
      {"nav": float|None, "prev_nav": float|None, "daily_change_pct": float|None, "jzrq": str, "name": str}
    """
    c = _norm_code6(code)
    if not c:
        return {}

    now = time.time()
    cache_ts = float(_OPEN_FUND_DAILY_CACHE.get("ts") or 0.0)
    data_map = _OPEN_FUND_DAILY_CACHE.get("data") or {}
    if isinstance(data_map, dict) and (now - cache_ts) <= _OPEN_FUND_DAILY_TTL_SECONDS:
        return dict(data_map.get(c) or {})

    latest_map: Dict[str, Dict[str, Any]] = {}
    try:
        import akshare as ak  # type: ignore
    except Exception:
        _OPEN_FUND_DAILY_CACHE["ts"] = now
        _OPEN_FUND_DAILY_CACHE["data"] = latest_map
        return {}

    try:
        from backend.services.sector_flow_service import akshare_no_proxy
    except Exception:
        akshare_no_proxy = None

    try:
        fn = getattr(ak, "fund_open_fund_daily_em", None)
        if not callable(fn):
            _OPEN_FUND_DAILY_CACHE["ts"] = now
            _OPEN_FUND_DAILY_CACHE["data"] = latest_map
            return {}
        if callable(akshare_no_proxy):
            with akshare_no_proxy():
                df = fn()
        else:
            df = fn()
    except Exception:
        df = None

    try:
        if df is None or getattr(df, "empty", True):
            _OPEN_FUND_DAILY_CACHE["ts"] = now
            _OPEN_FUND_DAILY_CACHE["data"] = latest_map
            return {}
    except Exception:
        _OPEN_FUND_DAILY_CACHE["ts"] = now
        _OPEN_FUND_DAILY_CACHE["data"] = latest_map
        return {}

    code_col = _pick_col_contains(df, ["基金代码", "代码"])
    name_col = _pick_col_contains(df, ["基金简称", "基金名称", "名称"])
    nav_col = _pick_col_contains(df, ["单位净值", "最新净值", "净值"])
    pct_col = _pick_col_contains(df, ["日增长率", "日涨跌幅", "涨跌幅"])
    date_col = _pick_col_contains(df, ["净值日期", "日期", "更新"])

    if not code_col or not nav_col:
        _OPEN_FUND_DAILY_CACHE["ts"] = now
        _OPEN_FUND_DAILY_CACHE["data"] = latest_map
        return {}

    try:
        for _, row in df.iterrows():
            code_raw = row.get(code_col)
            cc = _norm_code6(str(code_raw or ""))
            if not cc:
                continue
            nav = _safe_float(row.get(nav_col))
            if nav is None:
                continue

            pct = _safe_float(row.get(pct_col)) if pct_col else None
            prev_nav = None
            if pct is not None and pct > -100:
                try:
                    prev_nav = float(nav) / (1.0 + float(pct) / 100.0)
                except Exception:
                    prev_nav = None

            jzrq = ""
            if date_col:
                jzrq = str(row.get(date_col) or "").strip()
            if not jzrq:
                jzrq = datetime.now().strftime("%Y-%m-%d")

            latest_map[cc] = {
                "nav": float(nav),
                "prev_nav": float(prev_nav) if prev_nav is not None else None,
                "daily_change_pct": float(pct) if pct is not None else None,
                "jzrq": jzrq,
                "name": str(row.get(name_col) or "").strip() if name_col else "",
            }
    except Exception:
        latest_map = {}

    _OPEN_FUND_DAILY_CACHE["ts"] = now
    _OPEN_FUND_DAILY_CACHE["data"] = latest_map
    return dict(latest_map.get(c) or {})


def _fallback_quote_from_settled_nav(code: str) -> Dict[str, Any]:
    c = str(code or "").strip()
    if not c:
        return {"ok": False, "error": "empty code"}
    snap = _fetch_settled_nav_snapshot(c, "")
    nav = _safe_float(snap.get("nav")) if snap else None
    if nav is None:
        return {"ok": False, "error": "settled snapshot unavailable"}
    prev_nav = _safe_float(snap.get("prev_nav")) if snap.get("prev_nav") is not None else None
    pct = _safe_float(snap.get("daily_change_pct")) if snap.get("daily_change_pct") is not None else None
    if pct is None and prev_nav not in (None, 0):
        try:
            pct = (float(nav) - float(prev_nav)) / float(prev_nav) * 100.0
        except Exception:
            pct = None

    name = ""
    try:
        from data import get_fund_name

        name = str(get_fund_name(c) or "").strip()
    except Exception:
        name = ""

    return {
        "ok": True,
        "code": c,
        "name": name,
        "nav": float(nav),
        "prev_nav": float(prev_nav) if prev_nav is not None else None,
        "daily_change_pct": float(pct) if pct is not None else None,
        "jzrq": str(snap.get("jzrq") or "").strip(),
        "gztime": "",
        "source": "settled_fallback",
    }


def fetch_fund_gz(code: str) -> Dict[str, Any]:
    """Fetch realtime/estimated fund info from Eastmoney fundgz."""
    c = str(code).strip()
    if not c:
        return {"ok": False, "error": "empty code"}

    now = time.time()
    cached = _FUNDGZ_CACHE.get(c)
    if cached and (now - cached[0]) <= _FUNDGZ_TTL_SECONDS:
        return cached[1]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://fund.eastmoney.com/",
        "Connection": "close",
    }

    url = f"https://fundgz.1234567.com.cn/js/{c}.js"

    sess = requests.Session()
    try:
        sess.trust_env = False
    except Exception:
        pass

    try:
        resp = sess.get(url, headers=headers, timeout=(5, 20), proxies={})
        if resp.status_code != 200:
            fb = _fallback_quote_from_settled_nav(c)
            if fb.get("ok"):
                _FUNDGZ_CACHE[c] = (now, fb)
                return fb
            out = {"ok": False, "error": f"HTTP {resp.status_code}"}
            _FUNDGZ_CACHE[c] = (now, out)
            return out

        resp.encoding = "utf-8"
        obj = _parse_jsonp_obj(resp.text)
        if not obj:
            fb = _fallback_quote_from_settled_nav(c)
            if fb.get("ok"):
                _FUNDGZ_CACHE[c] = (now, fb)
                return fb
            out = {"ok": False, "error": "empty json"}
            _FUNDGZ_CACHE[c] = (now, out)
            return out

        def _sf(v: Any) -> Optional[float]:
            return _safe_float(v)

        name = str(obj.get("name") or "").strip()
        gsz = _sf(obj.get("gsz"))
        dwjz = _sf(obj.get("dwjz"))
        gszzl = _sf(obj.get("gszzl"))

        nav = gsz if gsz is not None else dwjz
        prev_nav = dwjz

        out = {
            "ok": True,
            "code": c,
            "name": name,
            "nav": nav,
            "prev_nav": prev_nav,
            "daily_change_pct": gszzl,
            "jzrq": str(obj.get("jzrq") or "").strip(),
            "gztime": str(obj.get("gztime") or "").strip(),
            "source": "fundgz",
        }

        # Nightly/settled phase: replace estimate with settled NAV snapshot when available.
        try:
            if _is_nav_settled(out):
                snap = _fetch_settled_nav_snapshot(c, str(out.get("jzrq") or ""))
                if snap:
                    snap_nav = _safe_float(snap.get("nav"))
                    snap_prev = _safe_float(snap.get("prev_nav")) if snap.get("prev_nav") is not None else None
                    snap_pct = _safe_float(snap.get("daily_change_pct")) if snap.get("daily_change_pct") is not None else None
                    if snap_nav is not None:
                        out["nav"] = snap_nav
                    if snap_prev is not None:
                        out["prev_nav"] = snap_prev
                    if snap_pct is not None:
                        out["daily_change_pct"] = snap_pct
                    out["jzrq"] = str(snap.get("jzrq") or out.get("jzrq") or "").strip()
                    out["source"] = "settled_nav"
        except Exception:
            pass

        _FUNDGZ_CACHE[c] = (now, out)
        return out

    except Exception as e:
        fb = _fallback_quote_from_settled_nav(c)
        if fb.get("ok"):
            _FUNDGZ_CACHE[c] = (now, fb)
            return fb
        out = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        _FUNDGZ_CACHE[c] = (now, out)
        return out


def _infer_sector_from_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return ""

    kw_map = [
        ("半导体", "半导体"),
        ("芯片", "半导体"),
        ("机器人", "机器人"),
        ("军工", "军工"),
        ("医药", "医药"),
        ("医疗", "医药"),
        ("消费", "消费"),
        ("酒", "消费"),
        ("新能源", "新能源"),
        ("光伏", "新能源"),
        ("锂电", "新能源"),
        ("银行", "银行"),
        ("证券", "证券"),
        ("保险", "保险"),
        ("红利", "红利"),
        ("央企", "央企"),
        ("科技", "科技"),
        ("AI", "AI"),
        ("人工智能", "AI"),
        ("通信", "通信"),
        ("军民融合", "军工"),
    ]
    for kw, sector in kw_map:
        if kw in n:
            return sector
    return ""


def _get_sector_label(code: str, name: str = "") -> str:
    ov = get_sector_override(code)
    if ov:
        return ov

    # 优先使用板块缓存表（miss 时会拉取一次并回填）。
    try:
        from backend.fund_sector_service import resolve_and_cache_fund_sector

        cached_or_resolved = str(
            resolve_and_cache_fund_sector(
                str(code or "").strip(),
                fund_name=str(name or "").strip(),
                static_fallback=str(FUND_TO_SECTOR.get(str(code or "").strip()) or ""),
            )
            or ""
        ).strip()
        if cached_or_resolved:
            return cached_or_resolved
    except Exception:
        pass

    # 没覆盖才走你原来的逻辑：静态映射 / 名称推断 / 默认未知
    try:
        s = get_sector_by_fund(code)
        if s and s != "未知板块":
            return s
    except Exception:
        pass
    inferred = _infer_sector_from_name(name)
    return inferred or "未知板块"


def _round_or_none(x: Optional[float], nd: int = 2) -> Optional[float]:
    if x is None:
        return None
    try:
        return round(float(x), nd)
    except Exception:
        return None


def enrich_position(pos: Dict[str, Any]) -> Dict[str, Any]:
    code = str(pos.get("code") or "").strip()
    shares = _safe_float(pos.get("shares")) or 0.0
    cost = _safe_float(pos.get("cost")) or 0.0

    gz = fetch_fund_gz(code) if code else {"ok": False}
    name = str(gz.get("name") or "").strip() if gz.get("ok") else ""

    nav = _safe_float(gz.get("nav")) if gz.get("ok") else None
    prev_nav = _safe_float(gz.get("prev_nav")) if gz.get("ok") else None

    daily_change_pct = _safe_float(gz.get("daily_change_pct")) if gz.get("ok") else None
    if daily_change_pct is None and nav is not None and prev_nav is not None and prev_nav != 0:
        daily_change_pct = (float(nav) - float(prev_nav)) / float(prev_nav) * 100.0

    market_value = (shares * nav) if (nav is not None) else None
    holding_profit = (shares * (nav - cost)) if (nav is not None) else None
    holding_profit_pct = ((nav - cost) / cost * 100.0) if (nav is not None and cost > 0) else None

    daily_profit = (shares * (nav - prev_nav)) if (nav is not None and prev_nav is not None) else None
    jzrq = str(gz.get("jzrq") or "").strip() if gz.get("ok") else ""
    nav_settled = _is_nav_settled(gz) if gz.get("ok") else False

    # 若净值已结算，优先用历史净值回填真实日涨幅（避免继续显示估值涨幅）。
    if nav_settled and code:
        snap = _fetch_settled_nav_snapshot(code, jzrq)
        if snap:
            nav = _safe_float(snap.get("nav")) or nav
            prev_nav = _safe_float(snap.get("prev_nav")) if snap.get("prev_nav") is not None else prev_nav
            daily_change_pct = _safe_float(snap.get("daily_change_pct")) if snap.get("daily_change_pct") is not None else daily_change_pct
            jzrq = str(snap.get("jzrq") or jzrq).strip()
            market_value = (shares * nav) if (nav is not None) else market_value
            holding_profit = (shares * (nav - cost)) if (nav is not None) else holding_profit
            holding_profit_pct = ((nav - cost) / cost * 100.0) if (nav is not None and cost > 0) else holding_profit_pct
            daily_profit = (shares * (nav - prev_nav)) if (nav is not None and prev_nav is not None) else daily_profit

    sector_label = _get_sector_label(code, name)
    sector_pct = None
    try:
        from sector import get_sector_sentiment

        senti = get_sector_sentiment(sector_label) if sector_label else {}
        sector_pct = _safe_float((senti or {}).get("flow_pct"))
    except Exception:
        sector_pct = None

    out = dict(pos)
    out.update(
        {
            "name": name,
            "sector": sector_label,
            "sector_pct": _round_or_none(sector_pct, 2),
            "latest_nav": _round_or_none(nav, 6),
            "prev_nav": _round_or_none(prev_nav, 6),
            "daily_change_pct": _round_or_none(daily_change_pct, 2),
            "daily_profit": _round_or_none(daily_profit, 2),
            "market_value": _round_or_none(market_value, 2),
            "holding_profit": _round_or_none(holding_profit, 2),
            "holding_profit_pct": _round_or_none(holding_profit_pct, 2),
            "data_source": "fundgz" if gz.get("ok") else "",
            "jzrq": jzrq,
            "nav_settled": bool(nav_settled),
            "gztime": str(gz.get("gztime") or "").strip() if gz.get("ok") else "",
        }
    )
    return out


def _norm_account_id(account_id: Optional[int]) -> int:
    try:
        aid = int(account_id or DEFAULT_ACCOUNT_ID)
    except Exception:
        aid = DEFAULT_ACCOUNT_ID
    return aid if aid > 0 else DEFAULT_ACCOUNT_ID


def _norm_user_id(user_id: Optional[int]) -> int:
    try:
        uid = int(user_id or 1)
    except Exception:
        uid = 1
    return uid if uid > 0 else 1


def _account_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "user_id": int(row["user_id"] or 1),
        "name": str(row["name"] or ""),
        "avatar": str(row["avatar"] or ""),
        "cash": float(row["cash"] or 0.0),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def ensure_default_account_for_user(user_id: int) -> Dict[str, Any]:
    uid = _norm_user_id(user_id)
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, user_id, name, avatar, cash, created_at, updated_at
            FROM accounts
            WHERE user_id = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (uid,),
        ).fetchone()
        if row:
            return _account_from_row(dict(row))

        cur = conn.execute(
            """
            INSERT INTO accounts (user_id, name, avatar, cash, created_at, updated_at)
            VALUES (?, '默认账户', '', 0, datetime('now','localtime'), datetime('now','localtime'))
            """,
            (uid,),
        )
        aid = int(cur.lastrowid)
        created = conn.execute(
            """
            SELECT id, user_id, name, avatar, cash, created_at, updated_at
            FROM accounts
            WHERE id = ?
            """,
            (aid,),
        ).fetchone()
    if not created:
        raise ValueError("create default account failed")
    return _account_from_row(dict(created))


def resolve_account_id_for_user(account_id: Optional[int], user_id: int) -> int:
    uid = _norm_user_id(user_id)
    if account_id is None:
        return int(ensure_default_account_for_user(uid)["id"])

    aid = _norm_account_id(account_id)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM accounts WHERE id = ? AND user_id = ?",
            (aid, uid),
        ).fetchone()
    if not row:
        raise ValueError(f"account not found: {aid}")
    return int(row["id"])


def list_accounts(user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    uid = _norm_user_id(user_id) if user_id is not None else None
    with get_conn() as conn:
        if uid is None:
            rows = conn.execute(
                """
                SELECT id, user_id, name, avatar, cash, created_at, updated_at
                FROM accounts
                ORDER BY id ASC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, user_id, name, avatar, cash, created_at, updated_at
                FROM accounts
                WHERE user_id = ?
                ORDER BY id ASC
                """,
                (uid,),
            ).fetchall()
    return [_account_from_row(dict(r)) for r in rows]


def get_account(account_id: Optional[int] = None, user_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    uid = _norm_user_id(user_id) if user_id is not None else None
    with get_conn() as conn:
        if uid is None:
            aid = _norm_account_id(account_id)
            row = conn.execute(
                """
                SELECT id, user_id, name, avatar, cash, created_at, updated_at
                FROM accounts
                WHERE id = ?
                """,
                (aid,),
            ).fetchone()
        else:
            if account_id is None:
                row = conn.execute(
                    """
                    SELECT id, user_id, name, avatar, cash, created_at, updated_at
                    FROM accounts
                    WHERE user_id = ?
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (uid,),
                ).fetchone()
            else:
                aid = _norm_account_id(account_id)
                row = conn.execute(
                    """
                    SELECT id, user_id, name, avatar, cash, created_at, updated_at
                    FROM accounts
                    WHERE id = ? AND user_id = ?
                    """,
                    (aid, uid),
                ).fetchone()
    if not row:
        if uid is not None and account_id is None:
            return ensure_default_account_for_user(uid)
        return None
    return _account_from_row(dict(row))


def create_account(name: str, cash: float = 0.0, avatar: str = "", user_id: Optional[int] = None) -> Dict[str, Any]:
    account_name = str(name or "").strip()
    if not account_name:
        raise ValueError("account name required")
    avatar_text = str(avatar or "").strip()
    uid = _norm_user_id(user_id)

    with get_conn() as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO accounts (user_id, name, avatar, cash, created_at, updated_at)
                VALUES (?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
                """,
                (uid, account_name, avatar_text, float(cash)),
            )
        except sqlite3.IntegrityError as e:
            raise ValueError(f"account name already exists: {account_name}") from e
        account_id = int(cur.lastrowid)

    created = get_account(account_id, user_id=uid)
    if not created:
        raise ValueError("create account failed")
    return created


def update_account(
    account_id: int,
    *,
    name: Optional[str] = None,
    avatar: Optional[str] = None,
    user_id: Optional[int] = None,
) -> Dict[str, Any]:
    aid = _norm_account_id(account_id)
    uid = _norm_user_id(user_id) if user_id is not None else None
    current = get_account(aid, user_id=uid)
    if not current:
        raise ValueError(f"account not found: {aid}")

    new_name = current["name"] if name is None else str(name).strip()
    new_avatar = current["avatar"] if avatar is None else str(avatar).strip()
    if not new_name:
        raise ValueError("account name required")

    with get_conn() as conn:
        try:
            if uid is None:
                conn.execute(
                    """
                    UPDATE accounts
                    SET name = ?, avatar = ?, updated_at = datetime('now','localtime')
                    WHERE id = ?
                    """,
                    (new_name, new_avatar, aid),
                )
            else:
                conn.execute(
                    """
                    UPDATE accounts
                    SET name = ?, avatar = ?, updated_at = datetime('now','localtime')
                    WHERE id = ? AND user_id = ?
                    """,
                    (new_name, new_avatar, aid, uid),
                )
        except sqlite3.IntegrityError as e:
            raise ValueError(f"account name already exists: {new_name}") from e

    updated = get_account(aid, user_id=uid)
    if not updated:
        raise ValueError(f"account not found: {aid}")
    return updated


def get_account_cash(account_id: Optional[int] = None, user_id: Optional[int] = None) -> float:
    account = get_account(account_id, user_id=user_id)
    if not account:
        raise ValueError(f"account not found: {_norm_account_id(account_id)}")
    return float(account["cash"])


def set_account_cash(cash: float, account_id: Optional[int] = None, user_id: Optional[int] = None) -> None:
    aid = _norm_account_id(account_id)
    uid = _norm_user_id(user_id) if user_id is not None else None
    with get_conn() as conn:
        if uid is None:
            cur = conn.execute(
                """
                UPDATE accounts
                SET cash = ?, updated_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (float(cash), aid),
            )
        else:
            cur = conn.execute(
                """
                UPDATE accounts
                SET cash = ?, updated_at = datetime('now','localtime')
                WHERE id = ? AND user_id = ?
                """,
                (float(cash), aid, uid),
            )
    if int(cur.rowcount or 0) <= 0:
        raise ValueError(f"account not found: {aid}")


def list_positions(account_id: Optional[int] = None) -> List[Dict[str, Any]]:
    aid = _norm_account_id(account_id)
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT account_id, code, shares, cost, updated_at
            FROM positions
            WHERE account_id = ?
            ORDER BY code
            """,
            (aid,),
        ).fetchall()
    return [enrich_position(dict(r)) for r in rows]


def get_position(code: str, account_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    aid = _norm_account_id(account_id)
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT account_id, code, shares, cost, updated_at
            FROM positions
            WHERE account_id = ? AND code = ?
            """,
            (aid, code),
        ).fetchone()
    return enrich_position(dict(row)) if row else None


def _upsert_position(code: str, shares: float, cost: float, account_id: Optional[int] = None) -> None:
    aid = _norm_account_id(account_id)
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO positions (account_id, code, shares, cost, updated_at)
            VALUES (?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(account_id, code) DO UPDATE SET
                shares = excluded.shares,
                cost = excluded.cost,
                updated_at = datetime('now','localtime')
            """,
            (aid, code, float(shares), float(cost)),
        )


def _delete_position(code: str, account_id: Optional[int] = None) -> None:
    aid = _norm_account_id(account_id)
    with get_conn() as conn:
        conn.execute("DELETE FROM positions WHERE account_id = ? AND code = ?", (aid, code))


def remove_position(code: str, account_id: Optional[int] = None) -> Dict[str, Any]:
    aid = _norm_account_id(account_id)
    code = str(code).strip()
    if not code:
        return {"ok": False, "error": "code required"}

    pos = get_position(code, account_id=aid)
    if not pos:
        return {"ok": False, "error": "position not found", "code": code, "account_id": aid}

    _delete_position(code, account_id=aid)
    return {
        "ok": True,
        "account_id": aid,
        "code": code,
        "removed": {
            "shares": float(pos.get("shares") or 0.0),
            "cost": float(pos.get("cost") or 0.0),
        },
    }


def add_trade(
    code: str,
    action: str,
    amount: Optional[float] = None,
    price: Optional[float] = None,
    shares: Optional[float] = None,
    account_id: Optional[int] = None,
    note: str = "",
    ts: Optional[str] = None,
) -> int:
    ts = ts or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    action = action.upper()
    aid = _norm_account_id(account_id)

    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO trades (account_id, ts, code, action, amount, price, shares, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (aid, ts, code, action, amount, price, shares, note),
        )
        return int(cur.lastrowid)


def list_trades(limit: int = 50, account_id: Optional[int] = None) -> List[Dict[str, Any]]:
    aid = _norm_account_id(account_id)
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, account_id, ts, code, action, amount, price, shares, note
            FROM trades
            WHERE account_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (aid, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def apply_trade_to_portfolio(
    *,
    code: str,
    action: str,
    price: float,
    amount: Optional[float] = None,
    shares: Optional[float] = None,
    account_id: Optional[int] = None,
) -> Dict[str, Any]:
    action = action.upper()
    code = code.strip()
    aid = _norm_account_id(account_id)

    if price <= 0:
        return {"ok": False, "error": "price must be > 0"}

    if shares is None:
        if amount is None or amount <= 0:
            return {"ok": False, "error": "either shares or amount required"}
        shares = amount / price

    if shares <= 0:
        return {"ok": False, "error": "shares must be > 0"}

    cash = get_account_cash(account_id=aid)
    pos = get_position(code, account_id=aid)
    cur_shares = float(pos["shares"]) if pos else 0.0
    cur_cost = float(pos["cost"]) if pos else 0.0

    if action in ("BUY", "SIP"):
        spend = shares * price
        if cash < spend:
            return {"ok": False, "error": f"insufficient cash: need {spend:.2f}, have {cash:.2f}"}

        new_shares = cur_shares + shares
        new_cost = (cur_shares * cur_cost + shares * price) / new_shares if new_shares > 0 else price

        set_account_cash(cash - spend, account_id=aid)
        _upsert_position(code, new_shares, new_cost, account_id=aid)

        return {
            "ok": True,
            "account_id": aid,
            "action": action,
            "code": code,
            "shares_delta": shares,
            "cash_delta": -spend,
            "new_shares": new_shares,
            "new_cost": new_cost,
        }

    if action in ("SELL", "REDEEM"):
        if cur_shares <= 0:
            return {"ok": False, "error": "no position to sell"}

        if shares > cur_shares:
            shares = cur_shares

        income = shares * price
        new_shares = cur_shares - shares

        set_account_cash(cash + income, account_id=aid)

        if new_shares <= 1e-8:
            _delete_position(code, account_id=aid)
        else:
            _upsert_position(code, new_shares, cur_cost, account_id=aid)

        return {
            "ok": True,
            "account_id": aid,
            "action": action,
            "code": code,
            "shares_delta": -shares,
            "cash_delta": income,
            "new_shares": new_shares,
            "new_cost": cur_cost,
        }

    return {"ok": False, "error": f"unsupported action: {action}"}


def get_cashflow_summary(days: int = 7, account_id: Optional[int] = None) -> Dict[str, float]:
    aid = _norm_account_id(account_id)
    inflow = 0.0
    outflow = 0.0
    trades = 0

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT action, amount, price, shares
            FROM trades
            WHERE account_id = ? AND ts >= datetime('now', ?)
            """,
            (aid, f"-{int(days)} day"),
        ).fetchall()

    for r in rows:
        action = str(r["action"]).upper()
        value = None

        if r["amount"] is not None:
            value = float(r["amount"])
        elif r["price"] is not None and r["shares"] is not None:
            value = float(r["price"]) * float(r["shares"])

        if value is None:
            continue

        trades += 1

        if action in ("BUY", "SIP"):
            outflow += value
        elif action in ("SELL", "REDEEM"):
            inflow += value

    return {
        "inflow": round(inflow, 2),
        "outflow": round(outflow, 2),
        "net": round(inflow - outflow, 2),
        "trades": float(trades),
    }


def add_quote(code: str, nav: float) -> int:
    code = str(code).strip()
    nav = float(nav)

    if not code:
        raise ValueError("code required")
    if nav <= 0:
        raise ValueError("nav must be > 0")

    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO quotes(code, nav, ts) VALUES (?, ?, datetime('now','localtime'))",
            (code, nav),
        )
        return int(cur.lastrowid)


def get_latest_quote(code: str) -> Optional[Dict[str, Any]]:
    code = str(code).strip()
    if not code:
        return None

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT code, nav, ts
            FROM quotes
            WHERE code = ?
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            (code,),
        ).fetchone()

    if not row:
        return None

    return {"code": row["code"], "nav": float(row["nav"]), "ts": row["ts"]}


def get_account_asset_estimated(account_id: Optional[int] = None) -> Dict[str, Any]:
    cash = get_account_cash(account_id=account_id)
    positions = list_positions(account_id=account_id)
    mv = 0.0
    mv_missing = 0
    for p in positions:
        if p.get("market_value") is None:
            mv_missing += 1
            continue
        mv += float(p.get("market_value") or 0.0)
    return {
        "cash": round(float(cash), 2),
        "positions_value": round(float(mv), 2),
        "positions_missing": int(mv_missing),
        "total_asset": round(float(cash) + float(mv), 2),
    }

def get_sector_override(code: str) -> Optional[str]:
    code = (code or "").strip()
    if not code:
        return None
    with get_conn() as conn:
        row = conn.execute(
            "SELECT sector FROM sector_overrides WHERE code = ?",
            (code,)
        ).fetchone()
        return (row["sector"] if row else None)

def set_sector_override(code: str, sector: str) -> None:
    code = (code or "").strip()
    sector = (sector or "").strip()
    if not code:
        return
    with get_conn() as conn:
        if not sector:
            conn.execute("DELETE FROM sector_overrides WHERE code = ?", (code,))
        else:
            conn.execute(
                """
                INSERT INTO sector_overrides(code, sector, updated_at)
                VALUES(?, ?, datetime('now','localtime'))
                ON CONFLICT(code) DO UPDATE SET
                  sector=excluded.sector,
                  updated_at=excluded.updated_at
                """,
                (code, sector)
            )
