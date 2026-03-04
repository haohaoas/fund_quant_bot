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

from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
import os
import random
import time
import sqlite3
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

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
_FUNDGZ_SETTLED_TTL_SECONDS = int(os.getenv("FUNDGZ_SETTLED_TTL_SECONDS", "180"))
_FUNDGZ_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_FUNDGZ_CACHE_MAX = int(os.getenv("FUNDGZ_CACHE_MAX", "300"))
_FUNDGZ_CONNECT_TIMEOUT_SECONDS = 0.4
_FUNDGZ_READ_TIMEOUT_SECONDS = 0.8
_SETTLED_NAV_TTL_SECONDS = 900
_SETTLED_NAV_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_SETTLED_NAV_CACHE_MAX = int(os.getenv("SETTLED_NAV_CACHE_MAX", "500"))
_OPEN_FUND_DAILY_TTL_SECONDS = 600
_OPEN_FUND_DAILY_CACHE: Dict[str, Any] = {"ts": 0.0, "data": {}}
_ETF_SPOT_TTL_SECONDS = 120
_ETF_SPOT_CACHE: Dict[str, Any] = {"ts": 0.0, "data": {}}
_SETTLED_SWITCH_HOUR = int(os.getenv("SETTLED_SWITCH_HOUR", "19"))
DEFAULT_ACCOUNT_ID = 1
_QUOTE_SOURCE_MODES = {"auto", "tiantian", "fund123", "eastmoney", "baidu"}
_QUOTE_SOURCE_ALIASES = {
    "estimate": "tiantian",
    "fund123": "fund123",
    "settled": "eastmoney",
    "eastmoney_settled": "eastmoney",
}
_PORTFOLIO_ENRICH_TIMEOUT_SECONDS = 8.0
_PORTFOLIO_ENRICH_MAX_WORKERS = int(os.getenv("PORTFOLIO_ENRICH_MAX_WORKERS", "3"))
_FUND123_CONNECT_TIMEOUT_SECONDS = float(os.getenv("FUND123_CONNECT_TIMEOUT_SECONDS", "0.8"))
_FUND123_READ_TIMEOUT_SECONDS = float(os.getenv("FUND123_READ_TIMEOUT_SECONDS", "1.5"))
_FUND123_BOOTSTRAP_TTL_SECONDS = float(os.getenv("FUND123_BOOTSTRAP_TTL_SECONDS", "1200"))
_FUND123_KEY_TTL_SECONDS = float(os.getenv("FUND123_KEY_TTL_SECONDS", "86400"))
_FUND123_FAIL_TTL_SECONDS = float(os.getenv("FUND123_FAIL_TTL_SECONDS", "45"))
_FUND123_TREND_TTL_SECONDS = float(os.getenv("FUND123_TREND_TTL_SECONDS", "20"))
_FUND123_KEY_CACHE_MAX = int(os.getenv("FUND123_KEY_CACHE_MAX", "300"))
_FUND123_FAIL_CACHE_MAX = int(os.getenv("FUND123_FAIL_CACHE_MAX", "300"))
_FUND123_TREND_CACHE_MAX = int(os.getenv("FUND123_TREND_CACHE_MAX", "300"))
_FUND123_STATE_LOCK = threading.Lock()
_FUND123_STATE: Dict[str, Any] = {"ts": 0.0, "csrf": "", "session": None}
_FUND123_KEY_CACHE: Dict[str, Tuple[float, str, str]] = {}
_FUND123_FAIL_CACHE: Dict[str, float] = {}
_FUND123_TREND_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_BAIDU_GS_CONNECT_TIMEOUT_SECONDS = float(os.getenv("BAIDU_GS_CONNECT_TIMEOUT_SECONDS", "0.8"))
_BAIDU_GS_READ_TIMEOUT_SECONDS = float(os.getenv("BAIDU_GS_READ_TIMEOUT_SECONDS", "1.5"))
_BAIDU_GS_FAIL_TTL_SECONDS = float(os.getenv("BAIDU_GS_FAIL_TTL_SECONDS", "45"))
_BAIDU_GS_FAIL_CACHE_MAX = int(os.getenv("BAIDU_GS_FAIL_CACHE_MAX", "300"))
_BAIDU_GS_FAIL_CACHE: Dict[str, float] = {}
_SETTLED_BG_REFRESH_LOCK = threading.Lock()
_SETTLED_BG_REFRESH_INFLIGHT: Set[str] = set()
_AK_RETRY_MAX_ATTEMPTS = int(os.getenv("AK_RETRY_MAX_ATTEMPTS", "2"))
_AK_RETRY_MIN_DELAY_SECONDS = float(os.getenv("AK_RETRY_MIN_DELAY_SECONDS", "0.6"))
_AK_RETRY_MAX_DELAY_SECONDS = float(os.getenv("AK_RETRY_MAX_DELAY_SECONDS", "1.6"))
_AK_MIN_INTERVAL_SECONDS = float(os.getenv("AK_MIN_INTERVAL_SECONDS", "0.0"))
_AK_CALL_TIMEOUT_SECONDS = float(os.getenv("AK_CALL_TIMEOUT_SECONDS", "8.0"))
_SETTLED_HISTORY_TIMEOUT_SECONDS = float(os.getenv("SETTLED_HISTORY_TIMEOUT_SECONDS", "2.5"))
_AK_CALL_LOCK = threading.Lock()
_AK_LAST_CALL_TS: Dict[str, float] = {}


def _trim_timed_cache(cache: Dict[str, Any], max_size: int) -> None:
    if max_size <= 0:
        return
    overflow = len(cache) - int(max_size)
    if overflow <= 0:
        return
    try:
        # cache value shape is usually (ts, payload)
        oldest = sorted(
            cache.items(),
            key=lambda kv: float(kv[1][0]) if isinstance(kv[1], tuple) else 0.0,
        )[:overflow]
        for k, _ in oldest:
            cache.pop(k, None)
    except Exception:
        # Fallback: evict arbitrary keys when timestamp extraction fails.
        for k in list(cache.keys())[:overflow]:
            cache.pop(k, None)


def _trim_ts_cache(cache: Dict[str, float], max_size: int) -> None:
    if max_size <= 0:
        return
    overflow = len(cache) - int(max_size)
    if overflow <= 0:
        return
    oldest = sorted(cache.items(), key=lambda kv: float(kv[1]))[:overflow]
    for k, _ in oldest:
        cache.pop(k, None)


def _trim_runtime_caches() -> None:
    _trim_timed_cache(_FUNDGZ_CACHE, _FUNDGZ_CACHE_MAX)
    _trim_timed_cache(_SETTLED_NAV_CACHE, _SETTLED_NAV_CACHE_MAX)
    _trim_timed_cache(_FUND123_KEY_CACHE, _FUND123_KEY_CACHE_MAX)
    _trim_ts_cache(_FUND123_FAIL_CACHE, _FUND123_FAIL_CACHE_MAX)
    _trim_timed_cache(_FUND123_TREND_CACHE, _FUND123_TREND_CACHE_MAX)
    _trim_ts_cache(_BAIDU_GS_FAIL_CACHE, _BAIDU_GS_FAIL_CACHE_MAX)


def clear_fund_gz_cache(code: Optional[str] = None) -> None:
    c = str(code or "").strip()
    if c:
        keys = [k for k in _FUNDGZ_CACHE.keys() if str(k).startswith(f"{c}|")]
        for k in keys:
            _FUNDGZ_CACHE.pop(k, None)
        cc = _norm_code6(c)
        _FUND123_FAIL_CACHE.pop(cc, None)
        _BAIDU_GS_FAIL_CACHE.pop(cc, None)
        trend_keys = [k for k in _FUND123_TREND_CACHE.keys() if str(k).startswith(f"{cc}|")]
        for k in trend_keys:
            _FUND123_TREND_CACHE.pop(k, None)
        return
    _FUNDGZ_CACHE.clear()
    _FUND123_FAIL_CACHE.clear()
    _BAIDU_GS_FAIL_CACHE.clear()
    _FUND123_TREND_CACHE.clear()


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


def _norm_quote_source_mode(mode: str) -> str:
    m = str(mode or "").strip().lower()
    if m in _QUOTE_SOURCE_ALIASES:
        return _QUOTE_SOURCE_ALIASES[m]
    if m in _QUOTE_SOURCE_MODES:
        return m
    return "auto"


def _norm_code6(code: str) -> str:
    digits = "".join(ch for ch in str(code or "").strip() if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return str(code or "").strip()


def _should_retry_network_error(err: Exception) -> bool:
    text = str(err or "").lower()
    markers = (
        "remotedisconnected",
        "connection aborted",
        "connection reset",
        "max retries exceeded",
        "temporarily unavailable",
        "timed out",
        "incompleteread",
        "name resolution",
        "nodename nor servname",
        "econnreset",
    )
    return any(m in text for m in markers)


def _throttle_ak_call(key: str) -> None:
    min_interval = max(0.0, float(_AK_MIN_INTERVAL_SECONDS))
    if min_interval <= 0:
        return
    now = time.time()
    with _AK_CALL_LOCK:
        last = float(_AK_LAST_CALL_TS.get(key) or 0.0)
    wait = min_interval - (now - last)
    if wait > 0:
        time.sleep(wait)
    with _AK_CALL_LOCK:
        _AK_LAST_CALL_TS[key] = time.time()


def _ak_call_with_retry(call_key: str, fn):
    attempts = max(1, int(_AK_RETRY_MAX_ATTEMPTS))
    low = max(0.0, float(_AK_RETRY_MIN_DELAY_SECONDS))
    high = max(low, float(_AK_RETRY_MAX_DELAY_SECONDS))
    last_err: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            _throttle_ak_call(call_key)
            pool = ThreadPoolExecutor(max_workers=1)
            try:
                fut = pool.submit(fn)
                return fut.result(timeout=max(0.5, float(_AK_CALL_TIMEOUT_SECONDS)))
            finally:
                try:
                    pool.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
        except Exception as e:
            last_err = e
            if i >= attempts or not _should_retry_network_error(e):
                raise
            time.sleep(random.uniform(low, high))
    if last_err is not None:
        raise last_err
    raise RuntimeError("ak call failed without exception")


def _refresh_settled_quote_async(code: str) -> None:
    raw = str(code or "").strip()
    c = _norm_code6(raw)
    if not raw and not c:
        return
    inflight_key = c or raw
    with _SETTLED_BG_REFRESH_LOCK:
        if inflight_key in _SETTLED_BG_REFRESH_INFLIGHT:
            return
        _SETTLED_BG_REFRESH_INFLIGHT.add(inflight_key)

    def _worker() -> None:
        try:
            q_code = c or raw
            out = _fallback_quote_from_settled_nav(q_code)
            if out.get("ok"):
                ts = time.time()
                _FUNDGZ_CACHE[f"{raw}|eastmoney"] = (ts, out)
                if c and c != raw:
                    _FUNDGZ_CACHE[f"{c}|eastmoney"] = (ts, out)
        except Exception:
            pass
        finally:
            with _SETTLED_BG_REFRESH_LOCK:
                _SETTLED_BG_REFRESH_INFLIGHT.discard(inflight_key)

    t = threading.Thread(target=_worker, name=f"settled-refresh-{c}", daemon=True)
    t.start()


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


def _quick_estimate_quote_from_fundgz(code: str) -> Dict[str, Any]:
    c = str(code or "").strip()
    if not c:
        return {"ok": False, "error": "empty code"}
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
        resp = sess.get(
            url,
            headers=headers,
            timeout=(
                _FUNDGZ_CONNECT_TIMEOUT_SECONDS,
                _FUNDGZ_READ_TIMEOUT_SECONDS,
            ),
            proxies={},
        )
        if resp.status_code != 200:
            return {"ok": False, "error": f"HTTP {resp.status_code}"}
        resp.encoding = "utf-8"
        obj = _parse_jsonp_obj(resp.text)
        if not obj:
            return {"ok": False, "error": "empty json"}
        nav = _safe_float(obj.get("gsz"))
        if nav is None:
            nav = _safe_float(obj.get("dwjz"))
        prev_nav = _safe_float(obj.get("dwjz"))
        out = {
            "ok": True,
            "code": c,
            "name": str(obj.get("name") or "").strip(),
            "nav": nav,
            "prev_nav": prev_nav,
            "daily_change_pct": _safe_float(obj.get("gszzl")),
            "jzrq": str(obj.get("jzrq") or "").strip(),
            "gztime": str(obj.get("gztime") or "").strip(),
            "source": "fundgz_estimate_quick",
        }
        return out
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _pick_first_nonempty(row: Dict[str, Any], keys: List[str]) -> Any:
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


def _is_biying_eligible(code: str) -> bool:
    """
    Biying fd/real/time mainly covers ETF / exchange-traded funds.
    Open-end mutual fund codes (like 017736/018463) are often unsupported (404).
    """
    c = _norm_code6(code)
    if not c or not c.isdigit():
        return False
    etf_prefixes = (
        "15", "16",  # SZ listed funds/ETF families
        "50", "51", "56", "58",  # SH ETF families
    )
    return c.startswith(etf_prefixes)


def _extract_biying_row(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                return item
        return {}
    if not isinstance(payload, dict):
        return {}

    data = payload.get("data")
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                return item
    elif isinstance(data, dict):
        # some providers return {"data": {"159001": {...}}}
        if any(k in data for k in ("p", "price", "yc", "pc")):
            return data
        for _, item in data.items():
            if isinstance(item, dict):
                return item

    if any(k in payload for k in ("p", "price", "yc", "pc")):
        return payload
    return {}


def _fetch_biying_quote(code: str) -> Dict[str, Any]:
    c = _norm_code6(code)
    if not c:
        return {"ok": False, "error": "empty code"}
    if not _is_biying_eligible(c):
        return {"ok": False, "error": "biying unsupported code family"}

    licence = str(os.getenv("BIYING_LICENCE") or os.getenv("BIYING_LICENSE") or "").strip()
    if not licence:
        return {"ok": False, "error": "biying licence missing"}

    base_url = str(os.getenv("BIYING_API_BASE") or "http://api.biyingapi.com").strip().rstrip("/")
    url = f"{base_url}/fd/real/time/{c}/{licence}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Connection": "close",
    }

    sess = requests.Session()
    try:
        sess.trust_env = False
    except Exception:
        pass

    try:
        # Keep Biying timeout short; avoid per-code serial latency causing API timeout.
        resp = sess.get(url, headers=headers, timeout=(0.8, 1.2), proxies={})
    except Exception as e:
        return {"ok": False, "error": f"biying request error: {type(e).__name__}: {e}"}

    if resp.status_code != 200:
        return {"ok": False, "error": f"biying HTTP {resp.status_code}"}

    try:
        payload = resp.json()
    except Exception:
        text = (resp.text or "").strip()
        if not text:
            return {"ok": False, "error": "biying empty response"}
        return {"ok": False, "error": "biying non-json response"}

    row = _extract_biying_row(payload)
    if not row:
        return {"ok": False, "error": "biying empty data"}

    nav = _safe_float(_pick_first_nonempty(row, ["p", "price", "latest", "close"]))
    prev_nav = _safe_float(_pick_first_nonempty(row, ["yc", "prev_close", "pre_close"]))
    daily_change_pct = _safe_float(_pick_first_nonempty(row, ["pc", "pct", "change_percent"]))

    if nav is None:
        return {"ok": False, "error": "biying missing price"}

    if daily_change_pct is None and nav is not None and prev_nav not in (None, 0):
        try:
            daily_change_pct = (float(nav) - float(prev_nav)) / float(prev_nav) * 100.0
        except Exception:
            daily_change_pct = None
    if prev_nav is None and nav is not None and daily_change_pct not in (None, -100):
        try:
            prev_nav = float(nav) / (1.0 + float(daily_change_pct) / 100.0)
        except Exception:
            prev_nav = None

    name = str(_pick_first_nonempty(row, ["mc", "name", "jjmc"]) or "").strip()
    if not name:
        try:
            from data import get_fund_name

            name = str(get_fund_name(c) or "").strip()
        except Exception:
            name = ""

    gztime = str(_pick_first_nonempty(row, ["t", "time", "tm"]) or "").strip()
    jzrq = datetime.now().strftime("%Y-%m-%d")
    if isinstance(gztime, str) and len(gztime) >= 10 and gztime[4:5] in ("-", "/"):
        jzrq = gztime[:10].replace("/", "-")

    return {
        "ok": True,
        "code": c,
        "name": name,
        "nav": float(nav),
        "prev_nav": float(prev_nav) if prev_nav is not None else None,
        "daily_change_pct": float(daily_change_pct) if daily_change_pct is not None else None,
        "jzrq": jzrq,
        "gztime": gztime,
        "source": "biying",
    }


def _new_requests_session() -> requests.Session:
    sess = requests.Session()
    try:
        sess.trust_env = False
    except Exception:
        pass
    return sess


def _fund123_common_headers(*, json_api: bool = False) -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": "https://www.fund123.cn",
        "Referer": "https://www.fund123.cn/fund",
        "Connection": "close",
    }
    if json_api:
        headers.update(
            {
                "Accept": "application/json,text/plain,*/*",
                "Content-Type": "application/json",
                "X-API-Key": "foobar",
            }
        )
    else:
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    return headers


def _fund123_bootstrap(force_refresh: bool = False) -> Tuple[Optional[requests.Session], str]:
    now = time.time()
    with _FUND123_STATE_LOCK:
        sess = _FUND123_STATE.get("session")
        csrf = str(_FUND123_STATE.get("csrf") or "").strip()
        ts = float(_FUND123_STATE.get("ts") or 0.0)
        if (
            not force_refresh
            and isinstance(sess, requests.Session)
            and csrf
            and (now - ts) <= _FUND123_BOOTSTRAP_TTL_SECONDS
        ):
            return sess, csrf

        sess = _new_requests_session()
        try:
            resp = sess.get(
                "https://www.fund123.cn/fund",
                headers=_fund123_common_headers(json_api=False),
                timeout=(
                    _FUND123_CONNECT_TIMEOUT_SECONDS,
                    _FUND123_READ_TIMEOUT_SECONDS,
                ),
                proxies={},
            )
            text = str(resp.text or "")
            match = re.search(r'"csrf":"([^"]+)"', text)
            csrf = str(match.group(1) if match else "").strip()
            if not csrf:
                return None, ""
            _FUND123_STATE["session"] = sess
            _FUND123_STATE["csrf"] = csrf
            _FUND123_STATE["ts"] = now
            return sess, csrf
        except Exception:
            return None, ""


def _fund123_search_meta(code: str) -> Dict[str, Any]:
    c = _norm_code6(code)
    if not c:
        return {}

    now = time.time()
    cached = _FUND123_KEY_CACHE.get(c)
    if cached and (now - cached[0]) <= _FUND123_KEY_TTL_SECONDS:
        return {"key": str(cached[1] or ""), "name": str(cached[2] or "")}

    for attempt in (0, 1):
        sess, csrf = _fund123_bootstrap(force_refresh=(attempt == 1))
        if not isinstance(sess, requests.Session) or not csrf:
            continue
        try:
            resp = sess.post(
                "https://www.fund123.cn/api/fund/searchFund",
                headers=_fund123_common_headers(json_api=True),
                params={"_csrf": csrf},
                json={"fundCode": c},
                timeout=(
                    _FUND123_CONNECT_TIMEOUT_SECONDS,
                    _FUND123_READ_TIMEOUT_SECONDS,
                ),
                proxies={},
            )
            payload = resp.json()
            if not isinstance(payload, dict) or not payload.get("success"):
                continue
            fund_info = payload.get("fundInfo") or {}
            key = str(fund_info.get("key") or "").strip()
            name = str(fund_info.get("fundName") or "").strip()
            if not key:
                continue
            _FUND123_KEY_CACHE[c] = (now, key, name)
            return {"key": key, "name": name}
        except Exception:
            continue
    return {}


def _fetch_fund123_quote(code: str) -> Dict[str, Any]:
    c = _norm_code6(code)
    if not c:
        return {"ok": False, "error": "empty code"}
    now = time.time()
    last_fail = float(_FUND123_FAIL_CACHE.get(c) or 0.0)
    if last_fail and (now - last_fail) <= _FUND123_FAIL_TTL_SECONDS:
        return {"ok": False, "error": "fund123 recent fail cached"}

    meta = _fund123_search_meta(c)
    product_key = str(meta.get("key") or "").strip()
    if not product_key:
        _FUND123_FAIL_CACHE[c] = now
        return {"ok": False, "error": "fund123 key not found"}

    for attempt in (0, 1):
        sess, csrf = _fund123_bootstrap(force_refresh=(attempt == 1))
        if not isinstance(sess, requests.Session) or not csrf:
            continue
        today = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        body = {
            "startTime": today,
            "endTime": tomorrow,
            "limit": 200,
            "productId": product_key,
            "format": True,
            "source": "WEALTHBFFWEB",
        }
        try:
            resp = sess.post(
                "https://www.fund123.cn/api/fund/queryFundEstimateIntraday",
                headers=_fund123_common_headers(json_api=True),
                params={"_csrf": csrf},
                json=body,
                timeout=(
                    _FUND123_CONNECT_TIMEOUT_SECONDS,
                    _FUND123_READ_TIMEOUT_SECONDS,
                ),
                proxies={},
            )
            payload = resp.json()
        except Exception:
            continue
        if not isinstance(payload, dict) or not payload.get("success"):
            continue

        rows = payload.get("list") or []
        if not isinstance(rows, list) or not rows:
            continue
        row = rows[-1] if isinstance(rows[-1], dict) else {}

        nav = _safe_float(row.get("forecastNetValue"))
        if nav is None:
            nav = _safe_float(row.get("netValue"))
        if nav is None:
            continue

        pct_raw = _safe_float(row.get("forecastGrowth"))
        pct = None
        if pct_raw is not None:
            pct = float(pct_raw * 100.0) if abs(float(pct_raw)) <= 2 else float(pct_raw)

        prev_nav = None
        if pct not in (None, -100):
            try:
                prev_nav = float(nav) / (1.0 + float(pct) / 100.0)
            except Exception:
                prev_nav = None

        gztime = ""
        jzrq = today
        try:
            t = row.get("time")
            if t is not None:
                ts = float(t)
                if ts > 1e12:
                    ts = ts / 1000.0
                dt = datetime.fromtimestamp(ts)
                gztime = dt.strftime("%Y-%m-%d %H:%M")
                jzrq = dt.strftime("%Y-%m-%d")
        except Exception:
            pass

        return {
            "ok": True,
            "code": c,
            "name": str(meta.get("name") or "").strip(),
            "nav": float(nav),
            "prev_nav": float(prev_nav) if prev_nav is not None else None,
            "daily_change_pct": float(pct) if pct is not None else None,
            "jzrq": jzrq,
            "gztime": gztime,
            "source": "fund123_estimate",
        }

    _FUND123_FAIL_CACHE[c] = now
    return {"ok": False, "error": "fund123 fetch failed"}


def _deep_pick_first(payload: Any, keys: Set[str], max_nodes: int = 3000) -> Any:
    seen = 0
    stack: List[Any] = [payload]
    while stack and seen < max_nodes:
        node = stack.pop()
        seen += 1
        if isinstance(node, dict):
            for k, v in node.items():
                kk = str(k or "").strip().lower()
                if kk in keys and not isinstance(v, (dict, list)):
                    return v
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return None


def _extract_text_field_from_blob(blob: str, keys: List[str]) -> str:
    text = str(blob or "")
    if not text:
        return ""
    for key in keys:
        esc = re.escape(str(key))
        m = re.search(rf'"{esc}"\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE)
        if m:
            return str(m.group(1) or "").strip()
        m = re.search(rf"'{esc}'\s*:\s*'([^']+)'", text, flags=re.IGNORECASE)
        if m:
            return str(m.group(1) or "").strip()
    return ""


def _extract_float_field_from_blob(blob: str, keys: List[str]) -> Optional[float]:
    text = str(blob or "")
    if not text:
        return None
    for key in keys:
        esc = re.escape(str(key))
        m = re.search(rf'"{esc}"\s*:\s*"?(-?\d+(?:\.\d+)?)%?"?', text, flags=re.IGNORECASE)
        if m:
            return _safe_float(m.group(1))
        m = re.search(rf"'{esc}'\s*:\s*'?(-?\d+(?:\.\d+)?)%?'?", text, flags=re.IGNORECASE)
        if m:
            return _safe_float(m.group(1))
    return None


def _build_baidu_quote_from_payload(code: str, payload: Any) -> Dict[str, Any]:
    c = _norm_code6(code)
    nav = _safe_float(
        _deep_pick_first(
            payload,
            {
                "gsz",
                "nav",
                "netvalue",
                "currentnav",
                "latestnav",
                "forecastnetvalue",
                "estimate_net_value",
                "estimatevalue",
                "price",
            },
        )
    )
    prev_nav = _safe_float(
        _deep_pick_first(
            payload,
            {
                "dwjz",
                "prevnav",
                "prev_net_value",
                "preclose",
                "previousnav",
                "yesterdaynav",
                "unitnav",
            },
        )
    )
    pct_raw = _deep_pick_first(
        payload,
        {
            "gszzl",
            "pct",
            "pctchange",
            "changeratio",
            "changepercent",
            "growth",
            "growthrate",
            "forecastgrowth",
            "rise",
            "risepercent",
        },
    )
    pct = _safe_float(pct_raw)
    if pct is None and isinstance(pct_raw, str) and pct_raw.strip().endswith("%"):
        pct = _safe_float(pct_raw.strip().rstrip("%"))
    if pct is None and nav is not None and prev_nav not in (None, 0):
        try:
            pct = (float(nav) - float(prev_nav)) / float(prev_nav) * 100.0
        except Exception:
            pct = None
    if prev_nav is None and nav is not None and pct not in (None, -100):
        try:
            prev_nav = float(nav) / (1.0 + float(pct) / 100.0)
        except Exception:
            prev_nav = None

    if nav is None:
        return {"ok": False, "error": "baidu payload missing nav"}

    name = str(
        _deep_pick_first(
            payload,
            {
                "name",
                "fundname",
                "fund_name",
                "jjmc",
                "shortname",
                "title",
            },
        )
        or ""
    ).strip()
    gztime = str(
        _deep_pick_first(
            payload,
            {"gztime", "updatetime", "updatetime_str", "time", "datatime", "quote_time"},
        )
        or ""
    ).strip()
    jzrq = str(
        _deep_pick_first(payload, {"jzrq", "tradedate", "date", "navdate"}) or ""
    ).strip()
    if not jzrq and len(gztime) >= 10:
        jzrq = gztime[:10].replace("/", "-")
    if not jzrq:
        jzrq = datetime.now().strftime("%Y-%m-%d")

    return {
        "ok": True,
        "code": c,
        "name": name,
        "nav": float(nav),
        "prev_nav": float(prev_nav) if prev_nav is not None else None,
        "daily_change_pct": float(pct) if pct is not None else None,
        "jzrq": jzrq,
        "gztime": gztime,
        "source": "baidu_gushitong",
    }


def _fetch_baidu_gushitong_quote(code: str) -> Dict[str, Any]:
    c = _norm_code6(code)
    if not c:
        return {"ok": False, "error": "empty code"}

    now = time.time()
    last_fail = float(_BAIDU_GS_FAIL_CACHE.get(c) or 0.0)
    if last_fail and (now - last_fail) <= _BAIDU_GS_FAIL_TTL_SECONDS:
        return {"ok": False, "error": "baidu recent fail cached"}

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Referer": f"https://gushitong.baidu.com/fund/fo-{c}",
        "Origin": "https://gushitong.baidu.com",
        "Connection": "close",
    }
    timeout_pair = (
        _BAIDU_GS_CONNECT_TIMEOUT_SECONDS,
        _BAIDU_GS_READ_TIMEOUT_SECONDS,
    )

    sess = _new_requests_session()
    url_template = str(os.getenv("BAIDU_GS_QUOTE_URL_TEMPLATE") or "").strip()
    if url_template:
        try:
            url = url_template.format(code=c)
            resp = sess.get(url, headers=headers, timeout=timeout_pair, proxies={})
            if resp.status_code == 200:
                try:
                    payload = resp.json()
                    out = _build_baidu_quote_from_payload(c, payload)
                    if out.get("ok"):
                        return out
                except Exception:
                    pass
                out = _build_baidu_quote_from_payload(c, resp.text)
                if out.get("ok"):
                    return out
        except Exception:
            pass

    api_candidates = [
        ("https://finance.pae.baidu.com/sapi/v1/fund/quote", {"code": c, "finClientType": "pc"}),
        ("https://finance.pae.baidu.com/vapi/v1/fund/quote", {"code": c, "finClientType": "pc"}),
        ("https://finance.pae.baidu.com/sapi/v1/fundquotation", {"code": c, "finClientType": "pc"}),
        ("https://finance.pae.baidu.com/vapi/v1/fundquotation", {"code": c, "finClientType": "pc"}),
    ]
    for url, params in api_candidates:
        try:
            resp = sess.get(url, params=params, headers=headers, timeout=timeout_pair, proxies={})
            if resp.status_code != 200:
                continue
            payload = resp.json()
            out = _build_baidu_quote_from_payload(c, payload)
            if out.get("ok"):
                return out
        except Exception:
            continue

    html_urls = [
        f"https://gushitong.baidu.com/fund/fo-{c}",
        f"https://gushitong.baidu.com/fund/{c}",
    ]
    for url in html_urls:
        try:
            resp = sess.get(
                url,
                headers={
                    **headers,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                timeout=timeout_pair,
                proxies={},
            )
            if resp.status_code != 200:
                continue
            txt = str(resp.text or "")
            if not txt:
                continue
            nav = _extract_float_field_from_blob(
                txt,
                [
                    "gsz",
                    "nav",
                    "netValue",
                    "currentNav",
                    "latestNav",
                    "forecastNetValue",
                    "estimateNetValue",
                    "price",
                ],
            )
            if nav is None:
                continue
            prev_nav = _extract_float_field_from_blob(
                txt,
                ["dwjz", "prevNav", "prevNetValue", "previousNav", "preClose"],
            )
            pct = _extract_float_field_from_blob(
                txt,
                ["gszzl", "pct", "pctChange", "changeRatio", "forecastGrowth", "growthRate"],
            )
            if pct is None and nav is not None and prev_nav not in (None, 0):
                try:
                    pct = (float(nav) - float(prev_nav)) / float(prev_nav) * 100.0
                except Exception:
                    pct = None
            if prev_nav is None and nav is not None and pct not in (None, -100):
                try:
                    prev_nav = float(nav) / (1.0 + float(pct) / 100.0)
                except Exception:
                    prev_nav = None
            name = _extract_text_field_from_blob(
                txt,
                ["name", "fundName", "fund_name", "jjmc", "shortName", "title"],
            )
            gztime = _extract_text_field_from_blob(
                txt,
                ["gztime", "updateTime", "updateTimeStr", "quoteTime", "time"],
            )
            jzrq = _extract_text_field_from_blob(
                txt,
                ["jzrq", "tradeDate", "date", "navDate"],
            )
            if not jzrq and len(gztime) >= 10:
                jzrq = gztime[:10].replace("/", "-")
            if not jzrq:
                jzrq = datetime.now().strftime("%Y-%m-%d")
            return {
                "ok": True,
                "code": c,
                "name": name,
                "nav": float(nav),
                "prev_nav": float(prev_nav) if prev_nav is not None else None,
                "daily_change_pct": float(pct) if pct is not None else None,
                "jzrq": jzrq,
                "gztime": gztime,
                "source": "baidu_gushitong",
            }
        except Exception:
            continue

    _BAIDU_GS_FAIL_CACHE[c] = now
    return {"ok": False, "error": "baidu gushitong fetch failed"}


def fetch_fund_intraday_trend(
    code: str,
    source_mode: str = "auto",
    max_points: int = 180,
) -> Dict[str, Any]:
    c = _norm_code6(code)
    if not c:
        return {"ok": False, "error": "empty code", "points": []}
    _trim_runtime_caches()
    mode = _norm_quote_source_mode(source_mode)
    try:
        limit = int(max_points)
    except Exception:
        limit = 180
    limit = max(30, min(limit, 300))

    now = time.time()
    cache_key = f"{c}|{mode}|{limit}"
    cached = _FUND123_TREND_CACHE.get(cache_key)
    if cached and (now - cached[0]) <= _FUND123_TREND_TTL_SECONDS:
        return cached[1]

    if mode in {"eastmoney", "tiantian", "baidu"}:
        latest = fetch_fund_gz(c, source_mode=mode)
        nav = _safe_float((latest or {}).get("nav"))
        pct = _safe_float((latest or {}).get("daily_change_pct"))
        if nav is not None:
            t = str((latest or {}).get("jzrq") or (latest or {}).get("gztime") or "").strip()
            points = [
                {
                    "time": t if t else datetime.now().strftime("%H:%M"),
                    "nav": round(float(nav), 6),
                    "pct": round(float(pct), 2) if pct is not None else None,
                    "ts": int(time.time() * 1000),
                }
            ]
            out = {
                "ok": True,
                "code": c,
                "name": str((latest or {}).get("name") or "").strip(),
                "source": str((latest or {}).get("source") or mode),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "points": points,
            }
            _FUND123_TREND_CACHE[cache_key] = (now, out)
            return out
        out = {
            "ok": False,
            "error": str((latest or {}).get("error") or f"{mode} quote unavailable"),
            "code": c,
            "name": str((latest or {}).get("name") or "").strip(),
            "source": mode,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "points": [],
        }
        _FUND123_TREND_CACHE[cache_key] = (now, out)
        return out

    meta = _fund123_search_meta(c)
    product_key = str(meta.get("key") or "").strip()
    if not product_key:
        out = {"ok": False, "error": "fund123 key not found", "code": c, "points": []}
        _FUND123_TREND_CACHE[cache_key] = (now, out)
        return out

    points: List[Dict[str, Any]] = []
    err = "fund123 fetch failed"
    for attempt in (0, 1):
        sess, csrf = _fund123_bootstrap(force_refresh=(attempt == 1))
        if not isinstance(sess, requests.Session) or not csrf:
            err = "fund123 bootstrap failed"
            continue

        today = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        body = {
            "startTime": today,
            "endTime": tomorrow,
            "limit": limit,
            "productId": product_key,
            "format": True,
            "source": "WEALTHBFFWEB",
        }
        try:
            resp = sess.post(
                "https://www.fund123.cn/api/fund/queryFundEstimateIntraday",
                headers=_fund123_common_headers(json_api=True),
                params={"_csrf": csrf},
                json=body,
                timeout=(
                    _FUND123_CONNECT_TIMEOUT_SECONDS,
                    _FUND123_READ_TIMEOUT_SECONDS,
                ),
                proxies={},
            )
            payload = resp.json()
        except Exception as e:
            err = f"fund123 request error: {type(e).__name__}: {e}"
            continue

        if not isinstance(payload, dict) or not payload.get("success"):
            err = "fund123 non-success payload"
            continue

        rows = payload.get("list") or []
        if not isinstance(rows, list) or not rows:
            err = "fund123 empty list"
            continue

        tmp: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            nav = _safe_float(row.get("forecastNetValue"))
            if nav is None:
                nav = _safe_float(row.get("netValue"))
            if nav is None:
                continue

            pct_raw = _safe_float(row.get("forecastGrowth"))
            pct = None
            if pct_raw is not None:
                pct = float(pct_raw * 100.0) if abs(float(pct_raw)) <= 2 else float(pct_raw)

            label = ""
            ts_ms = None
            try:
                t = row.get("time")
                if t is not None:
                    ts = float(t)
                    if ts > 1e12:
                        ts = ts / 1000.0
                        ts_ms = int(float(t))
                    else:
                        ts_ms = int(ts * 1000)
                    dt = datetime.fromtimestamp(ts)
                    label = dt.strftime("%H:%M")
            except Exception:
                label = ""

            if not label:
                label = str(row.get("timeLabel") or "").strip()
            if not label:
                continue

            tmp[label] = {
                "time": label,
                "nav": round(float(nav), 6),
                "pct": round(float(pct), 2) if pct is not None else None,
                "ts": ts_ms,
            }

        points = list(tmp.values())
        if points:
            points.sort(key=lambda x: int(x.get("ts") or 0))
            break

    if not points:
        latest = fetch_fund_gz(c, source_mode=mode)
        nav = _safe_float((latest or {}).get("nav"))
        pct = _safe_float((latest or {}).get("daily_change_pct"))
        if nav is not None:
            t = str((latest or {}).get("gztime") or "").strip()
            label = t[-5:] if len(t) >= 5 else datetime.now().strftime("%H:%M")
            points = [
                {
                    "time": label,
                    "nav": round(float(nav), 6),
                    "pct": round(float(pct), 2) if pct is not None else None,
                    "ts": int(time.time() * 1000),
                }
            ]
            out = {
                "ok": True,
                "code": c,
                "name": str((latest or {}).get("name") or meta.get("name") or "").strip(),
                "source": str((latest or {}).get("source") or "fallback_latest"),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "points": points,
            }
            _FUND123_TREND_CACHE[cache_key] = (now, out)
            return out

        out = {
            "ok": False,
            "error": err,
            "code": c,
            "name": str(meta.get("name") or "").strip(),
            "source": "fund123_estimate",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "points": [],
        }
        _FUND123_TREND_CACHE[cache_key] = (now, out)
        return out

    out = {
        "ok": True,
        "code": c,
        "name": str(meta.get("name") or "").strip(),
        "source": "fund123_estimate",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "points": points,
    }
    _FUND123_TREND_CACHE[cache_key] = (now, out)
    return out


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
    now_dt = datetime.now()
    if now_dt.weekday() < 5:
        # Workday: do not mark settled before nightly cutoff, and only mark
        # when NAV date reaches today (avoid showing previous day as "updated").
        if now_dt.hour < _SETTLED_SWITCH_HOUR:
            return False
        return jzrq_date >= now_dt.date()

    # Weekend: latest valid settled NAV is usually Friday.
    expected = now_dt.date()
    if now_dt.weekday() == 5:
        expected = expected - timedelta(days=1)
    elif now_dt.weekday() == 6:
        expected = expected - timedelta(days=2)
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
        "name": str,
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
                "name": str((daily or {}).get("name") or "").strip(),
            }
            _SETTLED_NAV_CACHE[key] = (now, out)
            return out
    except Exception:
        pass

    # 2) Fallback to history series (stable but less timely).
    try:
        def _load_history_df():
            from data import get_fund_history  # 延迟导入，避免启动时重依赖

            return get_fund_history(c, lookback_days=120)

        pool = ThreadPoolExecutor(max_workers=1)
        try:
            fut = pool.submit(_load_history_df)
            df = fut.result(timeout=max(0.5, float(_SETTLED_HISTORY_TIMEOUT_SECONDS)))
        except FuturesTimeoutError:
            try:
                fut.cancel()
            except Exception:
                pass
            df = None
        finally:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

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
    old_map: Dict[str, Dict[str, Any]] = data_map if isinstance(data_map, dict) else {}
    if old_map and (now - cache_ts) <= _OPEN_FUND_DAILY_TTL_SECONDS:
        return dict(old_map.get(c) or {})

    latest_map: Dict[str, Dict[str, Any]] = {}
    try:
        import akshare as ak  # type: ignore
    except Exception:
        _OPEN_FUND_DAILY_CACHE["ts"] = now
        _OPEN_FUND_DAILY_CACHE["data"] = old_map
        return dict(old_map.get(c) or {})

    try:
        from backend.services.sector_flow_service import akshare_no_proxy
    except Exception:
        akshare_no_proxy = None

    try:
        fn = getattr(ak, "fund_open_fund_daily_em", None)
        if not callable(fn):
            _OPEN_FUND_DAILY_CACHE["ts"] = now
            _OPEN_FUND_DAILY_CACHE["data"] = old_map
            return dict(old_map.get(c) or {})

        def _call():
            if callable(akshare_no_proxy):
                with akshare_no_proxy():
                    return fn()
            return fn()

        df = _ak_call_with_retry("ak:fund_open_fund_daily_em", _call)
    except Exception:
        df = None

    try:
        if df is None or getattr(df, "empty", True):
            _OPEN_FUND_DAILY_CACHE["ts"] = now
            _OPEN_FUND_DAILY_CACHE["data"] = old_map
            return dict(old_map.get(c) or {})
    except Exception:
        _OPEN_FUND_DAILY_CACHE["ts"] = now
        _OPEN_FUND_DAILY_CACHE["data"] = old_map
        return dict(old_map.get(c) or {})

    code_col = _pick_col_contains(df, ["基金代码", "代码"])
    name_col = _pick_col_contains(df, ["基金简称", "基金名称", "名称"])
    nav_col = _pick_col_contains(df, ["单位净值", "最新净值", "净值"])
    pct_col = _pick_col_contains(df, ["日增长率", "日涨跌幅", "涨跌幅"])
    date_col = _pick_col_contains(df, ["净值日期", "日期", "更新"])

    if not code_col or not nav_col:
        _OPEN_FUND_DAILY_CACHE["ts"] = now
        _OPEN_FUND_DAILY_CACHE["data"] = old_map
        return dict(old_map.get(c) or {})

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

    final_map = latest_map if latest_map else old_map
    _OPEN_FUND_DAILY_CACHE["ts"] = now
    _OPEN_FUND_DAILY_CACHE["data"] = final_map
    return dict(final_map.get(c) or {})


def _fallback_quote_from_settled_nav(code: str) -> Dict[str, Any]:
    c = str(code or "").strip()
    if not c:
        return {"ok": False, "error": "empty code"}
    snap = _fetch_settled_nav_snapshot(c, "")
    nav = _safe_float(snap.get("nav")) if snap else None
    if nav is None:
        etf = _fetch_etf_spot_snapshot(c)
        e_nav = _safe_float((etf or {}).get("nav"))
        if e_nav is None:
            return {"ok": False, "error": "settled snapshot unavailable"}
        e_prev = _safe_float((etf or {}).get("prev_nav"))
        e_pct = _safe_float((etf or {}).get("daily_change_pct"))
        if e_pct is None and e_prev not in (None, 0):
            try:
                e_pct = (float(e_nav) - float(e_prev)) / float(e_prev) * 100.0
            except Exception:
                e_pct = None
        return {
            "ok": True,
            "code": c,
            "name": str((etf or {}).get("name") or "").strip(),
            "nav": float(e_nav),
            "prev_nav": float(e_prev) if e_prev is not None else None,
            "daily_change_pct": float(e_pct) if e_pct is not None else None,
            "jzrq": str((etf or {}).get("jzrq") or "").strip(),
            "gztime": str((etf or {}).get("gztime") or "").strip(),
            "source": str((etf or {}).get("source") or "etf_fallback"),
        }
    prev_nav = _safe_float(snap.get("prev_nav")) if snap.get("prev_nav") is not None else None
    pct = _safe_float(snap.get("daily_change_pct")) if snap.get("daily_change_pct") is not None else None
    if pct is None and prev_nav not in (None, 0):
        try:
            pct = (float(nav) - float(prev_nav)) / float(prev_nav) * 100.0
        except Exception:
            pct = None

    name = str((snap or {}).get("name") or "").strip()
    try:
        from data import get_fund_name

        if not name:
            name = str(get_fund_name(c) or "").strip()
    except Exception:
        pass

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


def _build_etf_spot_map_from_df(df: Any, source: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    try:
        if df is None or getattr(df, "empty", True):
            return out
    except Exception:
        return out

    code_col = _pick_col_contains(df, ["基金代码", "代码"])
    name_col = _pick_col_contains(df, ["基金简称", "基金名称", "名称"])
    nav_col = _pick_col_contains(df, ["最新价", "现价", "最新", "收盘价", "单位净值", "最新净值"])
    pct_col = _pick_col_contains(df, ["涨跌幅", "日增长率", "涨幅"])
    if not code_col or not nav_col:
        return out

    try:
        for _, row in df.iterrows():
            cc = _norm_code6(str(row.get(code_col) or ""))
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
            out[cc] = {
                "name": str(row.get(name_col) or "").strip() if name_col else "",
                "nav": float(nav),
                "prev_nav": float(prev_nav) if prev_nav is not None else None,
                "daily_change_pct": float(pct) if pct is not None else None,
                "jzrq": datetime.now().strftime("%Y-%m-%d"),
                "gztime": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "source": source,
            }
    except Exception:
        return {}
    return out


def _fetch_etf_spot_snapshot(code: str) -> Dict[str, Any]:
    """
    ETF fallback quote chain:
    1) THS ETF spot
    2) EM ETF daily table
    """
    c = _norm_code6(code)
    if not c:
        return {}

    now = time.time()
    cache_ts = float(_ETF_SPOT_CACHE.get("ts") or 0.0)
    data_map = _ETF_SPOT_CACHE.get("data") or {}
    old_map: Dict[str, Dict[str, Any]] = data_map if isinstance(data_map, dict) else {}
    if old_map and (now - cache_ts) <= _ETF_SPOT_TTL_SECONDS:
        return dict(old_map.get(c) or {})

    merged: Dict[str, Dict[str, Any]] = {}

    try:
        import akshare as ak  # type: ignore
    except Exception:
        _ETF_SPOT_CACHE["ts"] = now
        _ETF_SPOT_CACHE["data"] = old_map
        return dict(old_map.get(c) or {})

    try:
        from backend.services.sector_flow_service import akshare_no_proxy
    except Exception:
        akshare_no_proxy = None

    def _load_df(fn_name: str):
        fn = getattr(ak, fn_name, None)
        if not callable(fn):
            return None
        try:
            def _call():
                if callable(akshare_no_proxy):
                    with akshare_no_proxy():
                        return fn()
                return fn()

            return _ak_call_with_retry(f"ak:{fn_name}", _call)
        except Exception:
            return None

    # THS spot first
    try:
        df_ths = _load_df("fund_etf_spot_ths")
        map_ths = _build_etf_spot_map_from_df(df_ths, "ths_etf_spot")
        merged.update(map_ths)
    except Exception:
        pass

    # EM ETF daily as backup; do not overwrite THS spot rows.
    try:
        df_em = _load_df("fund_etf_fund_daily_em")
        map_em = _build_etf_spot_map_from_df(df_em, "em_etf_daily")
        for k, v in map_em.items():
            merged.setdefault(k, v)
    except Exception:
        pass

    final_map = merged if merged else old_map
    _ETF_SPOT_CACHE["ts"] = now
    _ETF_SPOT_CACHE["data"] = final_map
    return dict(final_map.get(c) or {})


def fetch_fund_gz(code: str, source_mode: str = "auto") -> Dict[str, Any]:
    """Fetch realtime/estimated fund info from Eastmoney fundgz."""
    c = str(code).strip()
    if not c:
        return {"ok": False, "error": "empty code"}
    _trim_runtime_caches()
    mode = _norm_quote_source_mode(source_mode)

    now = time.time()
    now_dt = datetime.now()
    cache_key = f"{c}|{mode}"
    cached = _FUNDGZ_CACHE.get(cache_key)
    cache_ttl = _FUNDGZ_SETTLED_TTL_SECONDS if mode == "eastmoney" else _FUNDGZ_TTL_SECONDS
    if cached and (now - cached[0]) <= cache_ttl:
        return cached[1]

    if mode == "eastmoney":
        if cached and isinstance(cached[1], dict) and cached[1].get("ok"):
            # stale-while-revalidate: return last good settled quote immediately
            # and refresh in background to avoid blocking holdings refresh.
            _refresh_settled_quote_async(c)
            return cached[1]
        warm = _fallback_quote_from_settled_nav(c)
        if warm.get("ok"):
            _FUNDGZ_CACHE[cache_key] = (now, warm)
            return warm
        # No settled cache yet: trigger async warmup and return quickly.
        _refresh_settled_quote_async(c)
        return {"ok": False, "error": "eastmoney settled quote warming up"}
    elif mode == "fund123":
        f123 = _fetch_fund123_quote(c)
        if f123.get("ok"):
            _FUNDGZ_CACHE[cache_key] = (now, f123)
            return f123
        out = {"ok": False, "error": str(f123.get("error") or "fund123 quote unavailable")}
        _FUNDGZ_CACHE[cache_key] = (now, out)
        return out
    elif mode == "baidu":
        bq = _fetch_baidu_gushitong_quote(c)
        if bq.get("ok"):
            _FUNDGZ_CACHE[cache_key] = (now, bq)
            return bq
        out = {"ok": False, "error": str(bq.get("error") or "baidu quote unavailable")}
        _FUNDGZ_CACHE[cache_key] = (now, out)
        return out

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
        resp = sess.get(
            url,
            headers=headers,
            timeout=(
                _FUNDGZ_CONNECT_TIMEOUT_SECONDS,
                _FUNDGZ_READ_TIMEOUT_SECONDS,
            ),
            proxies={},
        )
        if resp.status_code != 200:
            if mode in {"auto"}:
                f123 = _fetch_fund123_quote(c)
                if f123.get("ok"):
                    _FUNDGZ_CACHE[cache_key] = (now, f123)
                    return f123
            if mode != "tiantian":
                fb = _fallback_quote_from_settled_nav(c)
                if fb.get("ok"):
                    _FUNDGZ_CACHE[cache_key] = (now, fb)
                    return fb
            out = {"ok": False, "error": f"HTTP {resp.status_code}"}
            _FUNDGZ_CACHE[cache_key] = (now, out)
            return out

        resp.encoding = "utf-8"
        obj = _parse_jsonp_obj(resp.text)
        if not obj:
            if mode in {"auto"}:
                f123 = _fetch_fund123_quote(c)
                if f123.get("ok"):
                    _FUNDGZ_CACHE[cache_key] = (now, f123)
                    return f123
            if mode != "tiantian":
                fb = _fallback_quote_from_settled_nav(c)
                if fb.get("ok"):
                    _FUNDGZ_CACHE[cache_key] = (now, fb)
                    return fb
            out = {"ok": False, "error": "empty json"}
            _FUNDGZ_CACHE[cache_key] = (now, out)
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

        # Nightly/closed-window phase: prefer settled NAV snapshot (real daily
        # change). For estimate mode, only probe when quote is already marked
        # settled, to keep refresh latency low and avoid premature switch.
        try:
            now_dt = datetime.now()
            should_try_settled = False
            if mode == "tiantian":
                should_try_settled = False
            elif mode == "eastmoney":
                should_try_settled = True
            else:
                should_try_settled = (
                    now_dt.weekday() >= 5
                    or now_dt.hour >= _SETTLED_SWITCH_HOUR
                    or _is_nav_settled(out)
                )
            if should_try_settled:
                snap = _fetch_settled_nav_snapshot(c, str(out.get("jzrq") or ""))
                if snap:
                    snap_nav = _safe_float(snap.get("nav"))
                    snap_prev = _safe_float(snap.get("prev_nav")) if snap.get("prev_nav") is not None else None
                    snap_pct = _safe_float(snap.get("daily_change_pct")) if snap.get("daily_change_pct") is not None else None
                    snap_jzrq = str(snap.get("jzrq") or "").strip()
                    snap_date = _parse_local_date(snap_jzrq)
                    quote_date = _parse_local_date(out.get("jzrq"))
                    if snap_nav is not None and (
                        quote_date is None
                        or snap_date is None
                        or snap_date >= quote_date
                    ):
                        out["nav"] = snap_nav
                        if snap_prev is not None:
                            out["prev_nav"] = snap_prev
                        if snap_pct is not None:
                            out["daily_change_pct"] = snap_pct
                        out["jzrq"] = snap_jzrq or str(out.get("jzrq") or "").strip()
                        out["source"] = "settled_nav"
        except Exception:
            pass

        _FUNDGZ_CACHE[cache_key] = (now, out)
        return out

    except Exception as e:
        if mode in {"auto"}:
            f123 = _fetch_fund123_quote(c)
            if f123.get("ok"):
                _FUNDGZ_CACHE[cache_key] = (now, f123)
                return f123
        if mode != "tiantian":
            fb = _fallback_quote_from_settled_nav(c)
            if fb.get("ok"):
                _FUNDGZ_CACHE[cache_key] = (now, fb)
                return fb
        out = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        _FUNDGZ_CACHE[cache_key] = (now, out)
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


def _get_sector_label(code: str, name: str = "", resolve_cache: bool = True) -> str:
    ov = get_sector_override(code)
    if ov:
        return ov

    # 优先使用板块缓存表（miss 时会拉取一次并回填）。
    if resolve_cache:
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


def enrich_position(pos: Dict[str, Any], quote_source: str = "auto") -> Dict[str, Any]:
    code = str(pos.get("code") or "").strip()
    shares = _safe_float(pos.get("shares")) or 0.0
    cost = _safe_float(pos.get("cost")) or 0.0
    source_mode = _norm_quote_source_mode(quote_source)
    # Fast quote modes should avoid heavy settled/sector backfill on each refresh.
    fast_mode = source_mode in {"tiantian", "fund123", "baidu", "eastmoney"}

    gz = fetch_fund_gz(code, source_mode=source_mode) if code else {"ok": False}
    name = str(gz.get("name") or "").strip() if gz.get("ok") else ""
    if not name and code and not fast_mode:
        # settled-only path may return empty name for some funds; fallback by code map.
        try:
            from data import get_fund_name

            name = str(get_fund_name(code) or "").strip()
        except Exception:
            name = ""
    if not name and code and fast_mode:
        name = code

    nav = _safe_float(gz.get("nav")) if gz.get("ok") else None
    prev_nav = _safe_float(gz.get("prev_nav")) if gz.get("ok") else None

    # Keep estimate metrics for intraday "当日涨幅/当日收益" display.
    est_nav = nav
    est_prev_nav = prev_nav
    est_daily_change_pct = _safe_float(gz.get("daily_change_pct")) if gz.get("ok") else None
    if est_daily_change_pct is None and est_nav is not None and est_prev_nav is not None and est_prev_nav != 0:
        est_daily_change_pct = (float(est_nav) - float(est_prev_nav)) / float(est_prev_nav) * 100.0

    jzrq = str(gz.get("jzrq") or "").strip() if gz.get("ok") else ""
    nav_settled = _is_nav_settled(gz) if gz.get("ok") else False

    # 持仓页口径：白天冻结“市值/持有收益”（不随估值跳动），
    # 但保留“当日涨幅/当日收益”的盘中估值展示。
    daily_change_pct: Optional[float] = est_daily_change_pct
    if not nav_settled:
        if prev_nav is not None:
            nav = prev_nav

    market_value = (shares * nav) if (nav is not None) else None
    holding_profit = (shares * (nav - cost)) if (nav is not None) else None
    holding_profit_pct = ((nav - cost) / cost * 100.0) if (nav is not None and cost > 0) else None
    if not nav_settled:
        if est_nav is not None and est_prev_nav is not None:
            daily_profit = shares * (est_nav - est_prev_nav)
        elif daily_change_pct is not None and nav is not None:
            daily_profit = shares * nav * (daily_change_pct / 100.0)
        else:
            daily_profit = None
    else:
        daily_profit = (shares * (nav - prev_nav)) if (nav is not None and prev_nav is not None) else None

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

    sector_label = _get_sector_label(code, name, resolve_cache=not fast_mode)
    sector_pct = None
    if not fast_mode:
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
            "data_source": str(gz.get("source") or "fundgz") if gz.get("ok") else "",
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


def list_positions(account_id: Optional[int] = None, quote_source: str = "auto") -> List[Dict[str, Any]]:
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
    if not rows:
        return []

    # Quote enrichment is network-bound; run in parallel to avoid serial timeout
    # accumulation when portfolio contains many funds.
    worker_count = min(
        max(1, int(_PORTFOLIO_ENRICH_MAX_WORKERS)),
        max(1, len(rows)),
    )
    if worker_count <= 1:
        return [enrich_position(dict(r), quote_source=quote_source) for r in rows]

    indexed_rows = [(idx, dict(r)) for idx, r in enumerate(rows)]
    ordered: List[Optional[Dict[str, Any]]] = [None] * len(indexed_rows)
    pool = ThreadPoolExecutor(max_workers=worker_count)
    try:
        fut_map = {
            pool.submit(enrich_position, row, quote_source=quote_source): idx
            for idx, row in indexed_rows
        }
        try:
            completed_futs = set(
                as_completed(
                    fut_map,
                    timeout=_PORTFOLIO_ENRICH_TIMEOUT_SECONDS,
                )
            )
        except FuturesTimeoutError:
            completed_futs = {f for f in fut_map.keys() if f.done()}

        for fut in completed_futs:
            idx = fut_map[fut]
            base = indexed_rows[idx][1]
            try:
                ordered[idx] = fut.result()
            except Exception:
                pass

        for fut, idx in fut_map.items():
            if ordered[idx] is not None:
                continue
            base = indexed_rows[idx][1]
            try:
                fut.cancel()
            except Exception:
                pass
            # Keep endpoint available even if individual quote fetch fails/times out.
            fallback = dict(base)
            fallback.update(
                {
                    "name": str(base.get("code") or ""),
                    "sector": "未知板块",
                    "sector_pct": None,
                    "latest_nav": None,
                    "prev_nav": None,
                    "daily_change_pct": None,
                    "daily_profit": None,
                    "market_value": None,
                    "holding_profit": None,
                    "holding_profit_pct": None,
                    "data_source": "",
                    "jzrq": "",
                    "nav_settled": False,
                    "gztime": "",
                }
            )
            ordered[idx] = fallback
    finally:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

    return [x for x in ordered if isinstance(x, dict)]


def get_position(
    code: str,
    account_id: Optional[int] = None,
    quote_source: str = "auto",
) -> Optional[Dict[str, Any]]:
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
    return enrich_position(dict(row), quote_source=quote_source) if row else None


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
