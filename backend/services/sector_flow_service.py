# backend/services/sector_flow_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Dict, List, Optional
import math
import os
import time
import logging

import requests

from backend.core.config import get_settings

logger = logging.getLogger("fund_quant_bot")

_ALLOWED_INDICATOR = {"今日", "5日", "10日"}
_ALLOWED_SECTOR_TYPE = {"行业资金流", "概念资金流", "地域资金流"}


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _seconds_since(ts_str: str) -> float:
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return (datetime.now() - dt).total_seconds()
    except Exception:
        return float("inf")


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass
    s = str(v).strip().lower()
    return s in ("", "--", "-", "—", "none", "nan")


def _pick_first(row: Any, keys: List[str]) -> Optional[Any]:
    for k in keys:
        try:
            v = row.get(k)
        except Exception:
            v = None
        if not _is_missing(v):
            return v
    return None


def _find_col(
    cols: List[str],
    include_any: List[str],
    include_all: List[str] = None,
    exclude_any: List[str] = None,
) -> Optional[str]:
    include_all = include_all or []
    exclude_any = exclude_any or []

    def ok(c: str) -> bool:
        if not any(k in c for k in include_any):
            return False
        if any(k in c for k in exclude_any):
            return False
        for k in include_all:
            if k not in c:
                return False
        return True

    for c in cols:
        if ok(c):
            return c
    return None


def _parse_percent(x: Any) -> float:
    if x is None:
        return 0.0
    try:
        if isinstance(x, float) and math.isnan(x):
            return 0.0
    except Exception:
        pass
    s = str(x).strip().replace(",", "")
    if s in ("--", "-", "—", "", "None", "nan", "NaN"):
        return 0.0
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_cn_amount_to_yi(x: Any) -> float:
    if x is None:
        return 0.0

    try:
        if isinstance(x, float) and math.isnan(x):
            return 0.0
    except Exception:
        pass

    if isinstance(x, (int, float)):
        v = float(x)
        if abs(v) >= 1e6:
            return v / 1e8
        return v

    s = str(x).strip().replace(",", "")
    if s in ("--", "-", "—", "", "None", "nan", "NaN"):
        return 0.0

    try:
        v = float(s)
        if abs(v) >= 1e6:
            return v / 1e8
        return v
    except Exception:
        pass

    try:
        if s.endswith("亿"):
            return float(s[:-1])
        if s.endswith("万"):
            return float(s[:-1]) / 10000.0
        if s.endswith("元"):
            return float(s[:-1]) / 1e8
    except Exception:
        return 0.0

    return 0.0


# -----------------------
# AkShare: hard-disable proxy
# -----------------------
def _clear_proxy_env_temporarily():
    keys = [
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "all_proxy",
        "NO_PROXY", "no_proxy",
    ]
    removed = {}
    for k in keys:
        if k in os.environ:
            removed[k] = os.environ.pop(k)
    return removed


def _restore_env(removed: Dict[str, str]):
    for k, v in (removed or {}).items():
        os.environ[k] = v


def akshare_no_proxy():
    """
    Context manager:
    - clears proxy env vars
    - NO_PROXY='*'
    - monkeypatch requests proxy resolution
    - forces Session.trust_env=False + proxies={}
    """
    class _Ctx:
        def __enter__(self):
            self._removed = _clear_proxy_env_temporarily()
            self._old_no_proxy = os.environ.get("NO_PROXY")
            self._old_no_proxy2 = os.environ.get("no_proxy")
            os.environ["NO_PROXY"] = "*"
            os.environ["no_proxy"] = "*"

            self._old_get_environ_proxies = getattr(requests.utils, "get_environ_proxies", None)
            self._old_should_bypass = getattr(requests.utils, "should_bypass_proxies", None)

            try:
                if self._old_get_environ_proxies is not None:
                    requests.utils.get_environ_proxies = lambda url, no_proxy=None: {}
            except Exception:
                pass

            try:
                if self._old_should_bypass is not None:
                    requests.utils.should_bypass_proxies = lambda url, no_proxy=None: True
            except Exception:
                pass

            self._old_session_request = getattr(requests.sessions.Session, "request", None)

            def _patched_request(session_self, method, url, **kwargs):
                try:
                    session_self.trust_env = False
                except Exception:
                    pass

                kwargs.pop("proxies", None)
                kwargs["proxies"] = {}

                hdrs = dict(kwargs.get("headers") or {})
                if "User-Agent" not in hdrs and "user-agent" not in hdrs:
                    hdrs["User-Agent"] = (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36"
                    )
                hdrs.setdefault("Accept", "*/*")
                hdrs.setdefault("Accept-Language", "zh-CN,zh;q=0.9")
                hdrs.setdefault("Referer", "https://quote.eastmoney.com/")
                hdrs.setdefault("Connection", "close")
                kwargs["headers"] = hdrs

                if "timeout" not in kwargs:
                    kwargs["timeout"] = (5, 20)

                resp = self._old_session_request(session_self, method, url, **kwargs)

                try:
                    u = str(url)
                    if "eastmoney.com" in u or "push2" in u or "/api/qt/" in u:
                        resp.encoding = "utf-8"
                except Exception:
                    pass

                return resp

            try:
                if self._old_session_request is not None:
                    requests.sessions.Session.request = _patched_request
            except Exception:
                pass

            return self

        def __exit__(self, exc_type, exc, tb):
            try:
                if self._old_get_environ_proxies is not None:
                    requests.utils.get_environ_proxies = self._old_get_environ_proxies
            except Exception:
                pass

            try:
                if self._old_should_bypass is not None:
                    requests.utils.should_bypass_proxies = self._old_should_bypass
            except Exception:
                pass

            try:
                if self._old_session_request is not None:
                    requests.sessions.Session.request = self._old_session_request
            except Exception:
                pass

            if self._old_no_proxy is None:
                os.environ.pop("NO_PROXY", None)
            else:
                os.environ["NO_PROXY"] = self._old_no_proxy

            if self._old_no_proxy2 is None:
                os.environ.pop("no_proxy", None)
            else:
                os.environ["no_proxy"] = self._old_no_proxy2

            _restore_env(getattr(self, "_removed", None))
            return False

    return _Ctx()


# -----------------------
# TuShare helpers
# -----------------------
try:
    import tushare as ts  # optional
except Exception:
    ts = None

_TUSHARE_PRO = None


def _get_tushare_pro():
    global _TUSHARE_PRO
    if ts is None:
        return None
    if _TUSHARE_PRO is not None:
        return _TUSHARE_PRO
    settings = get_settings()
    token = settings.tushare_token.strip()
    if not token:
        return None
    try:
        ts.set_token(token)
        _TUSHARE_PRO = ts.pro_api()
        return _TUSHARE_PRO
    except Exception:
        return None


def _sector_type_to_content_type(sector_type: str) -> str:
    if sector_type == "概念资金流":
        return "概念"
    if sector_type == "地域资金流":
        return "地域"
    return "行业"


def _indicator_to_n(indicator: str) -> int:
    if indicator == "5日":
        return 5
    if indicator == "10日":
        return 10
    return 1


def _indicator_to_ths_symbol(indicator: str) -> str:
    if indicator == "5日":
        return "5日排行"
    if indicator == "10日":
        return "10日排行"
    return "即时"


def _to_yi_from_yuan(v: Any) -> float:
    try:
        return float(v or 0.0) / 1e8
    except Exception:
        return 0.0


def _parse_rate(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, float) and math.isnan(v):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _tushare_last_trade_dates(n: int) -> List[str]:
    pro = _get_tushare_pro()
    if pro is None:
        return []
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=max(20, n * 10))).strftime("%Y%m%d")
    try:
        df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end, fields="cal_date,is_open")
        if df is None or len(df) == 0:
            return []
        df2 = df[df["is_open"] == 1].sort_values("cal_date")
        dates = [str(x) for x in df2["cal_date"].tolist()]
        return dates[-n:]
    except Exception:
        return []


def _fetch_sector_fund_flow_tushare(indicator: str, sector_type: str, top_n: int) -> Dict[str, Any]:
    pro = _get_tushare_pro()
    if pro is None:
        return {"ok": False, "error": "tushare not available"}

    n = _indicator_to_n(indicator)
    content_type = _sector_type_to_content_type(sector_type)
    dates = _tushare_last_trade_dates(n)
    if not dates:
        return {"ok": False, "error": "no trade dates"}

    agg: Dict[str, Dict[str, Any]] = {}
    for td in dates:
        try:
            df = pro.moneyflow_ind_dc(
                trade_date=td,
                content_type=content_type,
                fields="trade_date,content_type,ts_code,name,pct_change,close,net_amount,net_amount_rate,rank",
            )
        except Exception as e:
            return {"ok": False, "error": f"moneyflow_ind_dc failed: {type(e).__name__}: {e}"}

        if df is None or len(df) == 0:
            continue

        for _, r in df.iterrows():
            ts_code = str(r.get("ts_code") or "").strip()
            if not ts_code:
                continue
            name = str(r.get("name") or "").strip()

            net_yi = _to_yi_from_yuan(r.get("net_amount"))
            rate = _parse_rate(r.get("net_amount_rate"))

            it = agg.get(ts_code)
            if it is None:
                agg[ts_code] = {
                    "name": name,
                    "code": ts_code,
                    "main_net": 0.0,
                    "main_net_pct": 0.0,
                    "chg_pct": r.get("pct_change"),
                    "close": r.get("close"),
                }
                it = agg[ts_code]

            it["main_net"] = float(it.get("main_net", 0.0)) + float(net_yi)
            it["main_net_pct"] = float(it.get("main_net_pct", 0.0)) + float(rate)

    items = list(agg.values())
    if not items:
        return {"ok": False, "error": "empty"}

    for it in items:
        it["main_net_pct"] = float(it.get("main_net_pct", 0.0)) / float(max(1, len(dates)))
        net = float(it.get("main_net", 0.0))
        it["main_inflow"] = round(net if net > 0 else 0.0, 4)
        it["main_outflow"] = round((-net) if net < 0 else 0.0, 4)
        it["main_net"] = round(net, 4)
        it["chg_pct"] = "" if _is_missing(it.get("chg_pct")) else str(it.get("chg_pct"))
        it["unit"] = "亿元"

    items.sort(key=lambda x: x.get("main_net", 0.0), reverse=True)
    return {
        "ok": True,
        "items": items[: int(top_n)],
        "debug_columns": ["trade_date", "content_type", "ts_code", "name", "pct_change", "close", "net_amount", "net_amount_rate", "rank"],
        "fetched_dates": dates,
    }


def _fetch_sector_fund_flow_ths(indicator: str, sector_type: str, top_n: int) -> Dict[str, Any]:
    """
    同花顺板块资金流兜底：
    - 行业资金流: stock_fund_flow_industry
    - 概念资金流: stock_fund_flow_concept
    - 地域资金流: 同花顺无对应公开接口（返回 not supported）
    """
    if sector_type == "地域资金流":
        return {"ok": False, "error": "ths does not support 地域资金流"}

    try:
        import akshare as ak  # type: ignore
    except Exception as e:
        return {"ok": False, "error": f"akshare not available for ths fallback: {type(e).__name__}: {e}"}

    symbol = _indicator_to_ths_symbol(indicator)

    try:
        with akshare_no_proxy():
            if sector_type == "行业资金流":
                df = ak.stock_fund_flow_industry(symbol=symbol)
            else:
                df = ak.stock_fund_flow_concept(symbol=symbol)
    except Exception as e:
        return {"ok": False, "error": f"ths fetch failed: {type(e).__name__}: {e}"}

    if df is None or len(df) == 0:
        return {"ok": False, "error": "ths empty"}

    cols = list(df.columns)
    items: List[Dict[str, Any]] = []
    for _, r in df.head(int(top_n)).iterrows():
        name = _pick_first(r, ["行业", "概念", "板块名称", "名称"]) or ""
        code = _pick_first(r, ["代码", "板块代码"]) or ""

        chg = _pick_first(r, ["行业-涨跌幅", "阶段涨跌幅", "涨跌幅"])
        inflow_raw = _pick_first(r, ["流入资金", "流入"])
        outflow_raw = _pick_first(r, ["流出资金", "流出"])
        net_raw = _pick_first(r, ["净额", "资金流入净额", "主力净流入-净额"])

        inflow_yi = _parse_cn_amount_to_yi(inflow_raw) if not _is_missing(inflow_raw) else 0.0
        outflow_yi = _parse_cn_amount_to_yi(outflow_raw) if not _is_missing(outflow_raw) else 0.0
        net_yi = _parse_cn_amount_to_yi(net_raw) if not _is_missing(net_raw) else 0.0

        if net_yi == 0.0 and (inflow_yi != 0.0 or outflow_yi != 0.0):
            net_yi = inflow_yi - outflow_yi
        if inflow_yi == 0.0 and outflow_yi == 0.0 and net_yi != 0.0:
            inflow_yi = net_yi if net_yi > 0 else 0.0
            outflow_yi = (-net_yi) if net_yi < 0 else 0.0

        items.append(
            {
                "name": str(name),
                "code": "" if _is_missing(code) else str(code),
                "chg_pct": "" if _is_missing(chg) else str(chg),
                "main_net": round(net_yi, 4),
                "main_inflow": round(inflow_yi, 4),
                "main_outflow": round(outflow_yi, 4),
                "main_net_pct": 0.0,
                "unit": "亿元",
            }
        )

    items.sort(key=lambda x: x.get("main_net", 0.0), reverse=True)
    return {
        "ok": True,
        "items": items[: int(top_n)],
        "debug_columns": cols,
    }


# -----------------------
# Service with cache
# -----------------------
_SECTOR_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = Lock()


def _normalize_provider(provider: str) -> str:
    p = (provider or "").strip().lower()
    if p in ("ak", "akshare", "ak_share"):
        return "akshare"
    if p in ("ts", "tushare", "tu"):
        return "tushare"
    if p in ("ths", "tonghuashun", "10jqka"):
        return "ths"
    if p in ("auto", ""):
        return "auto"
    return p


def _cache_key(provider: str, indicator: str, sector_type: str) -> str:
    return f"{provider}::{indicator}::{sector_type}"


def sector_fund_flow_core(
    indicator: str = "今日",
    sector_type: str = "行业资金流",
    top_n: int = 30,
    provider: Optional[str] = None,
) -> Dict[str, Any]:
    if indicator not in _ALLOWED_INDICATOR:
        return {"ok": False, "error": f"indicator must be one of {_ALLOWED_INDICATOR}"}
    if sector_type not in _ALLOWED_SECTOR_TYPE:
        return {"ok": False, "error": f"sector_type must be one of {_ALLOWED_SECTOR_TYPE}"}
    if top_n <= 0 or top_n > 200:
        return {"ok": False, "error": "top_n must be between 1 and 200"}

    settings = get_settings()
    p = _normalize_provider(provider or settings.fund_board_provider)

    ck = _cache_key(p, indicator, sector_type)

    with _CACHE_LOCK:
        cached = _SECTOR_CACHE.get(ck)

    if cached:
        age = _seconds_since(cached.get("fetched_at", ""))
        if age <= settings.sector_cache_ttl_seconds:
            items_full = cached.get("items", [])
            return {
                "ok": True,
                "generated_at": _now_str(),
                "fetched_at": cached.get("fetched_at", ""),
                "stale": False,
                "warning": "",
                "indicator": indicator,
                "sector_type": sector_type,
                "top_n": top_n,
                "items": items_full[: int(top_n)],
                "debug_columns": cached.get("debug_columns", []),
                "provider": "cache",
            }

    # 1) TuShare (preferred when provider=tushare/auto)
    if p in ("tushare", "auto") and ts is not None:
        ts_res = _fetch_sector_fund_flow_tushare(indicator=indicator, sector_type=sector_type, top_n=top_n)
        if ts_res.get("ok"):
            fetched_at = _now_str()
            items = ts_res.get("items", [])
            with _CACHE_LOCK:
                _SECTOR_CACHE[ck] = {"fetched_at": fetched_at, "items": items, "debug_columns": ts_res.get("debug_columns", [])}
            return {
                "ok": True,
                "generated_at": _now_str(),
                "fetched_at": fetched_at,
                "stale": False,
                "warning": "",
                "indicator": indicator,
                "sector_type": sector_type,
                "top_n": top_n,
                "items": items,
                "debug_columns": ts_res.get("debug_columns", []),
                "provider": "tushare",
                "fetched_dates": ts_res.get("fetched_dates", []),
            }
        logger.warning("TuShare not available (%s), falling back to AkShare", ts_res.get("error"))

    # 2) THS-only mode
    if p == "ths":
        ths_res = _fetch_sector_fund_flow_ths(indicator=indicator, sector_type=sector_type, top_n=top_n)
        if ths_res.get("ok"):
            fetched_at = _now_str()
            items = ths_res.get("items", [])
            with _CACHE_LOCK:
                _SECTOR_CACHE[ck] = {"fetched_at": fetched_at, "items": items, "debug_columns": ths_res.get("debug_columns", [])}
            return {
                "ok": True,
                "generated_at": _now_str(),
                "fetched_at": fetched_at,
                "stale": False,
                "warning": "",
                "indicator": indicator,
                "sector_type": sector_type,
                "top_n": top_n,
                "items": items,
                "debug_columns": ths_res.get("debug_columns", []),
                "provider": "ths",
            }

        err = f"ths error: {ths_res.get('error')}"
        if cached and cached.get("items"):
            items_full = cached.get("items", [])
            return {
                "ok": True,
                "generated_at": _now_str(),
                "fetched_at": cached.get("fetched_at", ""),
                "stale": True,
                "warning": f"数据源拉取失败，展示上次成功结果（可能已过期）。原因：{err}",
                "indicator": indicator,
                "sector_type": sector_type,
                "top_n": top_n,
                "items": items_full[: int(top_n)],
                "debug_columns": cached.get("debug_columns", []),
                "provider": "cache",
            }
        return {
            "ok": False,
            "generated_at": _now_str(),
            "fetched_at": "",
            "stale": True,
            "warning": f"数据源拉取失败，且暂无缓存可用。原因：{err}. 可尝试：切换 ?provider=auto 或 ?provider=tushare。",
            "indicator": indicator,
            "sector_type": sector_type,
            "top_n": top_n,
            "items": [],
            "debug_columns": [],
            "provider": "ths",
        }

    # 3) AkShare
    try:
        import akshare as ak  # type: ignore

        last_exc: Optional[Exception] = None
        df = None

        with akshare_no_proxy():
            for attempt in range(3):
                try:
                    df = ak.stock_sector_fund_flow_rank(indicator=indicator, sector_type=sector_type)
                    break
                except Exception as e:
                    last_exc = e
                    time.sleep([0.6, 1.4, 3.0][attempt])

        if df is None:
            raise last_exc or RuntimeError("akshare fetch returned None")
        cols = list(df.columns)
    except Exception as e:
        err = f"akshare error: {type(e).__name__}: {e}"
        logger.exception("sector_fund_flow fetch failed: %s", err)

        # fallback to 同花顺 (auto / akshare)
        if p in ("auto", "akshare"):
            ths_res = _fetch_sector_fund_flow_ths(indicator=indicator, sector_type=sector_type, top_n=top_n)
            if ths_res.get("ok"):
                fetched_at = _now_str()
                items = ths_res.get("items", [])
                with _CACHE_LOCK:
                    _SECTOR_CACHE[ck] = {"fetched_at": fetched_at, "items": items, "debug_columns": ths_res.get("debug_columns", [])}
                return {
                    "ok": True,
                    "generated_at": _now_str(),
                    "fetched_at": fetched_at,
                    "stale": False,
                    "warning": f"AkShare 拉取失败，已自动切换同花顺源。原因：{err}",
                    "indicator": indicator,
                    "sector_type": sector_type,
                    "top_n": top_n,
                    "items": items,
                    "debug_columns": ths_res.get("debug_columns", []),
                    "provider": "ths",
                }
            logger.warning("THS fallback failed (%s)", ths_res.get("error"))

        # stale fallback
        if cached and cached.get("items"):
            items_full = cached.get("items", [])
            return {
                "ok": True,
                "generated_at": _now_str(),
                "fetched_at": cached.get("fetched_at", ""),
                "stale": True,
                "warning": f"数据源拉取失败，展示上次成功结果（可能已过期）。原因：{err}",
                "indicator": indicator,
                "sector_type": sector_type,
                "top_n": top_n,
                "items": items_full[: int(top_n)],
                "debug_columns": cached.get("debug_columns", []),
                "provider": "cache",
            }

        return {
            "ok": False,
            "generated_at": _now_str(),
            "fetched_at": "",
            "stale": True,
            "warning": f"数据源拉取失败，且暂无缓存可用。原因：{err}. 可尝试：稍后重试 / 降低 top_n / ?provider=akshare。",
            "indicator": indicator,
            "sector_type": sector_type,
            "top_n": top_n,
            "items": [],
            "debug_columns": [],
            "provider": "akshare",
        }

    chg_col = _find_col(cols, include_any=["涨跌幅"], include_all=[], exclude_any=[])
    net_col = _find_col(cols, include_any=["净流入"], include_all=["主力"], exclude_any=["占比", "%"])
    net_pct_col = _find_col(cols, include_any=["净占比", "占比"], include_all=["主力"], exclude_any=[])
    inflow_col = _find_col(cols, include_any=["流入"], include_all=["主力"], exclude_any=["净", "占比", "%"])
    outflow_col = _find_col(cols, include_any=["流出"], include_all=["主力"], exclude_any=["占比", "%"])

    items: List[Dict[str, Any]] = []

    for _, r in df.head(int(top_n)).iterrows():
        name = _pick_first(r, ["板块名称", "板块", "名称"]) or ""
        code = _pick_first(r, ["板块代码", "代码"]) or ""

        chg = r.get(chg_col) if chg_col else _pick_first(r, ["涨跌幅", "今日涨跌幅", "区间涨跌幅"])

        net_raw = r.get(net_col) if net_col else _pick_first(
            r, ["主力净流入-净额", "主力净流入净额", "主力净流入", "主力资金净流入", "主力净流入额"]
        )
        net_pct_raw = r.get(net_pct_col) if net_pct_col else _pick_first(
            r, ["主力净流入-净占比", "主力净流入净占比", "主力净流入占比", "主力净占比"]
        )

        inflow_raw = r.get(inflow_col) if inflow_col else _pick_first(r, ["主力流入", "主力资金流入", "主力资金流入额"])
        outflow_raw = r.get(outflow_col) if outflow_col else _pick_first(
            r, ["主力流出", "主力资金流出", "主力资金流出额", "主力净流出", "主力净流出额"]
        )

        inflow_yi = _parse_cn_amount_to_yi(inflow_raw) if not _is_missing(inflow_raw) else 0.0
        outflow_yi = _parse_cn_amount_to_yi(outflow_raw) if not _is_missing(outflow_raw) else 0.0
        net_yi = _parse_cn_amount_to_yi(net_raw) if not _is_missing(net_raw) else 0.0

        if net_yi == 0.0 and (inflow_yi != 0.0 or outflow_yi != 0.0):
            net_yi = inflow_yi - outflow_yi

        if inflow_yi == 0.0 and outflow_yi == 0.0 and net_yi != 0.0:
            inflow_yi = net_yi if net_yi > 0 else 0.0
            outflow_yi = (-net_yi) if net_yi < 0 else 0.0

        items.append(
            {
                "name": str(name),
                "code": "" if _is_missing(code) else str(code),
                "chg_pct": "" if _is_missing(chg) else str(chg),
                "main_net": round(net_yi, 4),
                "main_inflow": round(inflow_yi, 4),
                "main_outflow": round(outflow_yi, 4),
                "main_net_pct": _parse_percent(net_pct_raw),
                "unit": "亿元",
            }
        )

    fetched_at = _now_str()
    with _CACHE_LOCK:
        _SECTOR_CACHE[ck] = {"fetched_at": fetched_at, "items": items, "debug_columns": cols}

    return {
        "ok": True,
        "generated_at": _now_str(),
        "fetched_at": fetched_at,
        "stale": False,
        "warning": "",
        "indicator": indicator,
        "sector_type": sector_type,
        "top_n": top_n,
        "items": items,
        "debug_columns": cols,
        "provider": "akshare",
    }


def diagnostics_providers() -> Dict[str, Any]:
    settings = get_settings()
    token = settings.tushare_token
    return {
        "now": _now_str(),
        "board_provider": settings.fund_board_provider,
        "tushare_pkg_installed": ts is not None,
        "has_tushare_token": bool(token.strip()),
        "tushare_token_len": len(token.strip()),
        "proxy_env_present": {k: (k in os.environ) for k in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]},
        "requests_proxies": requests.utils.getproxies(),
    }
