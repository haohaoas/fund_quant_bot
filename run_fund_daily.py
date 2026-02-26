# run_fund_daily.py
def compute_daily_result() -> dict:
    """
    给 API / reco_service 用的结构化结果
    ⚠️ 这是 run_fund_daily 的“计算版”，不 print
    """

    funds = []

    # 复用你现有逻辑：WATCH_FUNDS + 最新价格 + AI 决策
    for code, cfg in WATCH_FUNDS.items():
        code_str = str(code)

        latest = get_latest_price(code_str)
        if latest is None:
            continue

        price = latest.get("price")

        sig = generate_today_signal(code_str, price)

        ai_decision = ask_deepseek_fund_decision(
            fund_name=cfg.get("name", code_str) if isinstance(cfg, dict) else code_str,
            code=code_str,
            latest=latest,
            quant_signal=sig,
            sector_info={},        # API 版先不塞复杂对象
            fund_profile=None,
        )

        funds.append(
            {
                "code": code_str,
                "name": cfg.get("name", code_str) if isinstance(cfg, dict) else code_str,
                "latest": {
                    "price": price,
                    "time": (
                        latest["time"].isoformat()
                        if hasattr(latest.get("time"), "isoformat")
                        else str(latest.get("time"))
                    ),
                },
                "signal": sig.get("action"),
                "ai_decision": {
                    "action": ai_decision.get("action"),
                    "reason": ai_decision.get("reason"),
                },
            }
        )

    return {
        "news": None,
        "funds": funds,
        "market_picker": None,
    }

import os

# ==== 代理环境变量兜底（关键修复：你的报错来自 ProxyError）====
# 很多机器会配置 HTTP(S)_PROXY，requests 默认会读取并导致 push2.eastmoney.com 连接失败。
# 默认行为：清理代理变量并强制 NO_PROXY=*（对所有域名直连）。
# 如你确实需要代理：运行前设置 FUND_DISABLE_PROXY=0
if str(os.environ.get("FUND_DISABLE_PROXY", "1")).strip().lower() in ("1", "true", "yes"):
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)
    # requests/urllib3 会尊重 no_proxy/NO_PROXY
    os.environ.setdefault("NO_PROXY", "*")
    os.environ.setdefault("no_proxy", "*")

from config import WATCH_FUNDS
from data import get_fund_latest_price
from strategy import generate_today_signal
from sector import get_sector_by_fund, get_sector_sentiment
from ai_advisor import ask_deepseek_fund_decision
from news_sentiment import get_market_news_sentiment
from ai_picker import pick_funds_for_tomorrow

from datetime import datetime, date, timedelta
import time
import difflib

# K线兜底：直接请求东方财富 push2his
# K线兜底：直接请求东方财富 push2his
try:
    import requests
except Exception:
    requests = None


# ==== HTTP 工具：统一 Session + 重试 + 代理容错 ====
_HTTP_SESSION = None


def _get_http_session():
    """返回一个全局 requests.Session。

    说明：很多用户环境里会配置错误的 HTTP(S)_PROXY，导致 push2.eastmoney.com 连接失败。
    - 默认：trust_env=False（忽略环境变量代理），更稳。
    - 如你确实需要代理：把环境变量 FUND_TRUST_ENV_PROXY=1 即可恢复 trust_env=True。
    """
    global _HTTP_SESSION
    if requests is None:
        return None
    if _HTTP_SESSION is not None:
        return _HTTP_SESSION

    import os
    s = requests.Session()

    # 默认忽略环境变量代理（避免 ProxyError / RemoteDisconnected）
    trust_env = str(os.environ.get("FUND_TRUST_ENV_PROXY", "0")).strip() in ("1", "true", "True")
    s.trust_env = bool(trust_env)

    # 统一 UA/Referer
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://quote.eastmoney.com",
            "Accept": "application/json,text/plain,*/*",
        }
    )

    _HTTP_SESSION = s
    return s


def _http_get_json(url: str, params: dict, timeout: int = 10, tries: int = 3, sleep_s: float = 0.6):
    """GET + JSON + 轻量重试。失败返回 None。"""
    import time

    s = _get_http_session()
    if s is None:
        return None

    for i in range(max(1, int(tries))):
        try:
            r = s.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(sleep_s * (i + 1))
    return None

# ==============================================================
# Providers: Tencent (intraday exchange quote) + TuShare Pro
# 目标：你说“都想换”，所以：
#   - 板块资金流/板块解析/板块K线：优先 TuShare（稳定、可控频次）
#   - 场内盘中最新价：优先腾讯快照（TuShare 多数接口盘后更稳）
#   - 场外基金/联接基金：腾讯/tuShare 不一定覆盖，保留现有 get_fund_latest_price 兜底
#
# Env:
#   TUSHARE_TOKEN=xxxx
#   FUND_PRICE_PROVIDER=auto|tencent|tushare|eastmoney
#   FUND_BOARD_PROVIDER=tushare|auto|akshare
# ==============================================================

# ---- Tencent quote: qt.gtimg.cn (intraday for exchange-listed) ----

def _to_tencent_symbol(code: str):
    c = (code or "").strip()
    if len(c) != 6 or (not c.isdigit()):
        return None

    # 场内基金常见号段
    if c.startswith("5"):
        return "sh" + c
    if c.startswith(("15", "16", "18", "159")):
        return "sz" + c

    # 股票号段兜底
    if c.startswith(("60", "68")):
        return "sh" + c
    if c.startswith(("00", "30", "20")):
        return "sz" + c

    return None


def get_fund_latest_price_tencent(code: str):
    """腾讯快照：适用于股票/ETF/LOF 等场内品种。取不到返回 None。"""
    if requests is None:
        return None

    sym = _to_tencent_symbol(code)
    if not sym:
        return None

    url = "https://qt.gtimg.cn/q=" + sym
    s = _get_http_session()
    if s is None:
        return None

    try:
        r = s.get(url, timeout=10)
        r.raise_for_status()
        text = r.content.decode("gb18030", errors="ignore")
        if '"' not in text or "~" not in text:
            return None

        inner = text.split('"', 2)[1]
        parts = inner.split("~")
        if len(parts) < 6:
            return None

        name = parts[1]
        code2 = parts[2]
        cur = float(parts[3])

        prev_close = None
        try:
            prev_close = float(parts[4])
        except Exception:
            prev_close = None

        pct = None
        if prev_close and prev_close != 0:
            pct = (cur / prev_close - 1.0) * 100.0

        return {
            "price": cur,
            "pct": pct,
            "time": datetime.now(),
            "source": "tencent",
            "name": name,
            "code": code2 or code,
            "symbol": sym,
        }
    except Exception:
        return None


# ---- TuShare Pro client ----
try:
    import tushare as ts
except Exception:
    ts = None

_TUSHARE_PRO = None
_TUSHARE_TRADEDATE_CACHE = {"ts": 0.0, "trade_date": None}


def _get_tushare_pro():
    global _TUSHARE_PRO
    if ts is None:
        return None
    if _TUSHARE_PRO is not None:
        return _TUSHARE_PRO

    token = str(os.environ.get("TUSHARE_TOKEN", "")).strip()
    if not token:
        return None

    try:
        ts.set_token(token)
        _TUSHARE_PRO = ts.pro_api()
        return _TUSHARE_PRO
    except Exception:
        return None


def _to_tushare_ts_code(code: str):
    c = (code or "").strip()
    if len(c) != 6 or (not c.isdigit()):
        return None

    # 场内基金
    if c.startswith("5"):
        return c + ".SH"
    if c.startswith(("15", "16", "18", "159")):
        return c + ".SZ"

    # 股票
    if c.startswith(("60", "68")):
        return c + ".SH"
    if c.startswith(("00", "30", "20")):
        return c + ".SZ"

    return None


def _get_last_trade_date() -> str:
    """获取最近交易日 YYYYMMDD。优先 trade_cal；失败回退今天。"""
    now = time.time()
    if _TUSHARE_TRADEDATE_CACHE["trade_date"] and (now - _TUSHARE_TRADEDATE_CACHE["ts"]) < 3600:
        return _TUSHARE_TRADEDATE_CACHE["trade_date"]

    td = datetime.now().strftime("%Y%m%d")

    pro = _get_tushare_pro()
    if pro is None:
        _TUSHARE_TRADEDATE_CACHE.update({"ts": now, "trade_date": td})
        return td

    try:
        start = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end, fields="cal_date,is_open")
        if df is None or len(df) == 0:
            raise RuntimeError("empty trade_cal")

        df2 = df[df["is_open"] == 1].sort_values("cal_date")
        if len(df2) == 0:
            raise RuntimeError("no open day")

        td = str(df2.iloc[-1]["cal_date"])
        _TUSHARE_TRADEDATE_CACHE.update({"ts": now, "trade_date": td})
        return td
    except Exception:
        _TUSHARE_TRADEDATE_CACHE.update({"ts": now, "trade_date": td})
        return td


def get_fund_latest_price_tushare(code: str):
    """TuShare：盘后稳定的最近交易日收盘价（ETF/fund_daily 或 股票/daily）。"""
    pro = _get_tushare_pro()
    if pro is None:
        return None

    ts_code = _to_tushare_ts_code(code)
    if not ts_code:
        return None

    trade_date = _get_last_trade_date()

    # ETF/基金：fund_daily
    try:
        df = pro.fund_daily(ts_code=ts_code, trade_date=trade_date,
                            fields="ts_code,trade_date,close,pre_close,pct_chg")
        if df is not None and len(df) > 0:
            r = df.iloc[0]
            return {
                "price": float(r.get("close")),
                "pct": (float(r.get("pct_chg")) if r.get("pct_chg") is not None else None),
                "time": datetime.strptime(str(r.get("trade_date")), "%Y%m%d"),
                "source": "tushare",
                "ts_code": str(r.get("ts_code")),
            }
    except Exception:
        pass

    # 股票：daily
    try:
        df = pro.daily(ts_code=ts_code, trade_date=trade_date,
                       fields="ts_code,trade_date,close,pre_close,pct_chg")
        if df is not None and len(df) > 0:
            r = df.iloc[0]
            return {
                "price": float(r.get("close")),
                "pct": (float(r.get("pct_chg")) if r.get("pct_chg") is not None else None),
                "time": datetime.strptime(str(r.get("trade_date")), "%Y%m%d"),
                "source": "tushare",
                "ts_code": str(r.get("ts_code")),
            }
    except Exception:
        return None

    return None


def get_latest_price(code: str):
    """统一最新价入口。

    - auto（默认）：盘中优先腾讯；否则/失败回退 TuShare（盘后收盘价）；再回退 eastmoney(get_fund_latest_price)
    """
    provider = str(os.environ.get("FUND_PRICE_PROVIDER", "auto")).strip().lower()

    if provider == "tencent":
        return get_fund_latest_price_tencent(code)
    if provider == "tushare":
        return get_fund_latest_price_tushare(code)
    if provider == "eastmoney":
        try:
            return get_fund_latest_price(code)
        except Exception:
            return None

    # auto
    tx = get_fund_latest_price_tencent(code)
    if tx is not None:
        return tx

    tsr = get_fund_latest_price_tushare(code)
    if tsr is not None:
        return tsr

    try:
        return get_fund_latest_price(code)
    except Exception:
        return None


# ---- TuShare: 板块资金流/板块列表解析/板块 close 序列（用于 K 线摘要） ----
# 使用接口：moneyflow_ind_dc （行业/概念/地域）

_TUSHARE_BOARD_CACHE = {"ts": 0.0, "data": {}}  # key: (trade_date, content_type)


def _tushare_content_type(bt: str) -> str:
    bt = (bt or "industry").strip().lower()
    if bt == "concept":
        return "概念"
    if bt == "region":
        return "地域"
    return "行业"


def _normalize_bk_to_dc(sym: str):
    s = (sym or "").strip()
    if not s:
        return None
    if s.endswith(".DC"):
        return s
    if s.startswith("BK"):
        return s + ".DC"
    return s


def _get_tushare_board_daily(trade_date: str, board_type: str):
    pro = _get_tushare_pro()
    if pro is None:
        return None

    td = str(trade_date)
    ct = _tushare_content_type(board_type)
    key = (td, ct)

    now = time.time()
    if key in _TUSHARE_BOARD_CACHE["data"] and (now - _TUSHARE_BOARD_CACHE["ts"]) < 300:
        return _TUSHARE_BOARD_CACHE["data"][key]

    try:
        df = pro.moneyflow_ind_dc(
            trade_date=td,
            content_type=ct,
            fields="trade_date,content_type,ts_code,name,pct_change,close,net_amount,net_amount_rate,rank",
        )
        _TUSHARE_BOARD_CACHE["ts"] = now
        _TUSHARE_BOARD_CACHE["data"][key] = df
        return df
    except Exception:
        return None


def get_market_board_fund_flow_rank_tushare(board_type: str = "industry", top_n: int = 20) -> dict:
    td = _get_last_trade_date()
    df = _get_tushare_board_daily(td, board_type)
    if df is None or len(df) == 0:
        return {"board_type": board_type, "items": [], "error": "tushare empty"}

    items = []
    for _, r in df.iterrows():
        items.append(
            {
                "name": str(r.get("name") or "").strip(),
                "symbol": str(r.get("ts_code") or "").strip(),
                "main_inflow": float(r.get("net_amount") or 0.0),
                "pct": (float(r.get("pct_change")) if r.get("pct_change") is not None else None),
                "close": (float(r.get("close")) if r.get("close") is not None else None),
                "trade_date": str(r.get("trade_date") or td),
            }
        )

    items.sort(key=lambda x: x.get("main_inflow", 0.0), reverse=True)

    try:
        n = int(top_n) if top_n is not None else 0
    except Exception:
        n = 0

    out = items if n <= 0 else items[: max(5, n)]
    return {"board_type": board_type, "items": out}


def _resolve_board_by_keyword_tushare(keyword: str) -> dict:
    kw = (keyword or "").strip()
    if not kw:
        return {"keyword": keyword, "resolved_name": None, "symbol": None, "board_type": None, "debug_candidates": []}

    td = _get_last_trade_date()

    candidates = []  # (score, name, ts_code, board_type)

    def add_from_bt(bt: str):
        df = _get_tushare_board_daily(td, bt)
        if df is None or len(df) == 0:
            return

        names = [str(x) for x in df["name"].tolist()]
        codes = [str(x) for x in df["ts_code"].tolist()]

        for n, c in zip(names, codes):
            if n == kw:
                candidates.append((1.00, n, c, bt))
                return

        for n, c in zip(names, codes):
            if kw in n or n in kw:
                candidates.append((0.80, n, c, bt))

        close = difflib.get_close_matches(kw, names, n=5, cutoff=0.6)
        for n in close:
            try:
                idx = names.index(n)
                candidates.append((0.65, n, codes[idx], bt))
            except Exception:
                pass

    add_from_bt("industry")
    add_from_bt("concept")

    if not candidates:
        return {"keyword": keyword, "resolved_name": None, "symbol": None, "board_type": None, "debug_candidates": []}

    candidates.sort(key=lambda x: (x[0], 1 if x[3] == "concept" else 0, -len(x[1])), reverse=True)
    best = candidates[0]

    debug_top = [{"score": float(s), "name": n, "symbol": sym, "board_type": bt} for (s, n, sym, bt) in candidates[:5]]

    return {"keyword": keyword, "resolved_name": best[1], "symbol": best[2], "board_type": best[3], "debug_candidates": debug_top}


def get_sector_main_fund_flow_tushare(sector_name: str, board_type: str, symbol: str = None, lookback: int = 3) -> dict:
    pro = _get_tushare_pro()
    if pro is None:
        return {"sector": sector_name, "board_type": board_type, "symbol": symbol, "error": "tushare not available"}

    td = _get_last_trade_date()
    ct = _tushare_content_type(board_type)
    ts_code = _normalize_bk_to_dc(symbol) if symbol else None

    try:
        end = datetime.strptime(td, "%Y%m%d")
    except Exception:
        end = datetime.now()
    start = (end - timedelta(days=max(10, int(lookback) * 5))).strftime("%Y%m%d")

    try:
        df = pro.moneyflow_ind_dc(
            ts_code=ts_code,
            start_date=start,
            end_date=td,
            content_type=ct,
            fields="trade_date,ts_code,name,pct_change,close,net_amount,net_amount_rate",
        )
        if df is None or len(df) == 0:
            return {"sector": sector_name, "board_type": board_type, "symbol": ts_code, "error": "empty"}

        df2 = df.sort_values("trade_date")
        tail = df2.tail(max(1, int(lookback)))
        vals = [float(x or 0.0) for x in tail["net_amount"].tolist()]

        last = df2.iloc[-1]
        last_date = str(last.get("trade_date"))

        today = vals[-1] if vals else 0.0
        ssum = sum(vals) if vals else 0.0

        return {
            "sector": str(last.get("name") or sector_name),
            "board_type": ("concept" if ct == "概念" else "industry"),
            "symbol": str(last.get("ts_code") or ts_code or ""),
            "last_date": last_date,
            "today_main_inflow": float(today),
            "sum_main_inflow_nd": float(ssum),
            "lookback_days": int(lookback),
            "today_pct": (float(last.get("pct_change")) if last.get("pct_change") is not None else None),
            "close": (float(last.get("close")) if last.get("close") is not None else None),
            "source": "tushare",
        }
    except Exception as e:
        return {"sector": sector_name, "board_type": board_type, "symbol": ts_code, "error": str(e)}


def get_sector_kline_features_tushare(sector: str, days: int = 120, tail: int = 20, symbol: str = None, board_type: str = None) -> dict:
    """TuShare 版板块K线特征：基于 moneyflow_ind_dc 的 close 序列构造（无 open/high/low）。"""
    pro = _get_tushare_pro()
    if pro is None:
        return {"sector": sector, "symbol": symbol, "error": "tushare not available"}

    td = _get_last_trade_date()
    ct = _tushare_content_type(board_type or "industry")
    ts_code = _normalize_bk_to_dc(symbol) if symbol else None

    try:
        end = datetime.strptime(td, "%Y%m%d")
    except Exception:
        end = datetime.now()
    start = (end - timedelta(days=max(200, int(days) * 3))).strftime("%Y%m%d")

    try:
        df = pro.moneyflow_ind_dc(
            ts_code=ts_code,
            start_date=start,
            end_date=td,
            content_type=ct,
            fields="trade_date,ts_code,name,close,pct_change",
        )
        if df is None or len(df) == 0:
            return {"sector": sector, "symbol": ts_code, "error": "empty"}

        df2 = df.sort_values("trade_date")
        df2 = df2.tail(max(30, int(days)))

        closes = [float(x) for x in df2["close"].tolist() if x is not None]
        dates = [str(x) for x in df2["trade_date"].tolist()]

        if len(closes) < 2:
            return {"sector": sector, "symbol": ts_code, "error": "not enough close"}

        last_close = closes[-1]
        last_date = dates[-1]

        ret_1d = _pct(closes[-1], closes[-2]) if len(closes) >= 2 else 0.0
        ret_5d = _pct(closes[-1], closes[-6]) if len(closes) >= 6 else 0.0
        ret_20d = _pct(closes[-1], closes[-21]) if len(closes) >= 21 else 0.0

        def ma(n):
            if len(closes) < n:
                return sum(closes) / len(closes)
            return sum(closes[-n:]) / n

        ma5 = ma(5)
        ma20 = ma(20)
        ma60 = ma(60)

        if ma5 > ma20 * 1.002:
            ma_cross = "bull"
        elif ma5 < ma20 * 0.998:
            ma_cross = "bear"
        else:
            ma_cross = "flat"

        rsi14 = float(_rsi(closes, 14))

        rets = []
        for i in range(1, len(closes)):
            if closes[i - 1] != 0:
                rets.append((closes[i] / closes[i - 1] - 1.0) * 100.0)
        vol20 = float(_std(rets[-20:])) if len(rets) >= 2 else 0.0

        lookback = min(20, len(closes))
        hi = max(closes[-lookback:])
        lo = min(closes[-lookback:])
        range_pos = 0.5
        if hi > lo:
            range_pos = (last_close - lo) / (hi - lo)

        t = min(int(tail), len(closes))
        candles = []
        for i in range(len(closes) - t, len(closes)):
            pct = 0.0
            if i > 0 and closes[i - 1] != 0:
                pct = (closes[i] / closes[i - 1] - 1.0) * 100.0
            candles.append({"date": dates[i], "open": None, "high": None, "low": None, "close": float(closes[i]), "pct": float(pct)})

        name = None
        try:
            name = str(df2.iloc[-1].get("name") or "").strip() or sector
        except Exception:
            name = sector

        return {
            "sector": name,
            "symbol": str(df2.iloc[-1].get("ts_code") or ts_code or ""),
            "board_type": ("concept" if ct == "概念" else "industry"),
            "last_date": last_date,
            "close": float(last_close),
            "ret_1d": float(ret_1d),
            "ret_5d": float(ret_5d),
            "ret_20d": float(ret_20d),
            "ma5": float(ma5),
            "ma20": float(ma20),
            "ma60": float(ma60),
            "ma_cross": ma_cross,
            "rsi14": float(rsi14),
            "volatility20": float(vol20),
            "range_pos_20d": float(range_pos),
            "candles": candles,
        }
    except Exception as e:
        return {"sector": sector, "symbol": ts_code, "error": str(e)}

# 可选：板块 K 线（AkShare - 东方财富）
try:
    import akshare as ak
except Exception:
    ak = None


# ==== 辅助函数：提升输出一致性，减少“矛盾感” ====

def _normalize_llm_suggest(s: str) -> str:
    """把 LLM 的 BUY/SELL/HOLD 口吻改成更像“市场倾向/关注清单”，避免一日游推荐。"""
    s = (s or "").strip().upper()
    if s.startswith("BUY"):
        return "WATCH（关注：仅在回撤/触发网格再加仓）"
    if s.startswith("SELL"):
        return "WATCH（关注：高位风险，触发止盈/止损再动作）"
    if s.startswith("HOLD"):
        return "HOLD（观望）"
    return s or "WATCH"



def _format_market_bias(news_view: dict) -> str:
    """把新闻情绪输出成更直观的“风险偏好/市场倾向”句子。"""
    if not news_view:
        return "市场倾向：未知（新闻模块不可用）"
    sent = (news_view.get("market_sentiment") or "neutral").lower()
    score = news_view.get("score", 50)
    risk = news_view.get("risk_level", "medium")
    mapping = {
        "bullish": "偏多（risk-on）",
        "bearish": "偏空（risk-off）",
        "neutral": "中性（震荡）",
        "volatile": "高波动（不确定）",
    }
    return f"市场倾向：{mapping.get(sent, sent)}，情绪分 {score}/100，风险 {risk}"




# ==== 板块K线特征（直接在本脚本内实现，避免改动其他模块） ====

def _fetch_board_kline_em_fallback(symbol: str, limit: int = 200) -> list:
    """直接从东方财富 push2his 拉 BK 板块日K（兜底用）。返回 list[dict]，字段尽量对齐 AkShare。"""
    if requests is None:
        return []

    sym = (symbol or "").strip()
    if not sym:
        return []

    # EastMoney 的板块通常用 secid=90.BKxxxx
    secid = f"90.{sym}" if not sym.startswith("90.") else sym

    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "klt": 101,   # 101=日K
        "fqt": 1,     # 1=前复权(对指数/板块影响不大)
        "lmt": int(limit),
        "end": "20500101",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        # fields1/fields2 是东财常用字段集；核心是 klines 字段
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
    }

    try:
        js = _http_get_json(url, params=params, timeout=10, tries=3, sleep_s=0.6)
        if not js:
            return []
        data = (js or {}).get("data") or {}
        klines = data.get("klines") or []
        rows = []
        for line in klines:
            # 典型格式：YYYY-MM-DD,open,close,high,low,vol,amt,amp,pct,chg,turn
            parts = str(line).split(",")
            if len(parts) < 6:
                continue
            rows.append(
                {
                    "日期": parts[0],
                    "开盘": float(parts[1]),
                    "收盘": float(parts[2]),
                    "最高": float(parts[3]),
                    "最低": float(parts[4]),
                    "成交量": float(parts[5]) if len(parts) > 5 else 0.0,
                    "成交额": float(parts[6]) if len(parts) > 6 else 0.0,
                }
            )
        return rows
    except Exception:
        return []

def _normalize_sector_name(name: str) -> str:
    """把常见别名/错别字归一化到东财板块名称，避免查不到代码。"""
    s = (name or "").strip()
    if not s:
        return s

    # 常见别名/写法差异
    alias = {
        "储存芯片": "存储芯片",
        "航空航天": "航天航空",
        "航天航空": "航天航空",
        "芯片": "半导体",
    }
    if s in alias:
        return alias[s]

    # 某些情况下只差一个字：储存/存储
    if "储存" in s:
        s2 = s.replace("储存", "存储")
        return s2

    return s

_BOARD_LIST_CACHE = {"ts": 0.0, "df": None}
_BOARD_LIST_TTL = 300  # seconds

# 概念板块列表缓存（很多基金的“板块”其实是概念，不是行业）
_CONCEPT_LIST_CACHE = {"ts": 0.0, "df": None}
_CONCEPT_LIST_TTL = 300  # seconds


def _get_industry_board_list_df():
    if ak is None:
        return None
    now = time.time()
    if _BOARD_LIST_CACHE["df"] is not None and (now - _BOARD_LIST_CACHE["ts"]) <= _BOARD_LIST_TTL:
        return _BOARD_LIST_CACHE["df"]
    try:
        df = ak.stock_board_industry_name_em()
        _BOARD_LIST_CACHE["ts"] = now
        _BOARD_LIST_CACHE["df"] = df
        return df
    except Exception:
        return None


def _get_concept_board_list_df():
    """获取东方财富概念板块列表（AkShare），带缓存。"""
    if ak is None:
        return None

    fn = getattr(ak, "stock_board_concept_name_em", None)
    if fn is None:
        return None

    now = time.time()
    if _CONCEPT_LIST_CACHE["df"] is not None and (now - _CONCEPT_LIST_CACHE["ts"]) <= _CONCEPT_LIST_TTL:
        return _CONCEPT_LIST_CACHE["df"]

    try:
        df = fn()
        _CONCEPT_LIST_CACHE["ts"] = now
        _CONCEPT_LIST_CACHE["df"] = df
        return df
    except Exception:
        return None


def _find_concept_board_symbol(board_name: str):
    """用板块名称找到概念板块代码（BKxxxx）。"""
    df = _get_concept_board_list_df()
    if df is None:
        return None

    name = (board_name or "").strip()
    if not name:
        return None

    # AkShare 概念板块列表常见列名："板块名称"、"板块代码"（与行业一致）；这里做容错
    col_name = "板块名称" if "板块名称" in df.columns else ("概念名称" if "概念名称" in df.columns else None)
    col_code = "板块代码" if "板块代码" in df.columns else ("概念代码" if "概念代码" in df.columns else None)
    if col_name is None or col_code is None:
        return None

    try:
        sub = df[df[col_name].astype(str).str.contains(name, na=False)]
        if len(sub) == 0:
            return None
        return str(sub.iloc[0][col_code]).strip()
    except Exception:
        return None


def _find_industry_board_symbol(board_name: str):
    df = _get_industry_board_list_df()
    if df is None:
        return None
    name = (board_name or "").strip()
    if not name:
        return None
    try:
        sub = df[df["板块名称"].astype(str).str.contains(name, na=False)]
        if len(sub) == 0:
            return None
        return str(sub.iloc[0]["板块代码"]).strip()
    except Exception:
        return None


def _resolve_board_by_keyword(keyword: str) -> dict:
    """用东财板块列表把一个关键词解析为最合适的板块（行业/概念）及 BK 代码。

    返回：{keyword,resolved_name,symbol,board_type,debug_candidates}
    """
    kw = (keyword or "").strip()
    if not kw:
        return {"keyword": keyword, "resolved_name": None, "symbol": None, "board_type": None, "debug_candidates": []}

    provider = str(os.environ.get("FUND_BOARD_PROVIDER", "tushare")).strip().lower()
    if provider in ("tushare", "auto"):
        r = _resolve_board_by_keyword_tushare(keyword)
        if r.get("symbol"):
            return r

    # 取两张“权威名单”：行业/概念
    ind_df = _get_industry_board_list_df()
    con_df = _get_concept_board_list_df()

    candidates = []  # (score, resolved_name, symbol, board_type)

    def add_from_df(df, board_type: str):
        if df is None or len(df) == 0:
            return
        if "板块名称" not in df.columns or "板块代码" not in df.columns:
            return

        names = [str(x) for x in df["板块名称"].tolist()]
        codes = [str(x) for x in df["板块代码"].tolist()]

        # 1) 精确等于（最高优先）
        for n, c in zip(names, codes):
            if n == kw:
                candidates.append((1.00, n, c, board_type))
                return

        # 2) 包含匹配（次优先）
        for n, c in zip(names, codes):
            if kw in n or n in kw:
                candidates.append((0.80, n, c, board_type))

        # 3) 相似度匹配（兜底）
        close = difflib.get_close_matches(kw, names, n=5, cutoff=0.6)
        for n in close:
            try:
                idx = names.index(n)
                candidates.append((0.65, n, codes[idx], board_type))
            except Exception:
                pass

    add_from_df(ind_df, "industry")
    add_from_df(con_df, "concept")

    # 小型归一化：把“储存->存储”等交给关键词修正（不直接硬编码映射到 symbol）
    if not candidates and "储存" in kw:
        kw = kw.replace("储存", "存储")
        add_from_df(ind_df, "industry")
        add_from_df(con_df, "concept")

    if not candidates:
        return {"keyword": keyword, "resolved_name": None, "symbol": None, "board_type": None, "debug_candidates": []}

    # 选最高分；同分优先概念（因为你很多是概念板块），再按名字长度更接近
    candidates.sort(key=lambda x: (x[0], 1 if x[3] == "concept" else 0, -len(x[1])), reverse=True)
    best = candidates[0]

    debug_top = [
        {"score": float(s), "name": n, "symbol": sym, "board_type": bt}
        for (s, n, sym, bt) in candidates[:5]
    ]

    return {
        "keyword": keyword,
        "resolved_name": best[1],
        "symbol": best[2],
        "board_type": best[3],
        "debug_candidates": debug_top,
    }


def _pct(a: float, b: float) -> float:
    try:
        if b == 0:
            return 0.0
        return (a / b - 1.0) * 100.0
    except Exception:
        return 0.0


def _rsi(values, period: int = 14) -> float:
    try:
        if values is None or len(values) < period + 1:
            return 50.0
        gains = []
        losses = []
        for i in range(1, len(values)):
            diff = values[i] - values[i - 1]
            if diff >= 0:
                gains.append(diff)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-diff)
        gains = gains[-period:]
        losses = losses[-period:]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 70.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))
    except Exception:
        return 50.0




def _std(vals) -> float:
    try:
        n = len(vals)
        if n <= 1:
            return 0.0
        m = sum(vals) / n
        var = sum((x - m) ** 2 for x in vals) / (n - 1)
        return var ** 0.5
    except Exception:
        return 0.0


# ==== LLM 友好版压缩摘要 ====

def _compact_kline(kline: dict) -> dict:
    """把板块K线特征压缩成 LLM 友好版本，避免把 candles（20根K线）塞进上下文导致超长。"""
    if not isinstance(kline, dict):
        return {}
    if kline.get("error"):
        return {"error": kline.get("error"), "sector": kline.get("sector"), "symbol": kline.get("symbol")}

    keep = [
        "sector", "symbol", "board_type", "last_date", "close",
        "ret_1d", "ret_5d", "ret_20d",
        "ma5", "ma20", "ma60", "ma_cross",
        "rsi14", "volatility20", "range_pos_20d",
    ]
    return {k: kline.get(k) for k in keep if k in kline}


def _compact_fund_flow(ff: dict) -> dict:
    """把资金流压缩成 LLM 友好版本。"""
    if not isinstance(ff, dict):
        return {}
    if ff.get("error"):
        return {"error": ff.get("error"), "sector": ff.get("sector"), "symbol": ff.get("symbol")}

    keep = [
        "sector", "board_type", "symbol", "source",
        "last_date", "hist_last_date",
        "today_main_inflow", "sum_main_inflow_nd", "lookback_days",
        "today_pct",
    ]
    return {k: ff.get(k) for k in keep if k in ff}


# ==== 板块主力资金流 ====

def _safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s in ("", "nan", "None"):
            return default
        # 去掉逗号/百分号等
        s = s.replace(",", "").replace("%", "")

        # 处理中文单位：亿/万
        mult = 1.0
        if s.endswith("亿"):
            mult = 1e8
            s = s[:-1]
        elif s.endswith("万"):
            mult = 1e4
            s = s[:-1]

        return float(s) * mult
    except Exception:
        return default


def _pick_first_col(df, candidates):
    """从 df.columns 中挑一个最可能的列名。

    - candidates: 可包含多个关键词（子串匹配）
    - 兼容：列名前后空格、大小写差异、常见同义字段
    """
    if df is None or len(getattr(df, "columns", [])) == 0:
        return None

    raw_cols = [str(c) for c in df.columns]
    norm_cols = [c.strip().lower().replace(" ", "") for c in raw_cols]

    keys = []
    for k in candidates:
        if k is None:
            continue
        keys.append(str(k).strip().lower().replace(" ", ""))

    # 1) 精确匹配（归一化后）
    for k in keys:
        for rc, nc in zip(raw_cols, norm_cols):
            if nc == k:
                return rc

    # 2) 子串匹配
    for k in keys:
        for rc, nc in zip(raw_cols, norm_cols):
            if k and (k in nc):
                return rc

    return None


def get_sector_main_fund_flow(sector_name: str, board_type: str, symbol: str = None, lookback: int = 3) -> dict:
    """获取板块主力资金走向（尽量用 AkShare 的资金流历史接口）。

    返回：{
      sector, board_type, symbol,
      today_main_inflow, last_date,
      sum_main_inflow_nd,
      error?
    }

    说明：
    - 行业板块优先用 stock_sector_fund_flow_hist(symbol=BKxxxx)
    - 概念板块优先用 stock_concept_fund_flow_hist(symbol=BKxxxx)
    - 列名在不同版本可能不同，这里做了宽松匹配
    """
    sector_name = (sector_name or "").strip()
    board_type = (board_type or "").strip() or None
    symbol = (str(symbol).strip() if symbol else None)

    if ak is None:
        return {"sector": sector_name, "board_type": board_type, "symbol": symbol, "error": "akshare not available"}

    # 选择函数
    fn = None
    if board_type == "concept":
        fn = getattr(ak, "stock_concept_fund_flow_hist", None)
    else:
        fn = getattr(ak, "stock_sector_fund_flow_hist", None)
        board_type = board_type or "industry"

    if fn is None:
        return {"sector": sector_name, "board_type": board_type, "symbol": symbol, "error": "fund_flow api not available"}

    if not symbol and not sector_name:
        return {"sector": sector_name, "board_type": board_type, "symbol": symbol, "error": "missing board symbol"}

    # 拉取（做一次轻量重试）
    df = None
    last_err = None

    # NOTE:
    # - 行业资金流历史通常接受 BK 代码（如 BK1036）
    # - 概念资金流历史在很多 AkShare 版本里接受“概念名称”（如 存储芯片），
    #   你传 BK 会触发 KeyError（你现在看到的 'BK1137'）。
    # 所以：概念优先用 sector_name 查，失败再用 BK。
    query_keys = []
    if board_type == "concept":
        if sector_name:
            query_keys.append(sector_name)
        if symbol:
            query_keys.append(symbol)
    else:
        if symbol:
            query_keys.append(symbol)
        if sector_name:
            query_keys.append(sector_name)

    for q in query_keys:
        for _ in range(2):
            try:
                df = fn(symbol=q)
                last_err = None
                break
            except TypeError:
                try:
                    df = fn(q)
                    last_err = None
                    break
                except Exception as e2:
                    last_err = e2
                    time.sleep(0.4)
            except Exception as e:
                last_err = e
                time.sleep(0.4)
        if df is not None and len(df) > 0:
            break

    if df is None or len(df) == 0:
        return {"sector": sector_name, "board_type": board_type, "symbol": symbol, "error": str(last_err) if last_err else "empty fund flow"}

    # 尽量找“日期/主力净流入”列
    date_col = _pick_first_col(df, ["日期", "时间", "date"])
    main_col = _pick_first_col(df, ["主力净流入", "主力净额", "主力", "净流入"])

    if main_col is None:
        return {"sector": sector_name, "board_type": board_type, "symbol": symbol, "error": f"cannot find main flow column in {list(df.columns)}"}

    # 取最后 N 行
    df2 = df.tail(max(int(lookback), 1)).copy()
    vals = [_safe_float(v) for v in df2[main_col].tolist()]

    last_date = None
    if date_col is not None:
        try:
            last_date = str(df.iloc[-1][date_col])
        except Exception:
            last_date = None

    today = vals[-1] if vals else 0.0
    ssum = sum(vals[-int(lookback):]) if vals else 0.0

    return {
        "sector": sector_name,
        "board_type": board_type,
        "symbol": symbol,
        "last_date": last_date,
        "today_main_inflow": float(today),
        "sum_main_inflow_nd": float(ssum),
        "lookback_days": int(lookback),
        "_col": main_col,
    }


def _fmt_money_yi(x: float) -> str:
    """把金额转成“亿元”字符串（输入通常是元；如果本身不是元也不会报错，只是做尺度展示）。"""
    try:
        v = float(x)
        return f"{v/1e8:.2f}亿"
    except Exception:
        return "--"


# === 全市场板块主力资金榜（行业/概念） ===
def get_market_board_fund_flow_rank(board_type: str = "industry", top_n: int = 20) -> dict:
    """获取全市场板块主力资金榜（尽量覆盖所有板块）。

    - industry：优先用 ak.stock_sector_fund_flow_rank()
    - concept：优先用 ak.stock_fund_flow_concept() / ak.stock_fund_flow_concept（不同版本命名差异）

    返回：{board_type, items:[{name, symbol, main_inflow, pct}], error?}
    """
    provider = str(os.environ.get("FUND_BOARD_PROVIDER", "tushare")).strip().lower()

    # TuShare 优先（更稳定）
    if provider in ("tushare", "auto"):
        res = get_market_board_fund_flow_rank_tushare(board_type=(board_type or "industry").strip().lower(), top_n=top_n)
        if not res.get("error") and (res.get("items") is not None):
            return res

    # 回退到 AkShare
    if ak is None:
        return {"board_type": board_type, "items": [], "error": "akshare not available"}

    bt = (board_type or "industry").strip().lower()

    # 选 rank 接口
    fn = None
    if bt == "concept":
        fn = getattr(ak, "stock_fund_flow_concept", None)
        if fn is None:
            fn = getattr(ak, "stock_fund_flow_concept_em", None)
    else:
        fn = getattr(ak, "stock_sector_fund_flow_rank", None)
        if fn is None:
            fn = getattr(ak, "stock_sector_fund_flow_summary", None)

    if fn is None:
        return {"board_type": bt, "items": [], "error": "rank api not available"}

    import time
    def _call_with_retry(_fn, tries: int = 3, sleep_s: float = 1.0):
        last = None
        for i in range(max(1, int(tries))):
            try:
                return _fn(), None
            except Exception as e:
                last = e
                # akshare 偶发返回 None 导致内部 `.text` 报错，属于瞬时网络/限流问题，重试即可
                time.sleep(sleep_s * (i + 1))
        return None, last

    df, err = _call_with_retry(fn, tries=3, sleep_s=1.0)

    # 概念榜再兜底一次：如果首选函数失败，尝试切换到另一个命名（不同版本/源可能更稳定）
    if (df is None or len(df) == 0) and err is not None and bt == "concept":
        alt = None
        if fn.__name__ == "stock_fund_flow_concept":
            alt = getattr(ak, "stock_fund_flow_concept_em", None)
        else:
            alt = getattr(ak, "stock_fund_flow_concept", None)
        if alt is not None:
            df2, err2 = _call_with_retry(alt, tries=3, sleep_s=1.0)
            if df2 is not None and len(df2) > 0:
                df, err = df2, None
            else:
                err = err2 or err

    if err is not None:
        return {"board_type": bt, "items": [], "error": str(err)}

    if df is None or len(df) == 0:
        return {"board_type": bt, "items": [], "error": "empty rank"}

    # 兼容不同列名
    name_col = _pick_first_col(df, ["板块名称", "概念", "行业", "名称"])
    code_col = _pick_first_col(df, ["板块代码", "概念代码", "行业代码", "代码", "bk", "symbol"])    # 行业榜常见：主力净流入/主力净额；概念榜常见：净额/流入资金/流出资金
    main_col = _pick_first_col(df, ["主力净流入", "主力净额", "净额", "净流入", "流入资金", "流出资金", "主力"])
    # 概念榜常见列名：行业-涨跌幅
    pct_col = _pick_first_col(df, ["行业-涨跌幅", "涨跌幅", "涨跌", "pct", "%"])

    # 如果没有代码列，就用我们现有的解析器补一个 BK 代码
    items = []
    for _, r in df.iterrows():
        name = str(r.get(name_col)) if name_col else ""
        if not name or name == "nan":
            continue
        sym = str(r.get(code_col)).strip() if code_col else ""
        if not sym or sym == "nan":
            res = _resolve_board_by_keyword(name)
            sym = str(res.get("symbol") or "").strip()

        main = _safe_float(r.get(main_col)) if main_col else 0.0

        if bt == "concept" and main_col and any(k in str(main_col) for k in ["净额", "流入", "流出"]):
            # 概念榜很多版本用“亿”为单位（数值通常在 0~500 之间）。
            # 只有在量级明显像“亿”时才换算成“元”，避免误把已经是元的数据再放大。
            if abs(main) > 0 and abs(main) < 5_000:  # 0~5000 更像“亿”
                main = main * 1e8

        pct = None
        if pct_col:
            try:
                pct = float(str(r.get(pct_col)).replace("%", ""))
            except Exception:
                pct = None

        items.append({"name": name, "symbol": sym or None, "main_inflow": float(main), "pct": pct})

    # 按主力净流入排序
    items.sort(key=lambda x: x.get("main_inflow", 0.0), reverse=True)

    # top_n<=0 表示返回全量（用于后续按板块名/代码精确查找）
    try:
        n = int(top_n) if top_n is not None else 0
    except Exception:
        n = 0

    if n <= 0:
        out = items
    else:
        out = items[: max(5, n)]

    return {"board_type": bt, "items": out}


def print_market_board_fund_flow_board(top_n: int = 15):
    """打印全市场板块主力资金榜（行业 + 概念）。"""
    print("\n=== 全市场板块主力资金榜（用于判断资金风向，不绑定单只基金） ===")
    try:
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        now_ts = str(datetime.now())
    print(f"抓取时间：{now_ts}")

    for bt, title in (("industry", "行业板块"), ("concept", "概念板块")):
        res = get_market_board_fund_flow_rank(board_type=bt, top_n=top_n)
        if res.get("error"):
            err = str(res["error"])
            if "ProxyError" in err or "proxy" in err.lower():
                err += "（提示：检测到代理错误；已默认忽略环境代理。如你确实需要代理，设置环境变量 FUND_TRUST_ENV_PROXY=1）"
            print(f"[{title}] 获取失败：{err}")
            continue

        print(f"\n--- {title} Top {min(top_n, len(res.get('items') or []))} ---")
        for i, it in enumerate(res.get("items") or [], 1):
            nm = it.get("name")
            sym = it.get("symbol")
            inflow = _fmt_money_yi(it.get("main_inflow"))
            pct = it.get("pct")
            pct_s = f"{pct:.2f}%" if isinstance(pct, (int, float)) else "--"
            code_s = f"{sym}" if sym else "--"
            print(f"{i:>2}. {nm} ({code_s})  主力净流入 {inflow}  涨跌幅 {pct_s}")


def get_sector_kline_features(sector: str, days: int = 120, tail: int = 20, symbol: str = None, board_type: str = None) -> dict:
    """给模型用的板块K线摘要：趋势/动量/位置 + 最近N根K线。失败不抛异常。"""
    sector = (sector or "").strip()
    if not sector:
        return {"sector": sector, "error": "empty sector"}
    if ak is None:
        return {"sector": sector, "error": "akshare not available"}

    # 如果上层已经解析出 BK 代码，就不要在这里再做名称匹配
    if symbol:
        symbol = str(symbol).strip()
        board_type = (board_type or "").strip() or None
        if board_type == "concept":
            hist_fn = getattr(ak, "stock_board_concept_hist_em", None)
        else:
            hist_fn = getattr(ak, "stock_board_industry_hist_em", None)
            board_type = board_type or "industry"

        if hist_fn is None:
            return {"sector": sector, "symbol": symbol, "error": "kline api not available"}

        # 直接往下走，用 symbol 拉K线
        resolved_symbol = symbol
        resolved_board_type = board_type

    else:
        resolved_symbol = None
        resolved_board_type = None

    if resolved_symbol is None:
        # 旧逻辑：给一个名字自己解析（保留，作为兜底）
        # 先行业板块，再概念板块；同时用“原名 + 归一化名”两次尝试
        board_type = "industry"

        sector_norm = _normalize_sector_name(sector)
        candidates = []
        for x in (sector, sector_norm):
            x = (x or "").strip()
            if x and x not in candidates:
                candidates.append(x)

        symbol = None
        hist_fn = getattr(ak, "stock_board_industry_hist_em", None)

        # 1) 先尝试行业板块
        for name_try in candidates:
            symbol = _find_industry_board_symbol(name_try)
            if symbol:
                board_type = "industry"
                hist_fn = getattr(ak, "stock_board_industry_hist_em", None)
                break

        # 2) 再尝试概念板块
        if not symbol:
            for name_try in candidates:
                symbol = _find_concept_board_symbol(name_try)
                if symbol:
                    board_type = "concept"
                    hist_fn = getattr(ak, "stock_board_concept_hist_em", None)
                    break

        if not symbol or hist_fn is None:
            return {"sector": sector, "error": f"no board symbol (industry/concept) for {sector}"}

        resolved_symbol = symbol
        resolved_board_type = board_type

    else:
        # 上层传入 symbol 时，已选好 hist_fn/resolved_*，这里无需处理
        symbol = resolved_symbol
        board_type = resolved_board_type

    try:
        # akshare 的东财接口偶发返回空结构导致内部报 index 0 out of bounds，做一次轻量重试
        df = None
        last_err = None
        for _ in range(2):
            try:
                df = hist_fn(symbol=symbol)
                last_err = None
                break
            except Exception as _e:
                last_err = _e
                time.sleep(0.4)

        rows = []
        if df is None or len(df) == 0:
            # 兜底：直接从东财 push2his 拉 BK 板块K线
            rows = _fetch_board_kline_em_fallback(symbol=symbol, limit=max(120, int(days)))
        else:
            # 正常：从 AkShare DataFrame 转成 rows 统一处理
            try:
                df2 = df.tail(max(30, int(days))).copy()
                for _, r in df2.iterrows():
                    rows.append({
                        "日期": str(r.get("日期")),
                        "开盘": float(r.get("开盘")),
                        "收盘": float(r.get("收盘")),
                        "最高": float(r.get("最高")),
                        "最低": float(r.get("最低")),
                        "成交量": float(r.get("成交量", 0.0)),
                        "成交额": float(r.get("成交额", 0.0)),
                    })
            except Exception as _e2:
                last_err = _e2
                rows = _fetch_board_kline_em_fallback(symbol=symbol, limit=max(120, int(days)))

        if not rows:
            if last_err is not None:
                return {"sector": sector, "symbol": symbol, "error": str(last_err)}
            return {"sector": sector, "symbol": symbol, "error": "empty kline"}

        # 用 rows 继续算特征
        tail_rows = rows[-max(30, int(days)) :]
        closes = [float(x["收盘"]) for x in tail_rows]
        opens = [float(x["开盘"]) for x in tail_rows]
        highs = [float(x["最高"]) for x in tail_rows]
        lows = [float(x["最低"]) for x in tail_rows]
        dates = [str(x["日期"]) for x in tail_rows]

        last_close = closes[-1]
        last_date = dates[-1]

        ret_1d = _pct(closes[-1], closes[-2]) if len(closes) >= 2 else 0.0
        ret_5d = _pct(closes[-1], closes[-6]) if len(closes) >= 6 else 0.0
        ret_20d = _pct(closes[-1], closes[-21]) if len(closes) >= 21 else 0.0

        def ma(n):
            if len(closes) < n:
                return sum(closes) / len(closes)
            return sum(closes[-n:]) / n

        ma5 = ma(5)
        ma20 = ma(20)
        ma60 = ma(60)

        if ma5 > ma20 * 1.002:
            ma_cross = "bull"
        elif ma5 < ma20 * 0.998:
            ma_cross = "bear"
        else:
            ma_cross = "flat"

        rsi14 = float(_rsi(closes, 14))

        rets = []
        for i in range(1, len(closes)):
            if closes[i - 1] != 0:
                rets.append((closes[i] / closes[i - 1] - 1.0) * 100.0)
        vol20 = float(_std(rets[-20:])) if len(rets) >= 2 else 0.0

        lookback = min(20, len(closes))
        hi = max(highs[-lookback:])
        lo = min(lows[-lookback:])
        range_pos = 0.5
        if hi > lo:
            range_pos = (last_close - lo) / (hi - lo)

        t = min(int(tail), len(closes))
        candles = []
        for i in range(len(closes) - t, len(closes)):
            pct = 0.0
            if i > 0 and closes[i - 1] != 0:
                pct = (closes[i] / closes[i - 1] - 1.0) * 100.0
            candles.append(
                {
                    "date": dates[i],
                    "open": float(opens[i]),
                    "high": float(highs[i]),
                    "low": float(lows[i]),
                    "close": float(closes[i]),
                    "pct": float(pct),
                }
            )

        return {
            "sector": sector,
            "symbol": resolved_symbol,
            "board_type": resolved_board_type,
            "last_date": last_date,
            "close": float(last_close),
            "ret_1d": float(ret_1d),
            "ret_5d": float(ret_5d),
            "ret_20d": float(ret_20d),
            "ma5": float(ma5),
            "ma20": float(ma20),
            "ma60": float(ma60),
            "ma_cross": ma_cross,
            "rsi14": float(rsi14),
            "volatility20": float(vol20),
            "range_pos_20d": float(range_pos),
            "candles": candles,
        }

    except Exception as e:
        return {"sector": sector, "symbol": symbol, "error": str(e)}



def run_fund_daily():
    print("=== 今日基金量化建议（实时估值 + 动态网格 + 板块情绪 + AI 综合决策 + 明日候选） ===")

    # 1）先看一眼今天的新闻情绪（宏观+舆情）
    try:
        news_view = get_market_news_sentiment(limit=50)
        print("\n=== 今日财经新闻情绪综述（AI） ===")
        print(f"整体情绪：{news_view.get('market_sentiment')}（得分：{news_view.get('score')} / 100）")
        print(f"风险水平：{news_view.get('risk_level')}")
        print(f"热点主题：{', '.join(news_view.get('hot_themes') or []) or '无明显主题'}")
        print(f"热点板块：{', '.join(news_view.get('hot_sectors') or []) or '无明显板块'}")
        print(f"风格建议：{news_view.get('suggested_style')}")
        print(_format_market_bias(news_view))
        print("新闻点评：", news_view.get("comment"))
        print("==============================================")
    except Exception as e:
        print(f"\n[warn] 今日新闻情绪获取失败，将跳过新闻模块：{e}")
        news_view = None

    # 0）先把“全市场资金风向”打出来：你想要的是所有板块的主力资金走向
    try:
        print_market_board_fund_flow_board(top_n=15)

    except Exception as _e:
        print(f"[warn] 全市场板块资金获取失败（将回退 rank）：{_e}")

    # 0.1）拉取全市场板块资金 rank（全量），用于单只基金“今日净流入”优先走 rank
    rank_index = {
        "industry": {"by_symbol": {}, "by_name": {}},
        "concept": {"by_symbol": {}, "by_name": {}},
    }
    try:
        ind_rank = get_market_board_fund_flow_rank(board_type="industry", top_n=0)
        con_rank = get_market_board_fund_flow_rank(board_type="concept", top_n=0)

        for bt, res in ("industry", ind_rank), ("concept", con_rank):
            for it in (res.get("items") or []):
                nm = str(it.get("name") or "").strip()
                sym = str(it.get("symbol") or "").strip()
                if sym and sym != "nan":
                    rank_index[bt]["by_symbol"][sym] = it
                if nm and nm != "nan":
                    rank_index[bt]["by_name"][nm] = it
    except Exception as _e:
        print(f"[warn] rank 全量索引构建失败（将回退 hist）：{_e}")

    # 用于后面 AI 选基器的汇总列表
    all_funds = []
    all_funds_for_picker = []

    # 2）逐只基金：实时估值 + 网格信号 + 板块情绪 + AI 综合决策
    for code, cfg in WATCH_FUNDS.items():
        code_str = str(code)

        latest = get_latest_price(code_str)
        if latest is None:
            print(f"\n[warn] 无法获取 {code_str} 的价格数据，跳过。")
            continue

        price = latest["price"]
        time_ = latest["time"]
        pct = latest.get("pct")
        source = latest.get("source", "unknown")

        # 量化网格信号
        sig = generate_today_signal(code_str, price)

        # 1) 先从“接口板块列表”解析出最匹配的东财板块（行业/概念）与 BK 代码
        sector_keyword = cfg.get("sector") if isinstance(cfg, dict) else None
        if not sector_keyword:
            sector_keyword = get_sector_by_fund(code_str)

        sector_keyword_norm = _normalize_sector_name(sector_keyword)
        board_res = _resolve_board_by_keyword(sector_keyword_norm)
        resolved_sector_name = board_res.get("resolved_name") or sector_keyword_norm

        # 2) 板块情绪（先用解析后的权威板块名；你的 sector.py 里如果是占位也没关系）
        sector_info = get_sector_sentiment(resolved_sector_name)

        # 3) 用解析出的 BK 代码直接拉板块K线（避免再次按名字匹配）
        sector_kline = get_sector_kline_features_tushare(
            resolved_sector_name,
            days=120,
            tail=20,
            symbol=board_res.get("symbol"),
            board_type=board_res.get("board_type"),
        )
        if isinstance(sector_kline, dict) and sector_kline.get("error"):
            sector_kline = get_sector_kline_features(
                resolved_sector_name,
                days=120,
                tail=20,
                symbol=board_res.get("symbol"),
                board_type=board_res.get("board_type"),
            )

        # 额外把“解析板块结果”也给模型（让它知道用的是哪个板块/代码）
        try:
            sector_info["board"] = board_res
        except Exception:
            pass
        try:
            sector_info["kline"] = sector_kline
        except Exception:
            pass

        # 4) 主力资金走向：优先从全市场 rank 榜取“今日净流入”，取不到再降级用 hist
        bt = (board_res.get("board_type") or "industry").strip() or "industry"
        sym = (str(board_res.get("symbol") or "").strip() or None)
        nm = (str(board_res.get("resolved_name") or resolved_sector_name or "").strip() or None)

        fund_flow = None
        it = None
        try:
            if sym and sym in rank_index.get(bt, {}).get("by_symbol", {}):
                it = rank_index[bt]["by_symbol"][sym]
            elif nm and nm in rank_index.get(bt, {}).get("by_name", {}):
                it = rank_index[bt]["by_name"][nm]
        except Exception:
            it = None

        if it is not None:
            # rank 命中：今日净流入以 rank 为准
            fund_flow = {
                "sector": nm or resolved_sector_name,
                "board_type": bt,
                "symbol": sym or (it.get("symbol") if isinstance(it, dict) else None),
                "last_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "today_main_inflow": float((it.get("main_inflow") if isinstance(it, dict) else 0.0) or 0.0),
                "today_pct": (it.get("pct") if isinstance(it, dict) else None),
                "source": "rank",
            }

            # 仍尝试用 hist 补齐近 N 日合计（hist 可能 T-1，这里只补合计，不强制一致）
            ff_hist = get_sector_main_fund_flow_tushare(
                sector_name=resolved_sector_name,
                board_type=bt,
                symbol=sym,
                lookback=3,
            )
            if isinstance(ff_hist, dict) and ff_hist.get("error"):
                ff_hist = get_sector_main_fund_flow(
                    sector_name=resolved_sector_name,
                    board_type=bt,
                    symbol=sym,
                    lookback=3,
                )
            if isinstance(ff_hist, dict) and not ff_hist.get("error"):
                fund_flow["sum_main_inflow_nd"] = float(ff_hist.get("sum_main_inflow_nd") or 0.0)
                fund_flow["lookback_days"] = int(ff_hist.get("lookback_days") or 3)
                fund_flow["hist_last_date"] = ff_hist.get("last_date")
        else:
            # rank 没命中：回退 hist
            fund_flow = get_sector_main_fund_flow_tushare(
                sector_name=resolved_sector_name,
                board_type=bt,
                symbol=sym,
                lookback=3,
            )
            if isinstance(fund_flow, dict) and fund_flow.get("error"):
                fund_flow = get_sector_main_fund_flow(
                    sector_name=resolved_sector_name,
                    board_type=bt,
                    symbol=sym,
                    lookback=3,
                )
                if isinstance(fund_flow, dict) and not fund_flow.get("error"):
                    fund_flow["source"] = "hist"

        try:
            sector_info["fund_flow"] = fund_flow
        except Exception:
            pass

        # 基金自身风险配置（从 WATCH_FUNDS 里拿）
        fund_profile = None
        if isinstance(cfg, dict):
            fund_profile = {
                "risk": cfg.get("risk", "unknown"),
                "max_position_pct": cfg.get("max_position_pct"),
            }

        # AI 综合决策（基于量化 + 板块情绪 +（可选）板块K线 + 风险配置）
        ai_decision = ask_deepseek_fund_decision(
            fund_name=cfg.get("name", code_str) if isinstance(cfg, dict) else code_str,
            code=code_str,
            latest=latest,
            quant_signal=sig,
            sector_info=sector_info,
            fund_profile=fund_profile,
        )

        # —— 单只基金的明细输出 —— #
        print("\n----------------------------------------")
        name = cfg.get("name", code_str) if isinstance(cfg, dict) else code_str
        print(f"{name} ({code_str})")

        # 数据来源说明
        if source == "realtime":
            print(f"数据来源：实时估值（估算净值） @ {time_}")
        else:
            try:
                date_str = time_.date()
            except Exception:
                date_str = str(time_)
            print(f"数据来源：历史净值（最近结算日） @ {date_str}")

        # 今日价格 + 涨跌
        if pct is not None:
            print(f"当前参考价格：{price:.4f}（今日涨跌：{pct:.2f}%）")
        else:
            print(f"当前参考价格：{price:.4f}（无当日涨跌数据）")

        # 网格信息
        print(f"参考中枢价：{sig['base_price']}")
        print(f"动态网格价：{sig['grids']}")
        print(f"量化模型建议：{sig['action']}")
        print(f"量化模型理由：{sig['reason']}")

        # 板块情绪
        if (
            sector_info.get("score") == 50
            and sector_info.get("level") in ("中性", "neutral", None)
            and "多空力量相对均衡" in str(sector_info.get("comment", ""))
        ):
            print("[hint] 板块情绪目前看起来是占位值（固定中性/50），与新闻偏多并不矛盾，只是该模块未接入真实板块情绪。")

        if board_res.get("symbol"):
            print(f"[board] 关键词={sector_keyword} -> {board_res.get('resolved_name')} ({board_res.get('board_type')}:{board_res.get('symbol')})")
        print(f"所属板块：{sector_info['sector']}")
        print(f"板块情绪：{sector_info['level']}（得分：{sector_info['score']}）")
        print(f"板块点评：{sector_info['comment']}")

        # 主力资金走向（板块）
        ff = sector_info.get("fund_flow") if isinstance(sector_info, dict) else None
        if isinstance(ff, dict) and not ff.get("error"):
            src = ff.get("source") or "unknown"
            d1 = ff.get("last_date") or "date?"
            d2 = ff.get("hist_last_date")
            tail_note = ""
            if src == "rank" and d2 and str(d2) != str(d1):
                tail_note = f"，hist截止 {d2}"

            if ff.get("sum_main_inflow_nd") is not None:
                print(
                    f"主力资金({src})：今日净流入 {_fmt_money_yi(ff.get('today_main_inflow'))}，"
                    f"近{ff.get('lookback_days', 3)}日合计 {_fmt_money_yi(ff.get('sum_main_inflow_nd'))}（{d1}{tail_note}）"
                )
            else:
                print(
                    f"主力资金({src})：今日净流入 {_fmt_money_yi(ff.get('today_main_inflow'))}（{d1}{tail_note}）"
                )
        elif isinstance(ff, dict) and ff.get("error"):
            print(f"[fundflow] {resolved_sector_name} 获取失败：{ff.get('error')}")

        # 板块K线摘要打印（便于你肉眼确认）
        if isinstance(sector_kline, dict) and not sector_kline.get("error"):
            print(
                f"板块K线（{sector_kline.get('board_type')}）：{sector_kline.get('symbol')} 近1/5/20日 "
                f"{sector_kline.get('ret_1d'):.2f}%/{sector_kline.get('ret_5d'):.2f}%/{sector_kline.get('ret_20d'):.2f}% "
                f"MA5/20 {sector_kline.get('ma5'):.0f}/{sector_kline.get('ma20'):.0f} RSI14 {sector_kline.get('rsi14'):.1f}"
            )
        else:
            # 失败也打印原因，避免你看不到
            if isinstance(sector_kline, dict):
                err = sector_kline.get("error")
                if err:
                    print(f"[kline] {resolved_sector_name} 拉取失败：{err}")

        print(f"AI 综合建议：{ai_decision['action']}")
        print(f"AI 理由：{ai_decision['reason']}")

        # —— 汇总信息 ——
        clean_latest = latest.copy()
        t_val = clean_latest.get("time")
        if isinstance(t_val, (datetime, date)):
            clean_latest["time"] = t_val.isoformat()

        # 用于打印/调试的“全量”摘要（保留）
        summary_full = {
            "code": code_str,
            "name": name,
            "sector": resolved_sector_name,
            "latest": clean_latest,
            "quant": {
                "action": sig.get("action"),
                "reason": sig.get("reason"),
                "base_price": sig.get("base_price"),
                "grids": sig.get("grids"),
            },
            "sector_view": {
                "score": sector_info.get("score"),
                "level": sector_info.get("level"),
                "comment": sector_info.get("comment"),
                "kline": sector_kline,
                "fund_flow": (sector_info.get("fund_flow") if isinstance(sector_info, dict) else None),
            },
            "ai_decision": {
                "action": ai_decision.get("action"),
                "reason": ai_decision.get("reason"),
            },
            "fund_profile": fund_profile or {},
        }

        # 送进 ai_picker 的“紧凑”摘要：删掉 candles/长文本，避免 prompt 超长
        summary_compact = {
            "code": code_str,
            "name": name,
            "sector": resolved_sector_name,
            "latest": {
                "price": clean_latest.get("price"),
                "pct": clean_latest.get("pct"),
                "time": clean_latest.get("time"),
                "source": clean_latest.get("source"),
            },
            "quant": {
                "action": sig.get("action"),
                "base_price": sig.get("base_price"),
                "grids": sig.get("grids"),
            },
            "sector_view": {
                "score": sector_info.get("score"),
                "level": sector_info.get("level"),
                "kline": _compact_kline(sector_kline if isinstance(sector_kline, dict) else {}),
                "fund_flow": _compact_fund_flow(sector_info.get("fund_flow") if isinstance(sector_info, dict) else {}),
            },
            "ai_decision": {
                "action": ai_decision.get("action"),
            },
            "fund_profile": {
                "risk": (fund_profile or {}).get("risk"),
                "max_position_pct": (fund_profile or {}).get("max_position_pct"),
            },
        }

        all_funds.append(summary_full)
        all_funds_for_picker.append(summary_compact)

    # 3）所有基金跑完之后，做“明日候选”AI 选基
    if all_funds:
        picker_res = pick_funds_for_tomorrow(
            news_view or {},
            all_funds_for_picker,
            use_llm_first=True,
            top_k_focus=2,
        )

        print("\n==============================================")
        print("\n==============================================")
        print("=== 自选池明日候选（只在你当前基金池里排序） ===")

        # 4）全市场：AI 挑板块 + ETF（不局限于你的自选池）
        try:
            from ai_picker import pick_market_funds_for_tomorrow

            market_res = pick_market_funds_for_tomorrow(
                news_view or {},
                use_llm_first=True,
                top_k_focus=10,
            )

            print("\n==============================================")
            print("=== 全市场 AI 选基（行业资金流 + ETF 实时行情） ===")

            m_focus = market_res.get("tomorrow_focus") or []
            m_style = market_res.get("style") or {}
            m_comment = market_res.get("comment") or ""
            m_ranking = market_res.get("ranking") or []

            if m_focus:
                print("明日可重点留意的基金代码：", ", ".join(m_focus))
            else:
                print("明日暂无明确全市场候选，整体建议观望或仅少量操作。")

            print(f"整体风格建议：{m_style.get('suggested_style') or '无'}")
            print("说明：新闻偏多=宏观/舆情风险偏好上升；但具体买卖仍以价格位置/网格/资金流为准，所以常出现‘偏多但保守’的组合，并不矛盾。")
            if m_style.get("risk_note"):
                print(f"风险提示：{m_style['risk_note']}")
            print("综合点评：", m_comment)

            print("\n—— 全市场综合评分排名 ——")
            for r in m_ranking:
                code = r.get("code", "")
                name = r.get("name", "")
                score = r.get("score", 0)
                suggest = _normalize_llm_suggest(r.get("suggest", ""))
                why = r.get("why", "")
                print(f"{code} {name}  |  综合分 {score}  |  建议：{suggest}")
                print("理由：", why)
                print("-" * 60)

        except Exception as e:
            print(f"\n[warn] 全市场 AI 选基失败：{e}")

        focus = picker_res.get("tomorrow_focus") or []
        style = picker_res.get("style") or {}
        comment = picker_res.get("comment") or ""


        if focus:
            print("优先关注基金代码：", ", ".join(focus))
        else:
            print("优先关注基金代码：无明确候选（建议观望或仅按网格小仓操作）")

        print(f"整体风格建议：{style.get('suggested_style') or '无'}")
        print("综合点评：", comment)

        print("\n—— 综合评分排名 ——")
        ranking = picker_res.get("ranking") or []
        for r in ranking:
            code = r.get("code", "")
            name = r.get("name", "")
            score = r.get("score", 0)
            suggest = _normalize_llm_suggest(r.get("suggest", ""))
            why = r.get("why", "")
            print(f"{code} {name}  |  综合分 {score}  |  建议：{suggest}")
            print("理由：", why)
            print("-" * 60)

    else:
        print("\n[warn] 今天没有成功处理任何基金，无法给出明日候选。")



if __name__ == "__main__":
    run_fund_daily()