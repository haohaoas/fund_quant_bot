# news_sentiment.py
"""新闻情绪模块：
方案1：新增“美股/全球风险偏好硬指标（SPX/纳指/VIX/DXY/美债10Y/可选期货）+ 美股新闻（软信号）”
只做加权：新闻仅用于加权，不能主导结论，最终以全球硬指标为主。

1) 从新浪滚动新闻（JSON）、东方财富策略研报 RSS（RSSHub）、或美股/全球新闻（Yahoo/Investing RSS）拉资讯（可切换/自动降级）
2) 用 DeepSeek 做情绪与热点分析
3) 输出结构化的市场情绪结论，给基金 Agent 当“今日新闻天气预报”

环境变量：
- NEWS_SOURCE: auto / sina / eastmoney（默认 auto：自动切换/优先级）
- SINA_NEWS_API: 覆盖新浪滚动新闻 JSON 接口
- EASTMONEY_RSS_URL: 覆盖 RSSHub 东财策略研报 RSS 地址
- DEEPSEEK_API_KEY: DeepSeek key（必填）
- DEEPSEEK_API_BASE: DeepSeek base_url（可选）
- AK_NEWS_SYMBOL: AkShare 个股资讯的 symbol（可选；留空表示取“全市场/宏观新闻”源）
"""

import os
import time
import json
import re
import inspect
from typing import List, Dict, Any

import requests
from openai import OpenAI

try:
    import akshare as ak
except Exception:
    ak = None


# ======== 一、基础配置 ========

# 新浪财经滚动新闻接口（JSON），可用环境变量覆盖
SINA_NEWS_API = os.getenv("SINA_NEWS_API", "https://feed.mix.sina.com.cn/api/roll/get")

# 东方财富策略研报 RSS（RSSHub 代理），返回 XML/RSS
EASTMONEY_RSS_URL = os.getenv(
    "EASTMONEY_RSS_URL",
    "https://rsshub.rssforever.com/eastmoney/report/strategyreport",
)

# 新闻源选择：auto / sina / eastmoney（默认 auto：自动切换/优先级）
NEWS_SOURCE = os.getenv("NEWS_SOURCE", "auto").lower()

# AkShare 资讯抓取时，若接口需要 symbol，用这个（可留空，默认全市场/宏观新闻模式）
AK_NEWS_SYMBOL = os.getenv("AK_NEWS_SYMBOL", "").strip()

# 记录本次实际用到的新闻源（sina / rsshub / akshare）
LAST_NEWS_SOURCE = "unknown"

# 默认拉多少条新闻
DEFAULT_NEWS_LIMIT = 40

# ======== 1.1 方案1：美股新闻 + 全球风险偏好硬指标 ========
# NEWS_REGION: cn / us / auto（auto: 交易日前早盘优先 us，其他时间 cn）
NEWS_REGION = os.getenv("NEWS_REGION", "cn").lower()

# US_NEWS_SOURCE: yahoo / rss / auto（默认 auto：先 yahoo 再 rss）
US_NEWS_SOURCE = os.getenv("US_NEWS_SOURCE", "auto").lower()

# Yahoo Finance 市场新闻 RSS（可用环境变量覆盖）
YAHOO_MARKET_RSS_URL = os.getenv(
    "YAHOO_MARKET_RSS_URL",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US",
)

# 备用美股新闻 RSS（可选）
US_NEWS_RSS_URL = os.getenv(
    "US_NEWS_RSS_URL",
    "https://www.investing.com/rss/news_25.rss",
)

# GLOBAL_SIGNAL_SOURCE: akshare / stooq / auto（目前以 akshare 动态探测为主）
GLOBAL_SIGNAL_SOURCE = os.getenv("GLOBAL_SIGNAL_SOURCE", "auto").lower()


# ======== 二、拉取财经新闻 ========

def fetch_finance_news_sina(limit: int = DEFAULT_NEWS_LIMIT, page: int = 1) -> List[Dict[str, Any]]:
    """从新浪财经滚动新闻（JSON）拉取最近的财经新闻列表（标题 + 链接 + 时间等）。"""
    global LAST_NEWS_SOURCE

    num = min(max(limit, 1), 50)

    params = {
        "pageid": 153,  # 财经频道 pageid
        "lid": 2516,  # 国内财经滚动
        "k": "",
        "num": num,
        "page": page,
        "r": f"{time.time():.16f}",
    }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
    }

    try:
        resp = requests.get(SINA_NEWS_API, params=params, headers=headers, timeout=8)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[news] 拉取新浪财经新闻失败：{e}")
        return []

    items = data.get("result", {}).get("data", []) or []
    news_list: List[Dict[str, Any]] = []

    for item in items:
        title = item.get("title") or ""
        url = item.get("url") or item.get("wapurl") or ""
        ctime = item.get("ctime") or item.get("intime")  # 有的字段叫 ctime，有的叫 intime
        media_name = item.get("media_name") or item.get("media") or "新浪财经"
        summary = item.get("intro") or item.get("summary") or ""

        if not title:
            continue

        news_list.append(
            {
                "title": title,
                "url": url,
                "ctime": ctime,
                "media_name": media_name,
                "summary": summary,
            }
        )

    LAST_NEWS_SOURCE = "sina"
    return news_list


def _clean_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _news_key(n: Dict[str, Any]) -> str:
    """用于去重：优先 url，其次 title。"""
    u = str(n.get("url") or "").strip()
    t = str(n.get("title") or "").strip()
    return u or t


def _merge_unique_news(base: List[Dict[str, Any]], extra: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """按顺序合并并去重，最多保留 limit 条。"""
    out = list(base or [])
    seen = set(_news_key(x) for x in out if _news_key(x))
    for n in (extra or []):
        k = _news_key(n)
        if not k:
            continue
        if k in seen:
            continue
        out.append(n)
        seen.add(k)
        if len(out) >= limit:
            break
    return out[:limit]



def _fetch_rss_generic(rss_url: str, limit: int, media_name: str) -> List[Dict[str, Any]]:
    """通用 RSS 拉取器（XML/RSS），返回统一字段。"""
    num = min(max(int(limit), 1), 50)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        resp = requests.get(rss_url, headers=headers, timeout=12)
        resp.raise_for_status()
        xml_text = resp.text
    except Exception as e:
        print(f"[news] 拉取 RSS 失败：{media_name} / {e}")
        return []

    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_text)
    except Exception as e:
        print(f"[news] 解析 RSS 失败：{media_name} / {e}")
        return []

    channel = root.find("channel") if str(root.tag).lower() == "rss" else root.find("./channel")
    if channel is None:
        channel = root

    news_list: List[Dict[str, Any]] = []
    for it in channel.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        url = (it.findtext("link") or "").strip()
        ctime = (it.findtext("pubDate") or it.findtext("published") or "").strip()
        summary = _clean_html((it.findtext("description") or "").strip())
        if not title:
            continue
        news_list.append(
            {
                "title": title,
                "url": url,
                "ctime": ctime,
                "media_name": media_name,
                "summary": summary,
            }
        )
        if len(news_list) >= num:
            break
    return news_list


def fetch_finance_news_eastmoney_rss(limit: int = DEFAULT_NEWS_LIMIT) -> List[Dict[str, Any]]:
    """从东方财富策略研报 RSSHub 拉取资讯（XML/RSS），返回统一字段。"""
    global LAST_NEWS_SOURCE
    news_list = _fetch_rss_generic(EASTMONEY_RSS_URL, limit, "东方财富")
    if news_list:
        LAST_NEWS_SOURCE = "rsshub"
    return news_list


def fetch_us_market_news(limit: int = DEFAULT_NEWS_LIMIT) -> List[Dict[str, Any]]:
    """美股/海外市场新闻（软信号）。默认优先 Yahoo Finance RSS，其次备用 RSS。"""
    global LAST_NEWS_SOURCE

    limit = min(max(int(limit), 1), 80)

    def _try_yahoo() -> List[Dict[str, Any]]:
        return _fetch_rss_generic(YAHOO_MARKET_RSS_URL, limit=limit, media_name="YahooFinance")

    def _try_backup() -> List[Dict[str, Any]]:
        return _fetch_rss_generic(US_NEWS_RSS_URL, limit=limit, media_name="US_RSS")

    src = (US_NEWS_SOURCE or "auto").lower()
    news: List[Dict[str, Any]] = []

    if src == "yahoo":
        news = _try_yahoo()
        if news:
            LAST_NEWS_SOURCE = "us:yahoo"
        return news

    if src == "rss":
        news = _try_backup()
        if news:
            LAST_NEWS_SOURCE = "us:rss"
        return news

    # auto
    news = _try_yahoo()
    if news:
        LAST_NEWS_SOURCE = "us:yahoo"
        return news
    news = _try_backup()
    if news:
        LAST_NEWS_SOURCE = "us:rss"
    return news
def _choose_news_region() -> str:
    """auto: 早盘（默认 00:00-13:30）偏向读取美股隔夜风向，其它时段用 A 股新闻。"""
    r = (NEWS_REGION or "auto").lower()
    if r in ("cn", "us"):
        return r
    # auto
    try:
        lt = time.localtime()
        hhmm = lt.tm_hour * 100 + lt.tm_min
        # 中国/台北时区：上午/中午更依赖隔夜美股影响
        return "us" if hhmm <= 1330 else "cn"
    except Exception:
        return "cn"


def _df_to_news_list(df, limit: int) -> List[Dict[str, Any]]:
    """把 AkShare 返回的 DataFrame 尽量统一成 news_list 字段。"""
    if df is None:
        return []
    try:
        cols = [str(c) for c in df.columns]
    except Exception:
        return []

    def pick_col(candidates):
        for kw in candidates:
            for c in cols:
                if kw in c:
                    return c
        return None

    title_col = pick_col(["标题", "title", "新闻标题"])
    url_col = pick_col(["链接", "url", "新闻链接"])
    time_col = pick_col(["时间", "发布时间", "日期", "pub", "ctime"])
    summary_col = pick_col(["摘要", "内容", "简介", "summary"])

    out: List[Dict[str, Any]] = []
    for _, row in df.head(min(max(limit, 1), 50)).iterrows():
        title = str(row.get(title_col, "") if title_col else "").strip()
        if not title:
            continue
        out.append(
            {
                "title": title,
                "url": str(row.get(url_col, "") if url_col else "").strip(),
                "ctime": str(row.get(time_col, "") if time_col else "").strip(),
                "media_name": "东方财富(AkShare)",
                "summary": str(row.get(summary_col, "") if summary_col else "").strip(),
            }
        )
    return out


def fetch_finance_news_akshare(limit: int = DEFAULT_NEWS_LIMIT) -> List[Dict[str, Any]]:
    """优先用 AkShare 获取东方财富资讯（如果你环境里装了 akshare）。

    由于 AkShare 不同版本函数名/参数可能不同，这里做“动态探测 + 最小参数调用”。
    """
    global LAST_NEWS_SOURCE

    if ak is None:
        return []

    # 先全市场/宏观类新闻（不需要 symbol），再尝试东方财富个股资讯（需要 symbol）
    candidates = [
        "news_cctv",              # 全市场/宏观新闻（不需要 symbol）
        "stock_news_cctv",        # 部分版本存在
        "news_sina",              # 若存在：全市场新闻
        "stock_news_em",          # 东方财富个股资讯（通常需要 symbol）
        "stock_news_em_async",    # 有些版本带 async
    ]

    for fn_name in candidates:
        fn = getattr(ak, fn_name, None)
        if fn is None:
            continue

        try:
            sig = inspect.signature(fn)
            kwargs = {}
            # 如果接口需要 symbol，但你没配置，就跳过（你要看“全市场”时不强制指定 000001）
            if "symbol" in sig.parameters:
                if not AK_NEWS_SYMBOL:
                    continue
                kwargs["symbol"] = AK_NEWS_SYMBOL
            # 常见的数量参数名
            for k in ("num", "limit", "size"):
                if k in sig.parameters:
                    kwargs[k] = min(max(int(limit), 1), 50)
                    break

            df = fn(**kwargs)
            news_list = _df_to_news_list(df, limit=limit)
            if news_list:
                LAST_NEWS_SOURCE = "akshare"
                return news_list
        except Exception:
            continue

    return []


def fetch_finance_news(limit: int = DEFAULT_NEWS_LIMIT, page: int = 1) -> List[Dict[str, Any]]:
    global LAST_NEWS_SOURCE

    # 统一限制：最多 50（单源限制），但我们会通过“多源补齐/新浪翻页”把总量尽量补到 limit
    limit = min(max(int(limit), 1), 80)  # 允许补齐到 80（避免过长prompt）

    region = _choose_news_region()
    if region == "us":
        news = fetch_us_market_news(limit)
        if news:
            return news
        # fallback to cn
        region = "cn"
    if region == "cn":
        src = (NEWS_SOURCE or "auto").lower()
        def _topup_with_sina(cur: List[Dict[str, Any]], need: int) -> List[Dict[str, Any]]:
            """新浪滚动支持翻页，每页最多 50，按需补齐。"""
            if need <= 0:
                return cur
            page_i = 1
            out = list(cur or [])
            while len(out) < limit and page_i <= 3:  # 最多翻 3 页，避免太慢
                more = fetch_finance_news_sina(limit=min(50, limit - len(out)), page=page_i)
                out = _merge_unique_news(out, more, limit)
                if len(out) >= limit:
                    break
                page_i += 1
            return out

        # ------- 固定源：akshare -------
        if src == "akshare":
            news = fetch_finance_news_akshare(limit=limit)
            if len(news) >= limit:
                return news
            # akshare 不足则补齐新浪
            LAST_NEWS_SOURCE = "akshare+sina"
            news = _topup_with_sina(news, need=limit - len(news))
            return news

        # ------- 固定源：eastmoney（优先 akshare，再 rsshub，再 sina 补齐） -------
        if src == "eastmoney":
            news = fetch_finance_news_akshare(limit=limit)
            if len(news) < limit:
                more = fetch_finance_news_eastmoney_rss(limit=limit)
                news = _merge_unique_news(news, more, limit)
            if len(news) < limit:
                LAST_NEWS_SOURCE = "eastmoney+topup_sina"
                news = _topup_with_sina(news, need=limit - len(news))
            return news

        # ------- 固定源：sina -------
        if src == "sina":
            LAST_NEWS_SOURCE = "sina"
            news = fetch_finance_news_sina(limit=min(50, limit), page=1)
            if len(news) < limit:
                news = _topup_with_sina(news, need=limit - len(news))
            return news

        # ------- auto：先 akshare（东财），再 rsshub，再新浪补齐 -------
        news = fetch_finance_news_akshare(limit=limit)
        if len(news) < limit:
            more = fetch_finance_news_eastmoney_rss(limit=limit)
            news = _merge_unique_news(news, more, limit)
        if len(news) < limit:
            LAST_NEWS_SOURCE = "auto+topup_sina"
            news = _topup_with_sina(news, need=limit - len(news))
        return news
    return []


# ======== 2.1 方案1：全球风险偏好硬指标（强信号，优先级高于新闻） ========

def _try_call_ak(fn_name: str, *args, **kwargs):
    if ak is None:
        return None
    fn = getattr(ak, fn_name, None)
    if fn is None:
        return None
    try:
        return fn(*args, **kwargs)
    except Exception:
        return None


def _latest_pct_from_df(df) -> Dict[str, Any]:
    """从常见 OHLC/收盘 DataFrame 提取最近一条与涨跌幅。"""
    if df is None:
        return {}
    try:
        if len(df) < 2:
            return {}
        cols = [str(c) for c in df.columns]
    except Exception:
        return {}

    def pick_col(keys):
        for k in keys:
            for c in cols:
                if k in c:
                    return c
        return None

    date_col = pick_col(["日期", "date", "时间", "Date", "TIME"])
    close_col = pick_col(["收盘", "close", "收市", "Close"])
    if close_col is None:
        # 有些接口用 "最新价" 或 "当前价"
        close_col = pick_col(["最新", "当前", "price"])

    try:
        tail = df.tail(2)
        last = tail.iloc[-1]
        prev = tail.iloc[-2]
        close_last = float(last.get(close_col)) if close_col else None
        close_prev = float(prev.get(close_col)) if close_col else None
        if close_last is None or close_prev is None or close_prev == 0:
            return {}
        pct = (close_last / close_prev - 1.0) * 100.0
        return {
            "date": str(last.get(date_col, "")) if date_col else "",
            "close": close_last,
            "pct": pct,
        }
    except Exception:
        return {}


def fetch_global_risk_indicators() -> Dict[str, Any]:
    """尽量用 AkShare 动态探测拉取：标普/纳指/VIX/DXY/美债10Y。

    说明：不同 AkShare 版本函数名差异很大，所以这里采用“多候选函数 + 动态签名探测”。
    拉不到就留空，最终会降级为中性。
    """
    out: Dict[str, Any] = {
        "as_of": time.strftime("%Y-%m-%d %H:%M:%S"),
        "items": {},
        "missing": [],
    }

    if ak is None:
        out["missing"] = ["akshare_not_installed"]
        return out

    # 1) 优先尝试 Investing 类接口（最通用）
    investing_fns = [
        "index_investing_global",
        "bond_investing_global",
        "currency_investing",
    ]

    # 常见中文/英文名称（AkShare 不同版本可能用不同名字）
    targets = {
        "spx": ["标普500", "S&P 500", "S&P500", "SPX"],
        "ixic": ["纳斯达克综合指数", "纳指", "NASDAQ Composite", "IXIC"],
        "vix": ["VIX恐慌指数", "VIX", "恐慌指数"],
        "dxy": ["美元指数", "DXY", "Dollar Index"],
        "us10y": ["美国10年期国债收益率", "美国10年期国债", "US 10Y", "US10Y"],
    }

    def _try_investing(symbol_name: str):
        for fn_name in investing_fns:
            fn = getattr(ak, fn_name, None)
            if fn is None:
                continue
            try:
                sig = inspect.signature(fn)
                kwargs = {}
                # 尽量适配不同签名
                if "symbol" in sig.parameters:
                    kwargs["symbol"] = symbol_name
                elif "index_name" in sig.parameters:
                    kwargs["index_name"] = symbol_name
                elif "name" in sig.parameters:
                    kwargs["name"] = symbol_name
                else:
                    # 不支持就跳
                    continue
                df = fn(**kwargs)
                return df
            except Exception:
                continue
        return None

    for key, names in targets.items():
        df = None
        for nm in names:
            df = _try_investing(nm)
            if df is not None:
                break
        info = _latest_pct_from_df(df)
        if info:
            out["items"][key] = info
        else:
            out["missing"].append(key)

    return out


def compute_global_risk_score(global_data: Dict[str, Any]) -> Dict[str, Any]:
    """把硬指标合成为 0-100 的 global_risk_score。

    解释：
    - 股票指数（spx/ixic）上涨 => risk-on
    - VIX 上涨 => risk-off
    - DXY 上涨 => risk-off
    - 美债10Y 大幅上行 => 对成长偏压制（轻微 risk-off）
    """
    items = (global_data or {}).get("items") or {}

    def get_pct(k):
        try:
            return float(items.get(k, {}).get("pct"))
        except Exception:
            return None

    spx = get_pct("spx")
    ixic = get_pct("ixic")
    vix = get_pct("vix")
    dxy = get_pct("dxy")
    us10y = get_pct("us10y")

    # 如果啥都没有，直接中性
    if all(x is None for x in (spx, ixic, vix, dxy, us10y)):
        return {
            "global_risk_score": 50,
            "global_risk_sentiment": "neutral",
            "global_risk_level": "medium",
            "explain": "全球硬指标缺失，按中性处理。",
        }

    def clip(x, lo, hi):
        return max(lo, min(hi, x))

    eq = 0.0
    eq_n = 0
    for x in (spx, ixic):
        if x is None:
            continue
        eq += clip(x, -2.0, 2.0)
        eq_n += 1
    eq = (eq / eq_n) if eq_n else 0.0

    # VIX/DXY/us10y：上涨偏 risk-off
    vix_adj = -clip(vix, -6.0, 6.0) if vix is not None else 0.0
    dxy_adj = -clip(dxy, -1.5, 1.5) if dxy is not None else 0.0
    y_adj = -clip(us10y, -1.5, 1.5) * 0.6 if us10y is not None else 0.0

    # 权重合成（可调）
    raw = 0.55 * eq + 0.20 * vix_adj + 0.15 * dxy_adj + 0.10 * y_adj
    # 映射到 0-100
    score = int(round(50 + raw * 10))
    score = int(clip(score, 0, 100))

    if score >= 62:
        sentiment = "bullish"
    elif score <= 38:
        sentiment = "bearish"
    else:
        sentiment = "neutral"

    # 风险等级：主要看 VIX 变动幅度
    risk_level = "low"
    if vix is not None:
        if vix >= 5.0:
            risk_level = "high"
        elif vix >= 2.0:
            risk_level = "medium"
        else:
            risk_level = "low"
    else:
        risk_level = "medium"

    explain_parts = []
    if spx is not None:
        explain_parts.append(f"SPX {spx:+.2f}%")
    if ixic is not None:
        explain_parts.append(f"IXIC {ixic:+.2f}%")
    if vix is not None:
        explain_parts.append(f"VIX {vix:+.2f}%")
    if dxy is not None:
        explain_parts.append(f"DXY {dxy:+.2f}%")
    if us10y is not None:
        explain_parts.append(f"US10Y {us10y:+.2f}%")

    return {
        "global_risk_score": score,
        "global_risk_sentiment": sentiment,
        "global_risk_level": risk_level,
        "explain": " / ".join(explain_parts) if explain_parts else "",
    }


# ======== 三、用 DeepSeek 做情绪分析 ========

def _get_deepseek_client() -> OpenAI:
    """获取 DeepSeek 的 OpenAI 兼容客户端。"""

    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("请先在环境变量中设置 DEEPSEEK_API_KEY")
    base_url = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
    return OpenAI(api_key=api_key, base_url=base_url)


def analyze_news_sentiment_with_llm(
    news_list: List[Dict[str, Any]],
    max_news_for_llm: int = 25,
    model: str = "deepseek-reasoner",
    global_signal: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """把新闻列表丢给 DeepSeek，总结情绪与热点。"""

    if not news_list:
        return {
            "market_sentiment": "neutral",
            "score": 50,
            "risk_level": "medium",
            "hot_themes": [],
            "hot_sectors": [],
            "suggested_style": "中性观望，可结合各基金自身网格策略小仓位操作。",
            "comment": "未成功获取新闻数据，使用默认中性情绪。",
            "news_sample_size": 0,
        }

    trimmed = news_list[:max_news_for_llm]

    news_text_lines = []
    for i, n in enumerate(trimmed, start=1):
        line = f"{i}. {n['title']}"
        if n.get("summary"):
            line += f" —— {n['summary']}"
        news_text_lines.append(line)

    news_text = "\n".join(news_text_lines)

    # Global risk summary
    g = global_signal or {}
    g_score = g.get("global_risk_score")
    g_sent = g.get("global_risk_sentiment")
    g_risk = g.get("global_risk_level")
    g_explain = g.get("explain")
    global_brief = ""
    if g_score is not None:
        global_brief = f"全球风险偏好硬指标(优先级高于新闻)：{g_sent} / score={g_score}/100 / risk={g_risk} / {g_explain}".strip()

    system_prompt = """
你是一个专业的 A 股与公募基金量化助手。

你会同时看到两类输入：
1) 全球风险偏好“硬指标”（SPX/纳指/VIX/DXY/美债10Y等）——这是强信号，优先级高。
2) 新闻标题与摘要（可能来自美股或A股）——这是软信号，只做加权，不能主导结论。

你的任务：输出结构化的市场情绪结论，用于次日风格（偏进攻/偏防守/网格波段）建议。

严格要求：
- 不要编造不存在的事件或热点（只能基于输入总结）。
- 若硬指标与新闻矛盾，以硬指标为主，新闻只做解释补充。
- 输出必须是 JSON（由调用方强制）。
"""

    user_prompt = f"""
下面是输入信息，请综合判断今天市场情绪，并用 JSON 格式输出分析结果。

【全球硬指标】（强信号）：
{global_brief or '（缺失）'}

【新闻列表】（软信号，仅加权）：
{news_text}

请严格输出一个 JSON 对象，格式如下（不要带任何多余文字）：

{{
  \"market_sentiment\": \"bullish | bearish | neutral | volatile\",
  \"score\": 0-100,
  \"risk_level\": \"low | medium | high\",
  \"hot_themes\": [\"主题1\", \"主题2\"],
  \"hot_sectors\": [\"板块1\", \"板块2\"],
  \"suggested_style\": \"一句话描述明天应偏进攻 / 防守 / 网格波段 等风格的建议（中文）\",
  \"comment\": \"用 3-5 句话，概括硬指标与新闻所反映的市场情绪、主要利好/利空因素，以及对明天整体 A 股和成长板块的影响（中文）\"
}}
"""

    client = _get_deepseek_client()

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": user_prompt.strip()},
        ],
        temperature=0.3,
        response_format={"type": "json_object"},
    )

    content = resp.choices[0].message.content.strip()

    try:
        result = json.loads(content)
    except Exception:
        print("[news] 无法解析 DeepSeek 返回的 JSON，原始内容：", content)
        result = {
            "market_sentiment": "neutral",
            "score": 50,
            "risk_level": "medium",
            "hot_themes": [],
            "hot_sectors": [],
            "suggested_style": "模型返回异常，建议明天以小仓位网格为主，避免激进加仓。",
            "comment": "模型返回内容解析失败，暂采用中性立场。",
        }

    result["news_sample_size"] = len(trimmed)
    return result


# ======== 四、对外主函数 ========

def get_market_news_sentiment(limit: int = DEFAULT_NEWS_LIMIT) -> Dict[str, Any]:
    # 1) 全球硬指标（强信号）
    g_raw = fetch_global_risk_indicators()
    g = compute_global_risk_score(g_raw)

    # 2) 新闻（软信号，只做加权；可能是 US 或 CN）
    news_list = fetch_finance_news(limit=limit)
    sentiment = analyze_news_sentiment_with_llm(news_list, global_signal=g)

    # 3) 融合：硬指标优先（70%），新闻情绪（30%）
    try:
        llm_score = int(sentiment.get("score", 50))
    except Exception:
        llm_score = 50

    final_score = int(round(0.7 * int(g.get("global_risk_score", 50)) + 0.3 * llm_score))

    # market_sentiment：以 final_score 为主；只有在高风险时允许 volatile 覆盖
    if sentiment.get("risk_level") == "high" and sentiment.get("market_sentiment") == "volatile":
        market_sent = "volatile"
    else:
        if final_score >= 62:
            market_sent = "bullish"
        elif final_score <= 38:
            market_sent = "bearish"
        else:
            market_sent = "neutral"

    sentiment["score"] = final_score
    sentiment["market_sentiment"] = market_sent

    # risk_level：取更谨慎的一方
    g_risk = str(g.get("global_risk_level", "medium"))
    llm_risk = str(sentiment.get("risk_level", "medium"))
    risk_rank = {"low": 0, "medium": 1, "high": 2}
    sentiment["risk_level"] = g_risk if risk_rank.get(g_risk, 1) >= risk_rank.get(llm_risk, 1) else llm_risk

    # 元数据
    sentiment["global_signal"] = g
    sentiment["global_indicators"] = g_raw
    sentiment["news_region"] = _choose_news_region()
    sentiment["fetch_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
    sentiment["last_news_source"] = LAST_NEWS_SOURCE
    return sentiment


if __name__ == "__main__":
    s = get_market_news_sentiment(limit=30)

    print(f"=== 今日财经新闻情绪（新闻源配置: {NEWS_SOURCE} / 实际使用: {LAST_NEWS_SOURCE} + DeepSeek） ===")
    print(f"整体情绪：{s.get('market_sentiment')}（得分：{s.get('score')} / 100）")
    print(f"风险水平：{s.get('risk_level')}")
    print(f"热点主题：{', '.join(s.get('hot_themes') or []) or '无明显主题'}")
    print(f"热点板块：{', '.join(s.get('hot_sectors') or []) or '无明显板块'}")
    print(f"风格建议：{s.get('suggested_style')}")
    print("简要点评：")
    print(s.get("comment"))
    print(f"（本次分析基于 {s.get('news_sample_size')} 条新闻）")
    gs = s.get("global_signal") or {}
    print(f"全球硬指标：{gs.get('global_risk_sentiment')}（score={gs.get('global_risk_score')}/100, risk={gs.get('global_risk_level')}） {gs.get('explain')}")
    print(f"新闻区域：{s.get('news_region')} / 新闻源实际使用：{s.get('last_news_source')}")
