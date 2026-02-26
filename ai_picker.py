# ai_picker.py
"""
AI 选基器（明日关注 & 排序）
将“新闻情绪（宏观）+ 每只基金的日内量化结论（微观）”融合，给出明天优先关注/买入候选。

输入（示例结构）：
    news_view: 从 news_sentiment.get_market_news_sentiment() 返回，例如：
      {
        "market_sentiment": "bullish|bearish|neutral|volatile",
        "score": 68,
        "risk_level": "medium",
        "hot_themes": ["AI", "新能源"],
        "hot_sectors": ["半导体芯片", "机器人"],
        "suggested_style": "...",
        "comment": "...",
        "news_sample_size": 25
      }

    fund_summaries: 列表，每只基金一条，建议在 run_fund_daily 中整理为：
      {
        "code": "008888",
        "name": "华夏国证半导体芯片ETF联接C",
        "sector": "半导体芯片",
        "latest": {
            "price": 1.5215,
            "pct": -3.97,        # 今日估值涨跌（百分比数值）
            "time": "2025-11-17 15:00",
            "source": "eastmoney_realtime"
        },
        "quant": {
            "action": "BUY|SELL|HOLD",
            "reason": "…",
            "base_price": 1.60,
            "grids": [1.58, 1.60, 1.62]
        },
        "sector_view": {
            "score": 52,         # 0-100
            "level": "中性",
            "comment": "…"
        },
        "ai_decision": {
            "action": "BUY|SELL|HOLD",
            "reason": "…"
        },
        # 可选：你在 config 里定义的风险/最大仓位等
        "fund_profile": {
            "risk": "high|medium|low|unknown",
            "max_position_pct": 0.3
        }
      }

输出（dict）：
{
  "tomorrow_focus": ["008888", "014881"],            # 最优先关注/操作的候选
  "ranking": [
    {"code":"008888","name":"…","score":91.3,"suggest":"BUY +5%","why":"…"},
    {"code":"014881","name":"…","score":84.1,"suggest":"BUY +3%","why":"…"},
    ...
  ],
  "style": {
    "market_sentiment":"bullish",
    "score":68,
    "risk_level":"medium",
    "suggested_style":"…（来自新闻情绪）"
  },
  "comment":"综合点评"
}

用法：
from ai_picker import pick_funds_for_tomorrow
result = pick_funds_for_tomorrow(news_view, fund_summaries)
"""

import os
import json
from typing import List, Dict, Any, Optional

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

from market_scanner import scan_market_for_tomorrow

# 可选：AkShare（用于“资金流Top板块 + 板块K线趋势”驱动全市场选基）
try:
    import akshare as ak
except Exception:
    ak = None


# ============ LLM 客户端 ============

def _get_client() -> Optional[OpenAI]:
    """
    支持 DeepSeek / 其它 OpenAI 兼容服务：
      - DEEPSEEK_API_KEY（优先）或 OPENAI_API_KEY
      - DEEPSEEK_API_BASE（可选，默认 https://api.deepseek.com）
      - DEEPSEEK_MODEL（默认 deepseek-reasoner / deepseek-chat 等）
    """
    if OpenAI is None:
        return None

    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    base = os.getenv("DEEPSEEK_API_BASE", "https://api.deepseek.com")
    return OpenAI(api_key=api_key, base_url=base)
# ========================
# 全市场选基的“真实驱动”：资金流Top板块 + 板块K线趋势
# ========================

import re
from datetime import datetime

def _mf_norm_name(name: str) -> str:
    """尽量把板块名规范化，提升 name->code 的命中率（去空格/全角括号内容等）。"""
    s = (name or "").strip()
    if not s:
        return s
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"\s+", "", s)
    # 去掉括号里的说明：如 跨境支付(CIPS) -> 跨境支付
    s2 = re.sub(r"\(.*?\)", "", s)
    return s2 or s

def _mf_lookup_symbol(name: str, mp: dict) -> str:
    """按多种方式尝试把板块名映射到 BK 代码。"""
    if not name or not mp:
        return None
    # 1) 直接命中
    if name in mp:
        return mp.get(name)
    # 2) 规范化后命中
    n1 = _mf_norm_name(name)
    if n1 in mp:
        return mp.get(n1)
    # 3) 反向遍历（容错：mp 里可能带括号/空格）
    for k, v in mp.items():
        if _mf_norm_name(k) == n1:
            return v
    return None


def _model_name() -> str:
    return os.getenv("DEEPSEEK_MODEL", "deepseek-reasoner")


# ============ 评分合成（无LLM兜底） ============

def _rule_based_score(item: Dict[str, Any], news_view: Dict[str, Any]) -> Dict[str, Any]:
    """
    无 LLM 或 LLM 失败时的兜底打分逻辑（简单、可解释）。
    返回 {"score": float, "suggest": "BUY +5%"|"HOLD"|"SELL -3%", "why": "..."}
    """
    latest = item.get("latest", {}) or {}
    quant = item.get("quant", {}) or {}
    sector_view = item.get("sector_view", {}) or {}
    profile = item.get("fund_profile", {}) or {}

    price = latest.get("price")
    pct = latest.get("pct")  # 今日估值涨跌
    q_action = (quant.get("action") or "HOLD").upper()
    base_price = quant.get("base_price")
    grids = quant.get("grids") or []
    sector = item.get("sector") or ""
    ai_action = ((item.get("ai_decision") or {}).get("action") or "HOLD").upper()

    news_sent = (news_view or {}).get("market_sentiment", "neutral")
    news_score = float((news_view or {}).get("score", 50))
    hot_secs = set((news_view or {}).get("hot_sectors") or [])

    def _in_hot(sector_name: str, hot_set: set) -> float:
        s = (sector_name or "").strip()
        if not s or not hot_set:
            return 0.0
        if s in hot_set:
            return 1.0
        # 宽松匹配：只要出现包含关系就算命中（例如“科技” vs “科技板块”）
        for h in hot_set:
            h2 = (h or "").strip()
            if not h2:
                continue
            if h2 in s or s in h2:
                return 1.0
        return 0.0

    in_hot = _in_hot(sector, hot_secs)

    # 基础分：来自新闻情绪（40%）
    score = 40.0 * (news_score / 100.0)  # 0~40

    # 行业热点加成（10%）
    score += 10.0 * in_hot

    # 量化信号（25%）：BUY +10，HOLD +5，SELL 0；若与 AI 同向再加权
    quant_part = 10.0 if q_action == "BUY" else (5.0 if q_action == "HOLD" else 0.0)
    ai_part = 10.0 if ai_action == "BUY" else (5.0 if ai_action == "HOLD" else 0.0)
    # 若同向，额外 +5
    if q_action == ai_action:
        quant_part += 5.0
    score += 25.0 * (quant_part + ai_part) / 30.0  # 0~25

    # 位置因子（15%）：当前价低于 base_price，按“折价”比例加分（最多 15）
    pos_bonus = 0.0
    if price and base_price:
        try:
            discount = (base_price - float(price)) / base_price  # 低于基准越多，越加分
            pos_bonus = max(0.0, min(0.15, discount)) * 100  # 映射 0~15
        except Exception:
            pos_bonus = 0.0
    score += pos_bonus

    # 当日跌幅（10%）：下跌但非暴跌，适度加分（便于低吸）
    if pct is not None:
        if -3.5 <= float(pct) <= -0.3:
            score += 8.0
        elif pct < -3.5:
            score += 3.0  # 可能有系统性风险，保守一点
        elif 0 <= float(pct) <= 1.0:
            score += 2.0  # 温和上涨日也可关注
        # 大涨就不鼓励追高：不加分

    # 风险画像（10%）：高风险基金，适度扣分；低风险，适度加分
    risk = (profile.get("risk") or "unknown").lower()
    if risk == "low":
        score += 6.0
    elif risk == "medium":
        score += 3.0
    elif risk == "high":
        score -= 2.0

    # 建议动作（简化）
    suggest = "HOLD"
    delta = 0
    why_parts = []
    why_parts.append(f"新闻情绪 {news_sent}（{int(news_score)}/100）")
    if in_hot:
        why_parts.append("处于热点板块")
    why_parts.append(f"量化={q_action} / AI={ai_action}")
    if base_price and price:
        try:
            dis_pct = (base_price - float(price)) / base_price * 100
            why_parts.append(f"相对中枢价折溢：{dis_pct:+.1f}%")
        except Exception:
            pass
    if pct is not None:
        why_parts.append(f"当日估值 {float(pct):+.2f}%")

    if score >= 80:
        suggest, delta = "BUY", +6
    elif score >= 70:
        suggest, delta = "BUY", +4
    elif score >= 60:
        suggest, delta = "BUY", +2
    elif score >= 50:
        suggest, delta = "HOLD", 0
    elif score >= 40:
        suggest, delta = "HOLD", 0
    else:
        suggest, delta = "HOLD", 0  # 基金不建议盲目做空/赎回，由你手动定止损

    return {
        "score": round(float(score), 1),
        "suggest": f"{suggest} {delta:+d}%",
        "why": "；".join(why_parts),
    }


# ============ LLM 版排序与推荐 ============

_SYSTEM_PROMPT = """
你是基金多Agent系统的“选基协调者”。请根据“市场新闻情绪”和“各基金的日内摘要”进行打分与排序，
目标是为“明天的操作”给出最优先关注的候选。风格保守，鼓励分批和网格，不鼓励追高。

打分规则（建议，但可被你自行优化）：
- 综合考虑：新闻情绪、是否为热点板块、量化动作与AI动作是否同向、相对中枢价的位置、当日跌幅。
- 若市场情绪偏空但个别基金处于热点板块且价格在低位，可小幅正向。
- 结果请总是用 JSON 严格输出。

重要约束：
- “热点板块/热点主题”必须严格以输入 news_view.hot_sectors / hot_themes 为准，不得自行新增或编造。
- 如果输入里提供了 drivers/_market_drivers 字段，说明热点来自资金流Top榜+板块K线过滤，你必须围绕它解释。
"""


_USER_PROMPT_TPL = """
【今日新闻情绪】：
{news_json}

【基金候选】（每条为一只基金的摘要）：
{funds_json}

请输出一个 JSON（不要任何额外文字）：
{{
  "tomorrow_focus": ["基金代码1","基金代码2"],           // 最优先关注/操作的候选，1~3只
  "ranking": [                                            // 按推荐度从高到低排序
    {{
      "code": "008888",
      "name": "华夏国证半导体芯片ETF联接C",
      "score": 0-100,                                     // 你的综合分
      "suggest": "BUY +5% | HOLD | SELL -3%",             // 明天建议动作（小仓/分批）
      "why": "中文简要理由"
    }}
  ],
  "style": {{
    "market_sentiment": "bullish|bearish|neutral|volatile",
    "score": 0-100,
    "risk_level": "low|medium|high",
    "suggested_style": "一句话风格建议（中文）"
  }},
  "comment": "3-6 行的综合点评（中文），尽量可执行。"
}}
"""

# ============ prompt 压缩与硬限长（避免超上下文） ============

def _compact_news_view_for_llm(news_view: Dict[str, Any]) -> Dict[str, Any]:
    """只保留结论级字段，避免把新闻原文/长列表塞进 LLM。"""
    if not isinstance(news_view, dict) or not news_view:
        return {}
    out = {
        "market_sentiment": news_view.get("market_sentiment"),
        "score": news_view.get("score"),
        "risk_level": news_view.get("risk_level"),
        "hot_themes": (news_view.get("hot_themes") or [])[:10],
        "hot_sectors": (news_view.get("hot_sectors") or [])[:20],
        "suggested_style": news_view.get("suggested_style"),
        "news_sample_size": news_view.get("news_sample_size"),
    }
    c = news_view.get("comment")
    if isinstance(c, str) and c:
        out["comment"] = c[:400]

    # drivers 可能很大：只留关键字段
    drv = news_view.get("drivers")
    if isinstance(drv, dict) and drv:
        out["drivers"] = {
            "driver_sectors": (drv.get("driver_sectors") or [])[:20],
            "fetched_at": drv.get("fetched_at"),
            "note": drv.get("note"),
        }
    return out


def _compact_fund_for_llm(item: Dict[str, Any]) -> Dict[str, Any]:
    """基金摘要压缩：只给 LLM 看必要字段，避免 token 爆炸。"""
    latest = item.get("latest", {}) or {}
    quant = item.get("quant", {}) or {}
    sector_view = item.get("sector_view", {}) or {}
    ai_dec = item.get("ai_decision", {}) or {}
    profile = item.get("fund_profile", {}) or {}

    return {
        "code": item.get("code"),
        "name": item.get("name"),
        "sector": item.get("sector"),
        "latest": {
            "price": latest.get("price"),
            "pct": latest.get("pct"),
            "time": latest.get("time"),
            "source": latest.get("source"),
        },
        "quant": {
            "action": quant.get("action"),
            "reason": (quant.get("reason") or "")[:180],
            "base_price": quant.get("base_price"),
            "grids": (quant.get("grids") or [])[:8],
        },
        "sector_view": {
            "score": sector_view.get("score"),
            "level": sector_view.get("level"),
            "comment": (sector_view.get("comment") or "")[:120],
        },
        "ai_decision": {
            "action": ai_dec.get("action"),
            "reason": (ai_dec.get("reason") or "")[:220],
        },
        "fund_profile": {
            "risk": profile.get("risk"),
            "max_position_pct": profile.get("max_position_pct"),
        },
    }


def _truncate_str(s: str, max_chars: int) -> str:
    if not isinstance(s, str):
        s = str(s)
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + "\n...[TRUNCATED]..."


def _build_llm_user_prompt(news_view: Dict[str, Any], fund_summaries: List[Dict[str, Any]], max_funds: int = 40, max_chars: int = 60000) -> str:
    """构造强约束的 user prompt：候选 TopN + 总字符硬限长。"""
    nv = _compact_news_view_for_llm(news_view or {})

    # 候选只取 TopN，避免把全量/冗余字段塞进去
    fs = (fund_summaries or [])[: max(1, int(max_funds))]
    fs = [_compact_fund_for_llm(x or {}) for x in fs]

    news_json = json.dumps(nv, ensure_ascii=False)
    funds_json = json.dumps(fs, ensure_ascii=False)

    user_prompt = _USER_PROMPT_TPL.format(news_json=news_json, funds_json=funds_json)
    user_prompt = _truncate_str(user_prompt, max_chars=max_chars)

    # 便于你排查：看这次到底塞了多少
    try:
        print(f"[ai_picker] llm_prompt_chars={len(user_prompt)} funds_for_llm={len(fs)}")
    except Exception:
        pass

    return user_prompt


def _llm_rank(news_view: Dict[str, Any], fund_summaries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    client = _get_client()
    if client is None:
        return None

    # 关键：强制压缩 & 限长，避免超过模型上下文
    user_prompt = _build_llm_user_prompt(
        news_view=news_view or {},
        fund_summaries=fund_summaries or [],
        max_funds=int(os.getenv("AI_PICKER_MAX_FUNDS", "40")),
        max_chars=int(os.getenv("AI_PICKER_MAX_CHARS", "60000")),
    )

    try:
        resp = client.chat.completions.create(
            model=_model_name(),
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT.strip()},
                {"role": "user", "content": user_prompt.strip()},
            ],
            temperature=0.3,
            response_format={"type": "json_object"},  # 强制纯 JSON
        )
        content = resp.choices[0].message.content.strip()
        return json.loads(content)
    except Exception as e:
        print(f"[ai_picker] LLM 排序失败：{e}")
        return None


# ========================
# 全市场选基的“真实驱动”：资金流Top板块 + 板块K线趋势
# ========================

def _mf_safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "").replace("%", "")
        if s in ("", "nan", "None"):
            return default
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


def _mf_pick_first_col(df, candidates):
    if df is None or len(getattr(df, "columns", [])) == 0:
        return None
    cols = [str(c) for c in df.columns]
    for key in candidates:
        for c in cols:
            if key in c:
                return c
    return None


def _mf_board_code_maps():
    """返回 (industry_name->BK, concept_name->BK) 两张映射表。"""
    if ak is None:
        return {}, {}
    ind_map = {}
    con_map = {}
    try:
        ind = ak.stock_board_industry_name_em()
        if ind is not None and len(ind) > 0 and "板块名称" in ind.columns and "板块代码" in ind.columns:
            for _, r in ind.iterrows():
                nm = str(r.get("板块名称"))
                cd = str(r.get("板块代码"))
                ind_map[nm] = cd
                ind_map[_mf_norm_name(nm)] = cd
    except Exception:
        pass
    try:
        con = ak.stock_board_concept_name_em()
        if con is not None and len(con) > 0 and "板块名称" in con.columns and "板块代码" in con.columns:
            for _, r in con.iterrows():
                nm = str(r.get("板块名称"))
                cd = str(r.get("板块代码"))
                con_map[nm] = cd
                con_map[_mf_norm_name(nm)] = cd
    except Exception:
        pass
    return ind_map, con_map


def _mf_kline_features(board_type: str, symbol: str, days: int = 120) -> dict:
    """板块K线趋势摘要（只返回关键特征，失败返回 error）。"""
    if ak is None:
        return {"error": "akshare not available"}
    try:
        if board_type == "concept":
            fn = getattr(ak, "stock_board_concept_hist_em", None)
        else:
            fn = getattr(ak, "stock_board_industry_hist_em", None)
            board_type = "industry"
        if fn is None:
            return {"error": "kline api not available"}
        df = fn(symbol=symbol)
        if df is None or len(df) == 0:
            return {"error": "empty kline"}
        df = df.tail(max(60, int(days))).copy()
        close = df["收盘"].astype(float).tolist()
        if len(close) < 5:
            return {"error": "kline too short"}

        def pct(a, b):
            try:
                if b == 0:
                    return 0.0
                return (a / b - 1.0) * 100.0
            except Exception:
                return 0.0

        def ma(n):
            n = int(n)
            if len(close) < n:
                return sum(close) / len(close)
            return sum(close[-n:]) / n

        ret_5d = pct(close[-1], close[-6]) if len(close) >= 6 else 0.0
        ret_20d = pct(close[-1], close[-21]) if len(close) >= 21 else 0.0
        ma5 = ma(5)
        ma20 = ma(20)
        if ma5 > ma20 * 1.002:
            ma_cross = "bull"
        elif ma5 < ma20 * 0.998:
            ma_cross = "bear"
        else:
            ma_cross = "flat"

        return {
            "ret_5d": float(ret_5d),
            "ret_20d": float(ret_20d),
            "ma5": float(ma5),
            "ma20": float(ma20),
            "ma_cross": ma_cross,
        }
    except Exception as e:
        return {"error": str(e)}


def _mf_market_drivers(top_n: int = 15, pick_n: int = 8) -> dict:
    """从资金流Top榜 + 板块趋势筛出“驱动板块列表”，供全市场选基使用。"""
    if ak is None:
        return {"driver_sectors": [], "error": "akshare not available"}

    ind_map, con_map = _mf_board_code_maps()

    def fetch_rank(bt: str):
        if bt == "concept":
            fn = getattr(ak, "stock_fund_flow_concept", None) or getattr(ak, "stock_fund_flow_concept_em", None)
        else:
            fn = getattr(ak, "stock_sector_fund_flow_rank", None) or getattr(ak, "stock_sector_fund_flow_summary", None)
            bt = "industry"
        if fn is None:
            return []
        df = fn()
        if df is None or len(df) == 0:
            return []

        name_col = _mf_pick_first_col(df, ["板块名称", "行业", "概念", "名称"]) or df.columns[1]
        pct_col = _mf_pick_first_col(df, ["行业-涨跌幅", "涨跌幅", "涨跌", "pct", "%"])
        main_col = _mf_pick_first_col(df, ["主力净流入", "主力净额", "净额", "净流入", "流入资金", "主力"])  # 概念榜多为净额

        items = []
        for _, r in df.iterrows():
            name = str(r.get(name_col))
            if not name or name == "nan":
                continue
            pct = None
            if pct_col:
                try:
                    pct = float(str(r.get(pct_col)).replace("%", ""))
                except Exception:
                    pct = None
            main = _mf_safe_float(r.get(main_col)) if main_col else 0.0
            # 概念榜资金字段通常以“亿”为单位（如 12.98），转成元方便统一排序
            if bt == "concept" and abs(main) < 1e6:
                main = main * 1e8

            symbol = _mf_lookup_symbol(name, ind_map if bt == "industry" else con_map)

            items.append({"name": name, "symbol": symbol, "board_type": bt, "main_inflow": float(main), "pct": pct})

        items.sort(key=lambda x: x.get("main_inflow", 0.0), reverse=True)
        return items[: max(5, int(top_n))]

    industry_top = fetch_rank("industry")
    concept_top = fetch_rank("concept")

    def add_trend(it: dict):
        sym = it.get("symbol")
        bt = it.get("board_type")
        k = _mf_kline_features(bt, sym) if sym else {"error": "no symbol"}
        trend = 0.0
        if not k.get("error"):
            trend += 1.0 if k.get("ma_cross") == "bull" else (-1.0 if k.get("ma_cross") == "bear" else 0.0)
            trend += 1.0 if k.get("ret_20d", 0.0) > 0 else -1.0
        it["kline"] = k
        it["trend_score"] = trend
        return it

    scored = []
    for it in (industry_top[: max(5, int(top_n))] + concept_top[: max(5, int(top_n))]):
        scored.append(add_trend(it))

    # 排序：先资金，再趋势（避免趋势明显走弱的板块被硬推）
    scored.sort(key=lambda x: (x.get("main_inflow", 0.0), x.get("trend_score", 0.0)), reverse=True)

    driver_sectors = []
    for it in scored:
        if len(driver_sectors) >= int(pick_n):
            break
        driver_sectors.append(it["name"])

    return {
        "driver_sectors": driver_sectors,
        "industry_top": [x for x in scored if x.get("board_type") == "industry"][: max(5, int(top_n))],
        "concept_top": [x for x in scored if x.get("board_type") == "concept"][: max(5, int(top_n))],
        "error": None,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "note": "热点板块来自：主力资金Top榜 + 板块K线趋势过滤（非LLM编造）",
    }


# ============ 对外主函数 ============

# 插入 drivers 相关代码
def pick_funds_for_tomorrow(
    news_view: Dict[str, Any],
    fund_summaries: List[Dict[str, Any]],
    use_llm_first: bool = True,
    top_k_focus: int = 2,
    scene_desc=None,
    **kwargs,
) -> Dict[str, Any]:
    """
    综合“新闻情绪 + 各基金摘要”，输出明日关注排序。
    - 优先使用 LLM；若失败或没配置，则使用规则兜底。
    """
    # 兼容版本差异：允许外部传入 scene_desc/scene 等字段，避免 unexpected keyword
    if scene_desc is None:
        scene_desc = kwargs.pop("scene", None)
    else:
        kwargs.pop("scene", None)
    # 其余多余参数直接丢弃
    kwargs.clear()

    if not fund_summaries:
        return {
            "tomorrow_focus": [],
            "ranking": [],
            "style": {
                "market_sentiment": (news_view or {}).get("market_sentiment", "neutral"),
                "score": (news_view or {}).get("score", 50),
                "risk_level": (news_view or {}).get("risk_level", "medium"),
                "suggested_style": (news_view or {}).get("suggested_style", "中性观望"),
            },
            "comment": "未提供基金候选，无法给出明日关注建议。",
        }

    # 1) 先尝试 LLM 版
    if use_llm_first:
        llm_res = _llm_rank(news_view, fund_summaries)
        if llm_res and isinstance(llm_res, dict) and "ranking" in llm_res:
            # 限制 tomorrow_focus 个数
            focus = list((llm_res.get("tomorrow_focus") or [])[:max(1, top_k_focus)])
            llm_res["tomorrow_focus"] = focus
            return llm_res

    # 2) 兜底：规则打分
    scored = []
    for item in fund_summaries:
        rb = _rule_based_score(item, news_view or {})
        scored.append({
            "code": item.get("code"),
            "name": item.get("name"),
            "score": rb["score"],
            "suggest": rb["suggest"],
            "why": rb["why"],
        })

    # 排序
    scored.sort(key=lambda x: x["score"], reverse=True)
    focus = [x["code"] for x in scored[:max(1, top_k_focus)]]

    return {
        "tomorrow_focus": focus,
        "ranking": scored,
        "style": {
            "market_sentiment": (news_view or {}).get("market_sentiment", "neutral"),
            "score": (news_view or {}).get("score", 50),
            "risk_level": (news_view or {}).get("risk_level", "medium"),
            "suggested_style": (news_view or {}).get("suggested_style", "中性观望"),
        },
        "comment": "基于规则的兜底排序：融合新闻情绪、热点板块、量化/AI方向一致性、相对中枢位置与当日跌幅等因素。",
    }
def pick_market_funds_for_tomorrow(
    news_view: Dict[str, Any],
    use_llm_first: bool = True,
    top_k_focus: int = 10,
) -> Dict[str, Any]:
    """
    全市场版本：
    - 调用你自己的 market_scanner.scan_market_for_tomorrow()
    - 拿到一批“热点板块对应的 ETF 候选”
    - 然后直接复用 pick_funds_for_tomorrow 做打分 & 排名
    """
    # 1) 真实驱动：资金流Top板块 + 板块K线趋势（用于决定“看哪些板块/拿哪些ETF候选”）
    try:
        drivers = _mf_market_drivers(top_n=15, pick_n=max(6, int(top_k_focus) * 3))
    except Exception as e:
        print(f"[warn] 全市场 AI 选基失败（资金流驱动降级）：{e}")
        drivers = {
            "driver_sectors": [],
            "industry_top": [],
            "concept_top": [],
            "error": str(e),
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "note": "资金流接口异常，已自动降级为无驱动全市场扫描",
        }
    driver_sectors = drivers.get("driver_sectors") or []

    # 2) 扫描候选：仍复用你的 scanner，但后面会用 driver_sectors 过滤，避免“模型瞎编热点”
    market_candidates = scan_market_for_tomorrow(
        max_sectors=max(8, len(driver_sectors) or 8),
        max_funds_per_sector=3,
    )

    # 3) 过滤：只保留 driver_sectors 命中的候选（名称允许包含匹配）
    if driver_sectors and market_candidates:
        hot_set = set(driver_sectors)
        filtered = []
        for it in market_candidates:
            sec = (it.get("sector") or "").strip()
            if not sec:
                continue
            ok = (sec in hot_set)
            if not ok:
                for h in hot_set:
                    if h in sec or sec in h:
                        ok = True
                        break
            if ok:
                filtered.append(it)

        # 如果过滤后太少（< top_k_focus），不要硬过滤，避免只剩 2~3 只
        if len(filtered) >= max(5, int(top_k_focus)):
            market_candidates = filtered

    # 4) 把 drivers 写回 news_view，让 LLM/规则打分都按“真实热点”工作
    nv = dict(news_view or {})
    if driver_sectors:
        nv["hot_sectors"] = driver_sectors
    nv["drivers"] = drivers

    res = pick_funds_for_tomorrow(
        news_view=nv,
        fund_summaries=market_candidates,
        use_llm_first=use_llm_first,
        top_k_focus=top_k_focus,
        scene_desc="全市场AI选基（驱动=资金流Top板块 + 板块K线趋势；候选=ETF实时行情）",
    )

    # 5) 把 drivers 也透传到返回结果（方便你在 run_fund_daily 里打印/追踪）
    if isinstance(res, dict):
        res["drivers"] = drivers
    return res
