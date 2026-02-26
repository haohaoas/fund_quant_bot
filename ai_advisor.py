# ai_advisor.py
"""
用 DeepSeek 做“多 Agent 基金投顾”的决策层。

思路：
- Python 这一层只做一件事：把我们已有的数据打包为结构化 JSON context；
- DeepSeek 在一个调用里扮演多个 Agent：
  - Market Data Analyst（市场数据分析师）
  - Technical Analyst（技术分析师）
  - Fundamentals Analyst（基础分析师，基金长期表现）
  - Sentiment Analyst（情绪分析师，板块情绪 + 市场氛围）
  - Valuation Analyst（估值分析师，贵 / 便宜）
  - Researcher Bull / Researcher Bear（多头 / 空头研究员）
  - Debate Room（多空辩论 + LLM 第三方打分）
  - Risk Manager（风险经理）
  - Portfolio Manager（投资组合经理，最终给出 BUY / SELL / HOLD）

对外只暴露一个函数：
    ask_deepseek_fund_decision(...)

调用方式保持不变：
    from ai_advisor import ask_deepseek_fund_decision

    ai_decision = ask_deepseek_fund_decision(
        fund_name=...,
        code=...,
        latest=...,
        quant_signal=...,
        sector_info=...,
        fund_profile=...,
    )

返回：
    {"action": "BUY/SELL/HOLD", "reason": "…"}
"""

import os
import json
from typing import Dict, Any, Optional

from openai import OpenAI

# === 配置 DeepSeek / OpenAI 兼容客户端 ===
# 优先使用 DEEPSEEK_API_KEY，没有就用 OPENAI_API_KEY
_DEEPSEEK_API_KEY = "sk-033b834656f24ee88f08254b6b66809f"
_DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
_MODEL_NAME = os.getenv("DEEPSEEK_MODEL", "deepseek-reasoner")

if not _DEEPSEEK_API_KEY:
    print("[ai] 警告：未设置 DEEPSEEK_API_KEY / OPENAI_API_KEY，AI 决策将使用量化策略默认结果。")
    _client: Optional[OpenAI] = None
else:
    _client = OpenAI(
        api_key=_DEEPSEEK_API_KEY,
        base_url=_DEEPSEEK_BASE_URL,
    )

# === 多 Agent 版的系统提示词 ===
SYSTEM_PROMPT = """
你是一个由多个“虚拟分析师”组成的基金投资决策系统，整体风格偏基金、利益最大化优先。
你要分析的是【公募基金 / ETF】，而不是单只股票。

系统内部包含以下 Agent（角色和职责）：

1. Market Data Analyst（市场数据分析师）
   - 读取当前估算净值、今日涨跌幅、价格相对中枢价的位置等信息。
   - 判断今天这只基金的短期走势（上行、下行、震荡）与波动情况。

2. Technical Analyst（技术分析师）
   - 基于网格中枢价 base_price 与 grids（网格价列表）分析：
       - 当前价格在第几层网格附近？是接近低吸区还是高位？
       - 量化策略给出的 action / reason 是否合理？
   - 给出技术面的信号：bullish / bearish / neutral + 置信度。

3. Fundamentals Analyst（基础分析师，基金版）
   - 基于 fund_profile 中可能提供的信息（如：历史表现、波动、基金风格、是否偏行业主题等），
     从“长期质量”和“策略匹配度”角度给出判断。
   - 如果数据有限，你要明确说明“信息不足”，不要臆造具体财务数字。
   - 仍然需要给出 bullish / bearish / neutral 信号与置信度（可以较低）。

4. Sentiment Analyst（情绪分析师）
   - 基于 sector 信息（行业名称、情绪得分、level、comment）判断当前市场对该板块的情绪：
     - 是明显悲观、明显乐观，还是中性犹豫。
   - 输出对情绪的判断：bullish / bearish / neutral + 置信度。

5. Valuation Analyst（估值分析师，基金版）
   - 对于基金没有单一“市盈率”，你需要根据以下思路判断“贵 / 便宜”：
     - 当前价格相对网格中枢价 base_price 的位置（折价 / 溢价）。
     - 如果提供了长期区间位置（例如在 fund_profile.metrics 中），也可以一并考虑。
   - 输出 cheap / expensive / fair 中的一种，再统一映射到 bullish / bearish / neutral 信号。

6. Researcher Bull（多头研究员）
   - 从偏多角度，把以上各 Agent 的结论综合起来，撰写一段看多论点：
     - 为什么现在可以考虑买 / 低吸？
     - 适合什么节奏（小仓 / 分批 / 中长期）？
   - 要求逻辑自洽、非无脑乐观。

7. Researcher Bear（空头研究员）
   - 从偏空角度，列出这只基金当前主要的风险点：
     - 板块景气度、估值水平、回撤风险、流动性风险、风格不匹配等。
   - 给出“不建议现在买 / 建议减仓”的论据，避免情绪化夸大。

8. Debate Room（辩论室）
   - 在多空观点基础上，你作为第三方评审，综合所有 Agent 的信号：
     - 给出 overall_bias: bullish / bearish / balanced
     - 给出 confidence: 0-1
     - 概括关键机会（key_opportunities）和关键风险（key_risks）。

9. Risk Manager（风险经理）
   - 结合：
       - fund_profile 中的 risk（如 high / medium / low / unknown）
       - max_position_pct（建议最大仓位）
       - 当前情绪 & 估值 & 技术信号 & 辩论结论
   - 评估总体风险等级：low / medium / high
   - 给出建议的仓位变化幅度（例如 +10 表示建议加仓 10%，-5 表示减仓 5%，0 表示观望不动）
     注意：建议的加仓后总仓位不宜超过 max_position_pct（如果提供的话）。
   - 输出 risk_view，包含：
       - "risk_level": "low/medium/high"
       - "suggested_delta_pct": 浮点数（-100~+100 之间的小数字即可，如 -10, +5）
       - "comment": 一段解释。

10. Portfolio Manager（投资组合经理）
   - 最终决策者，综合所有 Agent 的观点与 Risk Manager 的建议，给出：
       - action: "BUY" / "SELL" / "HOLD"
       - quantity_pct: 建议仓位变化百分比（例如 +10 表示在当前仓位基础上加 10%，-5 表示减 5%，0 表示不动）
       - confidence: 0-1
       - reasoning: 简洁清晰的中文理由，说明为什么这样操作更稳健。

你的整体风格要求：
- 偏激进，默认不梭哈，偏向“小仓分批、控制回撤”；
- 优先考虑风险，如板块极度悲观、消息面不明朗时，可以建议观望或小仓；
- 在没有足够信息时，要诚实指出“不确定”，而不是随意编造。

你会收到一个 JSON 作为上下文，字段包括：
- fund: {name, code}
- market: {price, pct_change_today, time, source}
- quant_signal: {base_price, grids, action, reason}
- sector: {sector, score, level, comment}
- fund_profile: {risk, max_position_pct, 以及未来可能扩展的 metrics 等}

请你严格按照下面的 JSON 结构进行“最终输出”（注意 key 名称大小写）：

{
  "trading_decision": {
    "action": "BUY 或 SELL 或 HOLD",
    "quantity_pct": 浮点数,         # 建议在当前仓位基础上增减多少百分比，例如 10 表示 +10%，-5 表示 -5%，0 表示不动
    "confidence": 浮点数,          # 0-1 之间
    "reasoning": "最终给用户看的中文理由"
  },
  "agent_signals": [
    {
      "agent": "TechnicalAnalyst",
      "signal": "bullish/bearish/neutral",
      "confidence": 浮点数,
      "details": "简要说明"
    }
    # 其他 Agent 同理，可以有 FundamentalsAnalyst / SentimentAnalyst / ValuationAnalyst 等
  ],
  "debate": {
    "overall_bias": "bullish/bearish/balanced",
    "confidence": 浮点数,
    "summary": "多空辩论的综合结论",
    "key_risks": ["风险点1", "风险点2"],
    "key_opportunities": ["机会点1", "机会点2"]
  },
  "risk": {
    "risk_level": "low/medium/high",
    "suggested_delta_pct": 浮点数,
    "comment": "风险经理的结论与仓位建议说明"
  }
}

注意：
- 所有字段必须可以被 JSON 解析；
- 不要输出任何额外文字（例如解释说明、Markdown 标题等），只输出一个合法 JSON 对象。
"""


def ask_deepseek_fund_decision(
    fund_name: str,
    code: str,
    latest: Dict[str, Any],
    quant_signal: Dict[str, Any],
    sector_info: Optional[Dict[str, Any]] = None,
    fund_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    fund_name: 基金名称，例如 '华夏国证半导体芯片ETF联接C'
    code: 基金代码，例如 '008888'
    latest: get_fund_latest_price 返回的 dict，例如：
        {
            "price": float,
            "pct": float,        # 今日估算涨跌（单位：百分比，例如 -2.5）
            "time": datetime 或 str,
            "source": "eastmoney_realtime" ...
        }
    quant_signal: generate_today_signal 返回的 dict，例如：
        {
            "action": "BUY/SELL/HOLD",
            "reason": "……",
            "base_price": float,
            "grids": [float, ...],
        }
    sector_info: get_sector_sentiment 返回的 dict，例如：
        {
            "sector": "半导体芯片",
            "score": 50,
            "level": "中性",
            "comment": "多空力量相对均衡……"
        }
    fund_profile: 配置中的基金元信息，例如：
        {
            "risk": "high/medium/low",
            "max_position_pct": 0.3,
            # 未来可以加 metrics: {...}
        }
    """
    # 如果没有配置 LLM client，则直接使用量化策略输出
    if _client is None:
        return {
            "action": quant_signal.get("action", "HOLD"),
            "reason": "未配置 AI API，直接采用量化策略建议作为最终动作。",
        }

    # 组装多 Agent 的上下文
    context = {
        "fund": {
            "name": fund_name,
            "code": code,
        },
        "market": {
            "price": latest.get("price"),
            "pct_change_today": latest.get("pct"),
            "time": str(latest.get("time")),
            "source": latest.get("source"),
        },
        "quant_signal": {
            "base_price": quant_signal.get("base_price"),
            "grids": quant_signal.get("grids"),
            "action": quant_signal.get("action"),
            "reason": quant_signal.get("reason"),
        },
        "sector": sector_info
        or {
            "sector": "未知板块",
            "score": 50,
            "level": "中性",
            "comment": "未提供板块信息，按中性情绪处理。",
        },
        "fund_profile": fund_profile
        or {
            "risk": "unknown",
            "max_position_pct": None,
        },
    }

    user_prompt = (
        "下面是某只基金的量化分析结果、今日行情、所属板块的情绪信息，以及风险配置。\n"
        "请你按照系统提示中定义的多 Agent 架构进行内部分析、辩论与风险评估，"
        "然后只输出一个 JSON 对象，结构必须与说明完全一致。\n\n"
        f"{json.dumps(context, ensure_ascii=False, indent=2)}"
    )

    try:
        resp = _client.chat.completions.create(
            model=_MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.3,
        )
        content = resp.choices[0].message.content
        data = json.loads(content)

        # 解析 Portfolio Manager 的最终决策
        trading = data.get("trading_decision", {}) if isinstance(data, dict) else {}
        raw_action = str(trading.get("action", "")).upper()
        reasoning = trading.get("reasoning", "") or data.get("reason", "")

        # 安全兜底
        if raw_action not in ("BUY", "SELL", "HOLD"):
            raw_action = quant_signal.get("action", "HOLD")
        if not reasoning:
            reasoning = "模型未给出详细理由。"

        # 你现在 run_fund_daily.py 只用到了 action/reason，其余信息如果想用，可以再加字段返回
        return {
            "action": raw_action,
            "reason": reasoning,
            # 如果你以后想调试 agent 细节，可以把整个 data 一并返回（当前用不上）
            # "raw": data,
        }

    except Exception as e:
        print(f"[ai] 调用 DeepSeek 失败 {code}: {e}")
        # 兜底：直接用原量化策略
        return {
            "action": quant_signal.get("action", "HOLD"),
            "reason": f"DeepSeek 调用失败，使用量化默认动作。原因: {e}",
        }