# backend/reco_service.py
"""
推荐服务：
- 读取真实账户状态（现金 / 持仓）
- 调用策略计算
- 输出“可执行”的投资建议（JSON）
"""

from typing import Dict, Any, List
from datetime import datetime

from backend.portfolio_service import (
    get_account_cash,
    list_positions,
)

# 直接复用你现有的策略模块
# ⚠️ 不要在这里 import FastAPI
from run_fund_daily import compute_daily_result


# ======================
# 配置：风控 & 约束
# ======================

MAX_POSITION_WEIGHT = 0.30      # 单只基金最大仓位 30%
MAX_DAILY_ADD_RATIO = 0.05      # 单日最大加仓：总资产的 5%


# ======================
# 工具函数
# ======================

def _calc_total_asset(positions: List[Dict[str, Any]], cash: float) -> float:
    total = cash
    for p in positions:
        latest = p.get("latest") or {}
        price = latest.get("price")
        if price is not None:
            total += price * p["shares"]
    return total


def _attach_position_metrics(
    positions: List[Dict[str, Any]],
    total_asset: float,
) -> None:
    """
    原地补充：
    - market_value
    - weight
    - pnl
    """
    for p in positions:
        latest = p.get("latest") or {}
        price = latest.get("price")
        if price is None:
            p["market_value"] = None
            p["weight"] = None
            p["pnl"] = None
            continue

        mv = price * p["shares"]
        p["market_value"] = mv
        p["weight"] = mv / total_asset if total_asset > 0 else 0
        p["pnl"] = mv - p["cost"] * p["shares"]


# ======================
# 核心接口
# ======================

def get_recommendations() -> Dict[str, Any]:
    """
    给前端用的主入口：
    - 真实账户
    - 今日策略
    - 最终建议
    """

    # 1️⃣ 账户真实状态
    cash = get_account_cash()
    positions = list_positions()

    # 2️⃣ 今日策略结果（结构化 JSON）
    strategy = compute_daily_result()

    # 3️⃣ 构建 fund_map
    fund_map = {f["code"]: f for f in strategy.get("funds", [])}

    # 4️⃣ 回填最新价
    for p in positions:
        f = fund_map.get(p["code"])
        if f:
            p["latest"] = f.get("latest")

    # 5️⃣ 总资产 & 指标
    total_asset = _calc_total_asset(positions, cash)
    _attach_position_metrics(positions, total_asset)

    # 6️⃣ 生成“可执行建议”
    actions: List[Dict[str, Any]] = []

    for p in positions:
        code = p["code"]
        weight = p.get("weight") or 0
        latest = p.get("latest") or {}
        price = latest.get("price")

        if price is None:
            continue

        strat = fund_map.get(code, {})
        ai_view = strat.get("ai_decision", {})

        # ===== 加仓判断 =====
        if (
            weight < MAX_POSITION_WEIGHT
            and cash > 0
            and ai_view.get("action") in ("BUY", "ADD")
        ):
            max_add_cash = total_asset * MAX_DAILY_ADD_RATIO
            suggested_cash = min(max_add_cash, cash)

            actions.append(
                {
                    "code": code,
                    "action": "ADD",
                    "suggest_cash": round(suggested_cash, 2),
                    "reason": "AI 看多 + 仓位未满",
                }
            )

        # ===== 减仓判断 =====
        if ai_view.get("action") in ("SELL", "REDUCE"):
            actions.append(
                {
                    "code": code,
                    "action": "REDUCE",
                    "suggest_ratio": 0.1,
                    "reason": "AI 风险提示 / 趋势转弱",
                }
            )

    # 7️⃣ 循环结束后，统一生成 summary
    summary = build_daily_summary(
        {
            "account": {"cash": cash},
            "positions": positions,
            "actions": actions,
        }
    )

    # 8️⃣ 返回最终结果
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "account": {
            "cash": cash,
            "total_asset": total_asset,
        },
        "positions": positions,
        "actions": actions,
        "summary": summary,
        "strategy_snapshot": {
            "news": strategy.get("news"),
            "market_picker": strategy.get("market_picker"),
        },
    }
def build_daily_summary(reco: Dict[str, Any]) -> Dict[str, Any]:
    """
    把结构化推荐，整理成“人话总结”
    """
    actions = reco.get("actions", [])
    positions = reco.get("positions", [])
    cash = reco.get("account", {}).get("cash", 0)

    summary_lines = []
    risk_notes = []

    if not actions:
        summary_lines.append("今天没有明显的操作机会，建议观望。")
    else:
        summary_lines.append(f"今天有 {len(actions)} 个可关注的操作建议。")

    for act in actions:
        if act["action"] == "ADD":
            summary_lines.append(
                f"👉 {act['code']}：建议加仓约 {act.get('suggest_cash', 0):.0f} 元（{act.get('reason')}）"
            )
        elif act["action"] == "REDUCE":
            ratio = int(act.get("suggest_ratio", 0) * 100)
            summary_lines.append(
                f"👉 {act['code']}：建议减仓约 {ratio}%（{act.get('reason')}）"
            )

    # 简单风控提示
    if cash <= 0:
        risk_notes.append("⚠️ 当前现金不足，注意流动性风险。")

    for p in positions:
        w = p.get("weight")
        if w is not None and w > 0.30:
            risk_notes.append(f"⚠️ {p['code']} 仓位已超过 30%，注意集中度风险。")

    return {
        "headline": " | ".join(summary_lines[:1]),
        "summary": summary_lines,
        "risk_notes": risk_notes,
    }