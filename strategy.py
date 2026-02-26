# strategy.py
"""
策略层（优化版）：
- 动态构建网格加仓点（基于历史波动 & 均线）
- 生成今日方向建议（BUY / HOLD / SELL）
- 同时给出仓位意图（ADD / REDUCE / KEEP）
- 提供结构化“策略状态”，供上层稳定使用
"""

from __future__ import annotations

from typing import Dict, Any, Optional, Tuple
from datetime import date

import numpy as np
import pandas as pd

from config import WATCH_FUNDS
from data import get_fund_history


# ============================================================
# 日级缓存：同一只基金当天只算一次网格
# ============================================================

_GRID_CACHE: Dict[Tuple[str, date], Dict[str, Any]] = {}


# ============================================================
# 构建动态网格
# ============================================================

def build_dynamic_grids(code: str) -> Dict[str, Any]:
    """
    针对某只基金，基于历史净值动态生成网格信息（日级稳定）：
    - base_price: 参考中枢价（默认 20 日均值）
    - grids: 向下网格价列表（float，降序）
    - volatility: 日波动率（仅用于网格密度）
    """
    today = date.today()
    cache_key = (code, today)

    if cache_key in _GRID_CACHE:
        return _GRID_CACHE[cache_key]

    cfg = WATCH_FUNDS.get(code, {})

    lookback = cfg.get("lookback_days", 60)
    default_step = cfg.get("grid_step_pct", 0.03)
    levels = cfg.get("grid_levels", 4)
    risk_level = cfg.get("risk_level", "balanced")

    df = get_fund_history(code, lookback_days=lookback)
    if df is None or df.empty or len(df) < 20:
        result = {
            "base_price": None,
            "grids": [],
            "volatility": None,
        }
        _GRID_CACHE[cache_key] = result
        return result

    closes = df["close"].astype(float)

    # 参考中枢：20 日均线
    base_price = closes.rolling(20).mean().iloc[-1]
    if np.isnan(base_price):
        base_price = float(closes.iloc[-1])

    # 日收益率 & 波动率
    rets = closes.pct_change().dropna()
    volatility = float(rets.std()) if len(rets) > 0 else None

    # 风险调节因子
    if risk_level == "aggressive":
        vol_factor = 1.5
    elif risk_level == "conservative":
        vol_factor = 0.8
    else:
        vol_factor = 1.0

    # 单格间距（防止极端波动导致网格失控）
    min_step = 0.02
    step_from_vol = volatility * vol_factor if volatility and volatility == volatility else default_step
    grid_step_pct = max(min_step, step_from_vol)

    grids = []
    for k in range(1, levels + 1):
        price_k = base_price * (1 - grid_step_pct * k)
        grids.append(round(float(price_k), 4))

    grids_sorted = sorted(grids, reverse=True)

    result = {
        "base_price": round(float(base_price), 4),
        "grids": grids_sorted,
        "volatility": volatility,
    }

    _GRID_CACHE[cache_key] = result
    return result


# ============================================================
# 今日信号生成
# ============================================================

def generate_today_signal(code: str, current_price: float) -> Dict[str, Any]:
    """
    根据当前价格 & 动态网格，生成今日策略信号。

    返回字段说明：
    - action: BUY / HOLD / SELL        （方向判断）
    - position_hint: ADD / REDUCE / KEEP（仓位意图）
    - hit_level: 命中的网格层级（None / 1 / 2 / ...）
    - price_vs_base_pct: 当前价相对中枢的偏离（%）
    - reason: 文本解释
    - grids / base_price: 展示与调试用
    """
    info = build_dynamic_grids(code)
    base = info["base_price"]
    grids = info["grids"]
    volatility = info["volatility"]

    if base is None or not grids:
        return {
            "action": "HOLD",
            "position_hint": "KEEP",
            "hit_level": None,
            "price_vs_base_pct": None,
            "reason": "历史数据不足，暂不产生网格信号",
            "grids": grids,
            "base_price": base,
        }

    # 当前价相对中枢的偏离
    price_vs_base_pct = (current_price / base - 1.0) * 100.0

    # 判断是否命中下方网格（低吸）
    hit_level = None
    hit_price = None
    for i, g in enumerate(grids):
        if current_price <= g:
            hit_level = i + 1
            hit_price = g
        else:
            break

    if hit_level is not None:
        action = "BUY"
        position_hint = "ADD"
        reason = (
            f"当前价格 {current_price:.4f} 低于第 {hit_level} 层网格价 "
            f"{hit_price:.4f}，符合分批低吸条件（动态网格策略）。"
        )
    else:
        # 止盈阈值：限制 volatility 的影响，防止极端行情推得过远
        if volatility and volatility == volatility:
            risk_vol = min(volatility, 0.05)  # 上限 5%
            up_threshold = base * (1 + 2 * risk_vol)
        else:
            up_threshold = base * 1.08  # 默认 8%

        if current_price >= up_threshold:
            action = "HOLD"
            position_hint = "REDUCE"
            reason = (
                f"当前价格 {current_price:.4f} 明显高于参考中枢 {base:.4f}，"
                f"可能处于阶段性高位，建议考虑部分止盈。"
            )
        else:
            action = "HOLD"
            position_hint = "KEEP"
            reason = (
                f"当前价格 {current_price:.4f} 接近中枢 {base:.4f}，"
                f"未触及加仓网格或止盈区间，建议观望。"
            )

    return {
        "action": action,
        "position_hint": position_hint,
        "hit_level": hit_level,
        "price_vs_base_pct": round(price_vs_base_pct, 2),
        "reason": reason,
        "grids": grids,
        "base_price": base,
    }