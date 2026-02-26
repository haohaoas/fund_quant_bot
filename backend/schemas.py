# backend/schemas.py
"""
API 数据结构定义（Pydantic Schemas）
用于：
- 请求校验
- 响应结构约束
- Swagger 文档
"""

from typing import Optional, List
from pydantic import BaseModel


# ======================
# Account
# ======================

class AccountOut(BaseModel):
    cash: float


class AccountUpdateIn(BaseModel):
    cash: float


# ======================
# Portfolio / Position
# ======================

class LatestPrice(BaseModel):
    price: float
    time: Optional[str] = None


class PositionOut(BaseModel):
    code: str
    shares: float
    cost: float

    # 动态字段（计算得出）
    latest: Optional[LatestPrice] = None
    market_value: Optional[float] = None
    pnl: Optional[float] = None
    weight: Optional[float] = None


class PortfolioOut(BaseModel):
    cash: float
    positions: List[PositionOut]


# ======================
# Trades
# ======================

class TradeIn(BaseModel):
    code: str
    action: str              # BUY / SELL / SIP / REDEEM
    price: float
    amount: Optional[float] = None
    shares: Optional[float] = None
    note: Optional[str] = None


class TradeOut(BaseModel):
    id: int
    ts: str
    code: str
    action: str
    amount: Optional[float]
    price: Optional[float]
    shares: Optional[float]
    note: Optional[str]


class TradeApplyResult(BaseModel):
    ok: bool
    action: str
    code: str
    shares_delta: float
    cash_delta: float
    new_shares: float
    new_cost: float


# ======================
# Recommendations
# ======================

class ActionSuggestion(BaseModel):
    code: str
    action: str                 # ADD / REDUCE / HOLD
    suggest_cash: Optional[float] = None
    suggest_ratio: Optional[float] = None
    reason: str


class RecommendationOut(BaseModel):
    generated_at: str

    account: AccountOut
    positions: List[PositionOut]
    actions: List[ActionSuggestion]

    # 快照（可选）
    strategy_snapshot: Optional[dict] = None