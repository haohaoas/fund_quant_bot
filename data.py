# data.py
"""
数据层：
- 优先使用基金实时估值（fund_value_estimation_em）
- 如果实时估值没找到该基金，再退回历史净值（fund_open_fund_info_em）

这样：
- 你看到的是“今天的涨跌 + 估值”
- 网格和买卖信号也基于今天的价格来判断
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, Optional

import akshare as ak
import pandas as pd
import requests


def _norm_fund_code(code: str) -> str:
    """简单规范化基金代码."""
    return code.strip()


@lru_cache(maxsize=1)
def _fund_name_map() -> Dict[str, str]:
    """缓存基金代码->基金名称映射，避免重复拉取."""
    try:
        df = ak.fund_name_em()
    except Exception as e:
        print(f"[data] 拉取基金名称列表失败: {e}")
        return {}

    if df is None or df.empty:
        return {}

    code_col = None
    name_col = None
    for c in ("基金代码", "代码", "symbol", "code"):
        if c in df.columns:
            code_col = c
            break
    for c in ("基金简称", "基金名称", "名称", "name"):
        if c in df.columns:
            name_col = c
            break

    if code_col is None or name_col is None:
        return {}

    out: Dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row.get(code_col, "")).strip()
        name = str(row.get(name_col, "")).strip()
        if not code or not name:
            continue
        out[code] = name
        if code.isdigit() and len(code) < 6:
            out[code.zfill(6)] = name
    return out


def get_fund_name(code: str) -> str:
    """根据代码获取基金名，失败返回空字符串."""
    c = _norm_fund_code(code)
    if not c:
        return ""
    mapping = _fund_name_map()
    if not mapping:
        return ""
    if c in mapping:
        return mapping[c]
    if c.isdigit():
        c6 = c.zfill(6)
        if c6 in mapping:
            return mapping[c6]
    return ""


# ========== 历史净值（日线），仍然用于网格计算 ==========
def get_fund_history(code: str, lookback_days: int = 180) -> pd.DataFrame:
    """
    获取基金最近 lookback_days 天的历史净值（日线）。
    返回 DataFrame: columns = [date, close]
    """
    fund_code = _norm_fund_code(code)

    try:
        # 你当前 akshare 版本大概率使用 symbol 参数
        raw = ak.fund_open_fund_info_em(
            symbol=fund_code,
            indicator="单位净值走势",
        )
    except TypeError as e:
        # 兼容另一种参数名写法
        try:
            raw = ak.fund_open_fund_info_em(
                fund=fund_code,
                indicator="单位净值走势",
            )
        except Exception as e2:
            print(f"[data] 获取基金 {fund_code} 历史净值失败: {e2}")
            return pd.DataFrame()
    except Exception as e:
        print(f"[data] 获取基金 {fund_code} 历史净值失败: {e}")
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    df = raw.copy()

    # 根据你本地 akshare 的字段名来适配，这里是最常见的一种
    if "净值日期" not in df.columns or "单位净值" not in df.columns:
        print(f"[data] 基金 {fund_code} 历史数据字段异常，列名: {df.columns}")
        return pd.DataFrame()

    df.rename(
        columns={
            "净值日期": "date",
            "单位净值": "close",
        },
        inplace=True,
    )

    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)

    if lookback_days is not None and lookback_days > 0:
        end_date = df["date"].max()
        start_date = end_date - timedelta(days=lookback_days * 2)
        df = df[df["date"] >= start_date].reset_index(drop=True)

    df["close"] = df["close"].astype(float)

    return df


# ========== 实时估值部分：今天买不买，看这个 ==========
def _get_realtime_estimation_row(code: str) -> Optional[Dict]:
    """
    使用 Eastmoney fundgz 接口获取单只基金的实时估值。
    如果失败则返回 None。
    """
    fund_code = _norm_fund_code(code)

    # fundgz 接口示例：https://fundgz.1234567.com.cn/js/008888.js?rt=1731822000000
    ts_ms = int(datetime.now().timestamp() * 1000)
    url = f"https://fundgz.1234567.com.cn/js/{fund_code}.js?rt={ts_ms}"

    try:
        resp = requests.get(url, timeout=5)
        text = resp.text.strip()
    except Exception as e:
        print(f"[data] 拉取实时估值失败 {fund_code}: {e}")
        return None

    if not text.startswith("jsonpgz(") or not text.endswith(");"):
        # 非预期格式，直接放弃，交给历史净值兜底
        print(f"[data] 实时估值返回格式异常 {fund_code}: {text[:80]}")
        return None

    # 去掉 jsonpgz( 和 结尾的 );
    json_str = text[len("jsonpgz("):-2]

    try:
        data = json.loads(json_str)
    except Exception as e:
        print(f"[data] 解析实时估值 JSON 失败 {fund_code}: {e}")
        return None

    # 估算净值 gsz，如果没有则退回 dwjz
    price = None
    gsz = data.get("gsz")
    dwjz = data.get("dwjz")
    try:
        if gsz not in (None, ""):
            price = float(gsz)
        elif dwjz not in (None, ""):
            price = float(dwjz)
    except Exception:
        price = None

    if price is None:
        return None

    # 涨跌幅：gszzl，单位本身就是百分比数值（字符串）
    pct = None
    pct_str = data.get("gszzl")
    try:
        if pct_str not in (None, ""):
            pct = float(pct_str)
    except Exception:
        pct = None

    # 时间：gztime
    ts = datetime.now()
    gztime = data.get("gztime")
    if isinstance(gztime, str) and gztime:
        try:
            # 格式一般为 "2025-11-18 15:00"
            ts = datetime.strptime(gztime, "%Y-%m-%d %H:%M")
        except Exception:
            pass

    return {
        "code": fund_code,
        "name": str(data.get("name") or "").strip() or get_fund_name(fund_code),
        "price": price,
        "pct": pct,        # 单位：百分比，例如 -1.23 表示 -1.23%
        "time": ts,
        "source": "realtime",
    }


def get_fund_latest_price(code: str) -> Optional[Dict]:
    """
    对外统一入口：
    1. 优先返回实时估值（今天涨跌）
    2. 如果实时估值失败，再退回最近一个历史净值
    返回字段统一为：
    {
        "price": float,
        "pct": float | None,   # 今日涨跌幅（百分比），历史净值时为 None
        "time": datetime,      # 对应价格的时间
        "source": "realtime" 或 "history"
    }
    """
    # 1) 尝试实时估值
    rt = _get_realtime_estimation_row(code)
    if rt is not None:
        return rt

    # 2) 回退到历史净值
    df = get_fund_history(code, lookback_days=30)
    if df is None or df.empty:
        print(f"[data] 无法获取 {code} 的历史净值作为兜底数据")
        return None

    last_row = df.iloc[-1]
    pct = None
    if len(df) >= 2:
        try:
            prev_close = float(df.iloc[-2]["close"])
            last_close = float(last_row["close"])
            if prev_close != 0:
                pct = (last_close - prev_close) / prev_close * 100
        except Exception:
            pct = None

    return {
        "code": _norm_fund_code(code),
        "name": get_fund_name(code),
        "price": float(last_row["close"]),
        "pct": pct,
        "time": last_row["date"],
        "source": "history",
    }
