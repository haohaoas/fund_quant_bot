from typing import List, Dict, Any, Optional, Tuple
import math
import time
import os
import json
from datetime import datetime

import akshare as ak
import requests

# ==== 数据源开关 ====
# 默认：优先尝试非东方财富的数据源（如果 AkShare 支持），减少 RemoteDisconnected/限流。
# 你也可以通过环境变量强制：
#   MARKET_ETF_SOURCE=sina  -> 优先新浪
#   MARKET_ETF_SOURCE=em    -> 强制东方财富
MARKET_ETF_SOURCE = str(os.environ.get("MARKET_ETF_SOURCE", "sina")).strip().lower()

# ===== ABC 模式：
# (A) 先筛候选：只看资金流 Top N（行业 + 概念 合并）
# (B) 再打分：资金流强度 + 趋势（MA/RSI/20日收益）
# (C) 新闻只做“加权”：不用于生成候选池，仅作为后续 AI 的权重因子

# 默认：先取多少个“原始板块”做候选池（行业+概念合并后按净流入排序）
ABC_RAW_TOP_N_DEFAULT = 60

# 默认：主题板块（聚合后）输出多少个
ABC_THEME_TOP_N_DEFAULT = 8

# 行业 → 用来匹配 ETF 名称/指数 名称的关键词（可按自己喜好扩）
SECTOR_KEYWORDS = {
    "半导体芯片": ["半导体", "芯片"],
    "机器人": ["机器人", "智能制造"],
    "创新药": ["创新药", "生物医药", "医药", "生物科技"],
    "新能源": ["新能源", "光伏", "风能", "锂电", "电池", "储能"],
    "消费": ["消费", "消费50"],
    "白酒": ["白酒", "酒"],
    "先进制造": ["先进制造", "制造", "装备"],
    "通信设备": ["通信", "5G"],
}
# ===== ETF 候选池（非东财兜底） =====
# 当 AkShare 无法提供 ETF 全市场列表（如 fund_etf_spot_sina 不存在）时，
# 我们用一个“常用 ETF 代码池”来做候选筛选，然后通过腾讯行情拉实时价与涨跌幅。
# 你可以按自己盯盘习惯继续补充/替换。
SECTOR_ETF_CODE_POOL: Dict[str, List[str]] = {
    # 半导体/芯片
    "半导体芯片": ["512480", "159995", "588000", "512760"],
    # 机器人/智能制造
    "机器人": ["159770", "159869", "515070"],
    # 创新药/医药
    "创新药": ["159992", "512290", "512010", "159928"],
    # 新能源/光伏/电池/储能
    "新能源": ["516160", "515030", "159806", "159755", "159867"],
    # 先进制造/军工/高端装备（偏宽泛）
    "先进制造": ["512660", "512800", "159872"],
    # 通信/5G/6G/算力/数据中心
    "通信设备": ["515050", "159994", "515880"],
    # 消费
    "消费": ["510150", "159915", "159928"],
    # 白酒
    "白酒": ["512690", "161725"],
}


def _to_tencent_symbol(code: str) -> str:
    """把 6 位代码转成腾讯行情 market 前缀格式：sh/sz + code"""
    c = str(code).strip()
    if not c:
        return ""
    # 深市 ETF/LOF 常见：15/16/18 开头
    if c.startswith(("15", "16", "18")):
        return f"sz{c}"
    # 其余默认上证
    return f"sh{c}"


def _fetch_tencent_quotes(codes: List[str], timeout: float = 6.0) -> Dict[str, Dict[str, Any]]:
    """通过腾讯行情（qt.gtimg.cn）拉实时价。
    返回：{code: {"price": float, "pct": float, "symbol": str}}
    """
    cc = [str(x).strip() for x in (codes or []) if str(x).strip()]
    if not cc:
        return {}

    symbols = [_to_tencent_symbol(c) for c in cc]
    url = "https://qt.gtimg.cn/?q=" + ",".join(symbols)

    # 显式禁用代理（避免系统代理导致 ProxyError）
    proxies = {"http": None, "https": None}

    r = requests.get(url, timeout=timeout, proxies=proxies)
    r.encoding = "gbk"  # 腾讯返回常见为 GBK
    text = r.text or ""

    out: Dict[str, Dict[str, Any]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or "=\"" not in line:
            continue
        try:
            # v_sh510300="...";  v_sz159995="..."
            left, right = line.split("=\"", 1)
            sym = left.split("v_", 1)[-1].strip()  # sh510300 / sz159995
            payload = right.split("\"", 1)[0]
            parts = payload.split("~")
            # 经验字段：parts[3] 当前价；parts[4] 昨收
            price = float(parts[3]) if len(parts) > 3 and parts[3] else float("nan")
            prev_close = float(parts[4]) if len(parts) > 4 and parts[4] else float("nan")
            if prev_close == prev_close and price == price and prev_close != 0:
                pct = (price / prev_close - 1.0) * 100.0
            else:
                pct = 0.0
            code = sym[-6:]
            out[code] = {"price": float(price), "pct": float(pct), "symbol": sym}
        except Exception:
            continue

    return out


def _get_etf_candidates_from_pool(sector: str, max_per_sector: int) -> List[str]:
    pool = SECTOR_ETF_CODE_POOL.get(sector) or []
    seen = set()
    out: List[str] = []
    for x in pool:
        c = str(x).strip()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(c)
        if len(out) >= max(1, int(max_per_sector)):
            break
    return out

# ===== 资金流原始行业 -> 我们关注的主题板块（用于ETF匹配） =====
# 说明：AkShare 行业名称可能是“专用设备/半导体/通信设备/光学光电子/软件开发/计算机设备/生物制品”等。
# 我们把它们归并到少数主题板块，便于后续用关键词去匹配 ETF。

THEME_ORDER = [
    "半导体芯片",
    "通信设备",
    "机器人",
    "创新药",
    "新能源",
    "先进制造",
    "消费",
    "白酒",
]

# 每个主题对应：行业名命中关键词（只要原始行业包含其中任意词，就归到该主题）
THEME_MATCH_RULES = {
    "半导体芯片": ["半导体", "芯片", "集成电路", "存储", "电子元件", "元件", "光刻", "封测"],
    # 通信/光模块/CPO/算力相关经常分散在“通信设备/光学光电子/互联网服务/软件开发”等行业里
    "通信设备": ["通信", "5G", "6G", "光模块", "CPO", "光学", "光电子", "光通信", "服务器", "算力", "数据中心"],
    "机器人": ["机器人", "自动化", "工业自动化", "人形", "工业母机", "机床"],
    "创新药": ["创新药", "医药", "生物", "生物制品", "医疗", "医疗器械", "CXO", "疫苗"],
    "新能源": ["新能源", "光伏", "风电", "锂电", "电池", "储能", "充电桩", "电源设备", "能源金属"],
    "先进制造": ["高端装备", "装备", "专用设备", "通用设备", "军工", "航空", "航天", "船舶", "工程机械"],
    "消费": ["消费", "食品", "饮料", "家电", "零售", "旅游", "酒店", "纺织", "服装"],
    "白酒": ["白酒", "酒类"],
}


def _map_raw_sector_to_theme(raw_sector: str) -> str:
    """把 AkShare 返回的原始行业/板块名归并到主题；不命中则返回空字符串。"""
    if raw_sector is None:
        return ""
    s = str(raw_sector).strip()
    if not s:
        return ""

    # 先按固定顺序匹配（避免同一个行业命中多个主题时随机归类）
    for theme in THEME_ORDER:
        kws = THEME_MATCH_RULES.get(theme, [])
        for kw in kws:
            if kw and kw in s:
                return theme
    return ""


from typing import Any as _Any, List as _List, Dict as _Dict


def _aggregate_sector_flows_to_themes(sector_flows: _List[_Dict[str, _Any]], top_n: int = 8) -> _List[_Dict[str, _Any]]:
    """把原始行业资金流聚合到主题板块：
    - today_net_inflow：累加
    - today_pct：用净流入作为权重的加权平均（没有就按0处理）

    返回结构仍保持和 scan_hot_sectors_by_flow 一致：
      {"sector": 主题名, "today_net_inflow": ..., "today_pct": ...}
    """

    buckets: Dict[str, Dict[str, float]] = {}

    for x in sector_flows or []:
        raw = x.get("sector")
        theme = _map_raw_sector_to_theme(raw)
        if not theme:
            continue

        inflow = _safe_float(x.get("today_net_inflow"), 0.0)
        pct = _safe_float(x.get("today_pct"), 0.0)

        if theme not in buckets:
            buckets[theme] = {"inflow": 0.0, "w_pct": 0.0, "w": 0.0}

        buckets[theme]["inflow"] += inflow
        # 以净流入绝对值作为权重更稳（避免负流入导致权重抵消）
        w = abs(inflow) if inflow != 0 else 1.0
        buckets[theme]["w_pct"] += pct * w
        buckets[theme]["w"] += w

    out: List[Dict[str, Any]] = []
    for theme, b in buckets.items():
        w = b["w"] or 1.0
        out.append(
            {
                "sector": theme,
                "today_net_inflow": float(b["inflow"]),
                "today_pct": float(b["w_pct"] / w),
                "raw_sector": theme,  # 主题扫描下 raw 就用主题本身（保持字段存在）
            }
        )

    out.sort(key=lambda x: x["today_net_inflow"], reverse=True)
    return out[:top_n]


# 简单缓存：减少频繁请求导致的超时/断连
_FLOW_CACHE = {
    "ts": 0.0,
    "data": None,
}

# 缓存有效期（秒），默认 60 秒
FLOW_CACHE_TTL = 60

# ======== 近N个交易日资金流快照（持久化） ========
# 说明：AkShare 的“历史资金流”接口在不同环境下可能不稳定/不统一；
# 所以这里用“每天运行一次脚本时把当日热点行业资金流快照落盘”的方式，稳定获得近3日上下文。
# 文件路径可通过环境变量覆盖。
SECTOR_FLOW_HISTORY_PATH = os.getenv(
    "SECTOR_FLOW_HISTORY_PATH",
    os.path.join(".cache", "sector_flow_history.json"),
)


def _load_sector_flow_history() -> List[Dict[str, Any]]:
    try:
        if not os.path.exists(SECTOR_FLOW_HISTORY_PATH):
            return []
        with open(SECTOR_FLOW_HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or []
    except Exception:
        return []


def _save_sector_flow_history(hist: List[Dict[str, Any]]) -> None:
    try:
        os.makedirs(os.path.dirname(SECTOR_FLOW_HISTORY_PATH), exist_ok=True)
        with open(SECTOR_FLOW_HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(hist, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _today_str() -> str:
    # 以本地时间为准（你在 +0800），用于“交易日快照”的键
    return datetime.now().strftime("%Y-%m-%d")


def record_today_sector_flow_snapshot(raw_sectors: List[Dict[str, Any]]) -> None:
    """把今天的原始行业资金流快照写入本地历史（最多保留最近 20 天）。"""
    if not raw_sectors:
        return

    day = _today_str()
    hist = _load_sector_flow_history()

    # 如果今天已经写过，就更新；否则追加
    updated = False
    for item in hist:
        if item.get("date") == day:
            item["raw_sectors"] = raw_sectors
            # 同时缓存一份主题聚合（方便模型直接用）
            item["themes"] = _aggregate_sector_flows_to_themes(raw_sectors, top_n=20)
            updated = True
            break

    if not updated:
        hist.append(
            {
                "date": day,
                "raw_sectors": raw_sectors,
                "themes": _aggregate_sector_flows_to_themes(raw_sectors, top_n=20),
            }
        )

    # 按日期排序、截断
    hist = sorted(hist, key=lambda x: str(x.get("date", "")))[-20:]
    _save_sector_flow_history(hist)


def get_last_n_days_sector_flow(n: int = 3) -> List[Dict[str, Any]]:
    """返回最近 n 个“已记录的交易日快照”（最新在前）。"""
    hist = _load_sector_flow_history()
    if not hist:
        return []
    hist = sorted(hist, key=lambda x: str(x.get("date", "")), reverse=True)
    return hist[: max(1, int(n))]


def build_market_tendency_context(days: int = 3) -> Dict[str, Any]:
    """给模型的“市场倾向”上下文：近 N 个交易日主题资金流 + 今日原始热点。"""
    snaps = get_last_n_days_sector_flow(days)
    ctx = {
        "days": days,
        "snapshots": snaps,
    }
    return ctx


def _safe_float(v, default=0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return default
        return float(str(v).replace(",", ""))
    except Exception:
        return default


def _pick_col(df, candidates):
    """从 df.columns 里按候选关键字挑最可能的列名。"""
    cols = [str(c) for c in df.columns]
    for kw in candidates:
        for c in cols:
            if kw in c:
                return c
    return None


# ====== 名称归一化 + BK代码查找（修复 “光伏概念/卫星导航 -> (--)”）======

def _norm_board_name(x: str) -> str:
    """规范化板块名，提升与 name_em 映射表的命中率。"""
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    # 去掉常见尾缀/空白
    s = s.replace(" ", "").replace("\u200b", "").replace("\ufeff", "")
    # 统一括号
    s = s.replace("（", "(").replace("）", ")")
    # 常见尾缀：概念/板块
    for suf in ("概念", "板块"):
        if s.endswith(suf) and len(s) > len(suf):
            s = s[: -len(suf)]
    return s


def _lookup_board_code(name_to_code: Dict[str, str], name: str) -> Optional[str]:
    """从 name->code 映射里找 code（更鲁棒）"""
    if not name_to_code:
        return None
    raw = str(name or "").strip()
    if not raw:
        return None

    # 1) 直接命中
    if raw in name_to_code:
        return name_to_code.get(raw)

    # 2) 归一化后命中
    n = _norm_board_name(raw)
    if n in name_to_code:
        return name_to_code.get(n)

    # 3) 反向：映射表里有 'xxx概念' / 其它变体
    for k, v in name_to_code.items():
        if not k:
            continue
        if _norm_board_name(k) == n:
            return v

    # 4) 模糊包含：短名包含在长名里（只作为兜底）
    # 例如：'CPO' vs '共封装光学(CPO)'
    for k, v in name_to_code.items():
        kk = str(k)
        if not kk:
            continue
        if n and (n in _norm_board_name(kk) or _norm_board_name(kk) in n):
            return v

    return None


def _get_sector_flow_rank_df():
    """尽量用 AkShare 的板块/行业资金流接口拿到 DataFrame。"""
    candidates = [
        ("stock_sector_fund_flow_rank", {}),
        ("stock_sector_fund_flow_summary", {}),
        ("stock_fund_flow_industry", {}),
        ("stock_sector_fund_flow_hist", {}),
    ]

    last_err = None
    for fn_name, kwargs in candidates:
        fn = getattr(ak, fn_name, None)
        if not fn:
            continue
        try:
            df = fn(**kwargs)
            if df is not None and len(getattr(df, "columns", [])) > 0:
                return df
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    raise RuntimeError("未找到可用的行业/板块资金流接口")


# === ABC 相关辅助函数 ===

def _get_industry_flow_rank_df() -> Any:
    """行业板块资金流榜（偏实时）。"""
    fn = getattr(ak, "stock_sector_fund_flow_rank", None)
    if fn is None:
        return _get_sector_flow_rank_df()
    return fn()


def _get_concept_flow_rank_df() -> Any:
    """概念板块资金流榜。"""
    fn = getattr(ak, "stock_fund_flow_concept", None) or getattr(ak, "stock_fund_flow_concept_em", None)
    if fn is None:
        raise RuntimeError("未找到可用的概念板块资金流接口")
    return fn()


def _build_board_name_to_code_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    """构建 行业/概念 板块名称 -> 板块代码 的映射，用于后续查K线。"""
    ind_map: Dict[str, str] = {}
    con_map: Dict[str, str] = {}

    try:
        ind = ak.stock_board_industry_name_em()
        if ind is not None and len(ind) > 0 and "板块名称" in ind.columns and "板块代码" in ind.columns:
            for _, r in ind.iterrows():
                name = str(r.get("板块名称", "")).strip()
                code = str(r.get("板块代码", "")).strip()
                if name and code:
                    ind_map[name] = code
                    ind_map[_norm_board_name(name)] = code
    except Exception:
        pass

    try:
        con = ak.stock_board_concept_name_em()
        if con is not None and len(con) > 0 and "板块名称" in con.columns and "板块代码" in con.columns:
            for _, r in con.iterrows():
                name = str(r.get("板块名称", "")).strip()
                code = str(r.get("板块代码", "")).strip()
                if name and code:
                    con_map[name] = code
                    con_map[_norm_board_name(name)] = code
    except Exception:
        pass

    return ind_map, con_map


def _calc_rsi14(closes: List[float]) -> float:
    """简单 RSI14（不依赖 numpy）。输入 closes 为按时间升序的收盘价序列。"""
    if not closes or len(closes) < 15:
        return 50.0
    gains = 0.0
    losses = 0.0
    # 取最近14个变化
    for i in range(len(closes) - 14, len(closes)):
        diff = closes[i] - closes[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses += (-diff)
    avg_gain = gains / 14.0
    avg_loss = losses / 14.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return float(max(0.0, min(100.0, rsi)))


def _mean(xs: List[float]) -> float:
    if not xs:
        return 0.0
    return float(sum(xs) / float(len(xs)))


def _board_kline_features(board_type: str, board_code: str) -> Optional[Dict[str, Any]]:
    """根据板块代码拉取K线并计算趋势特征。board_type: 'industry' | 'concept'"""
    try:
        if board_type == "industry":
            k = ak.stock_board_industry_hist_em(symbol=board_code)
        else:
            k = ak.stock_board_concept_hist_em(symbol=board_code)
    except Exception:
        return None

    if k is None or len(k) < 25:
        return None

    close_col = "收盘" if "收盘" in k.columns else ("收盘价" if "收盘价" in k.columns else None)
    if close_col is None:
        return None

    closes = [float(x) for x in k[close_col].tolist() if x is not None]
    if len(closes) < 25:
        return None

    c0 = closes[-1]
    c1 = closes[-2]
    ret1 = (c0 / c1 - 1.0) if c1 != 0 else 0.0

    def _ret(n: int) -> float:
        if len(closes) <= n:
            return 0.0
        base = closes[-1 - n]
        return (c0 / base - 1.0) if base != 0 else 0.0

    ret5 = _ret(5)
    ret20 = _ret(20)

    ma5 = _mean(closes[-5:])
    ma20 = _mean(closes[-20:])
    rsi14 = _calc_rsi14(closes)

    return {
        "ret1": float(ret1),
        "ret5": float(ret5),
        "ret20": float(ret20),
        "ma5": float(ma5),
        "ma20": float(ma20),
        "rsi14": float(rsi14),
        "close": float(c0),
    }


def _flow_to_yuan_if_needed(v: Any, assume_unit_yi: bool) -> float:
    """把资金流统一成“元”。
    - 行业榜一般已经是元
    - 概念榜（stock_fund_flow_concept）很多版本返回的净额是“亿”为单位的数值
    """
    x = _safe_float(v, 0.0)
    if assume_unit_yi:
        return float(x * 1e8)
    return float(x)


def _percentile_rank(values: List[float], x: float) -> float:
    """返回 x 在 values 中的分位（0~1），values 允许重复。"""
    if not values:
        return 0.0
    cnt = 0
    for v in values:
        if v <= x:
            cnt += 1
    return float(cnt / float(len(values)))


def _trend_score_from_features(feat: Optional[Dict[str, Any]]) -> float:
    """把K线特征压成 0~100 的趋势分（不追求完美，只求稳定）。"""
    if not feat:
        return 50.0
    close = feat.get("close", 0.0)
    ma5 = feat.get("ma5", 0.0)
    ma20 = feat.get("ma20", 0.0)
    ret20 = feat.get("ret20", 0.0)
    rsi = feat.get("rsi14", 50.0)

    score = 50.0
    if ma5 > ma20:
        score += 12.0
    else:
        score -= 8.0

    if close > ma20:
        score += 10.0
    else:
        score -= 10.0

    if ret20 > 0:
        score += min(18.0, ret20 * 200.0)
    else:
        score += max(-18.0, ret20 * 200.0)

    if 45.0 <= rsi <= 70.0:
        score += 10.0
    elif rsi > 80.0:
        score -= 8.0
    elif rsi < 35.0:
        score -= 6.0

    return float(max(0.0, min(100.0, score)))


def _scan_boards_abc(raw_top_n: int = ABC_RAW_TOP_N_DEFAULT) -> List[Dict[str, Any]]:
    """(A)(B) 核心：资金流TopN + 趋势打分"""
    raw_top_n = max(10, int(raw_top_n))

    ind_map, con_map = _build_board_name_to_code_maps()
    boards: List[Dict[str, Any]] = []

    # 1) 行业榜
    try:
        df_ind = _get_industry_flow_rank_df()
        name_col = _pick_col(df_ind, ["名称", "行业", "板块", "概念"]) or "名称"
        inflow_col = _pick_col(df_ind, ["今日主力净流入-净额", "主力净流入", "净流入", "资金净流入"])
        pct_col = _pick_col(df_ind, ["今日涨跌幅", "涨跌幅", "涨跌"])
        if inflow_col is None:
            inflow_col = _pick_col(df_ind, ["净额"])
        tmp = df_ind.copy()
        if inflow_col is not None:
            tmp["_inflow"] = tmp[inflow_col].apply(lambda x: _flow_to_yuan_if_needed(x, assume_unit_yi=False))
        else:
            tmp["_inflow"] = 0.0
        if pct_col is not None:
            tmp["_pct"] = tmp[pct_col].apply(_safe_float)
        else:
            tmp["_pct"] = 0.0

        tmp = tmp.sort_values("_inflow", ascending=False).head(raw_top_n)
        for _, r in tmp.iterrows():
            nm = str(r.get(name_col, "")).strip()
            if not nm:
                continue
            code = _lookup_board_code(ind_map, nm)
            boards.append(
                {
                    "board_name": nm,
                    "board_type": "industry",
                    "board_code": code,
                    "today_net_inflow_yuan": float(r.get("_inflow", 0.0)),
                    "today_pct": float(r.get("_pct", 0.0)),
                }
            )
    except Exception:
        pass

    # 2) 概念榜
    try:
        df_con = _get_concept_flow_rank_df()
        name_col = _pick_col(df_con, ["行业", "名称", "概念", "板块"]) or "行业"
        inflow_col = _pick_col(df_con, ["净额", "主力净流入", "净流入"]) or "净额"
        pct_col = _pick_col(df_con, ["行业-涨跌幅", "涨跌幅", "涨跌"])

        tmp = df_con.copy()
        tmp["_inflow"] = tmp[inflow_col].apply(lambda x: _flow_to_yuan_if_needed(x, assume_unit_yi=True))
        if pct_col is not None:
            tmp["_pct"] = tmp[pct_col].apply(_safe_float)
        else:
            tmp["_pct"] = 0.0

        tmp = tmp.sort_values("_inflow", ascending=False).head(raw_top_n)
        for _, r in tmp.iterrows():
            nm = str(r.get(name_col, "")).strip()
            if not nm:
                continue
            code = _lookup_board_code(con_map, nm)
            boards.append(
                {
                    "board_name": nm,
                    "board_type": "concept",
                    "board_code": code,
                    "today_net_inflow_yuan": float(r.get("_inflow", 0.0)),
                    "today_pct": float(r.get("_pct", 0.0)),
                }
            )
    except Exception:
        pass

    if not boards:
        return []

    boards.sort(key=lambda x: x.get("today_net_inflow_yuan", 0.0), reverse=True)
    boards = boards[:raw_top_n]

    inflows = [float(b.get("today_net_inflow_yuan", 0.0)) for b in boards]

    for b in boards:
        inflow = float(b.get("today_net_inflow_yuan", 0.0))
        flow_score = _percentile_rank(inflows, inflow) * 100.0

        code = b.get("board_code")
        feat = _board_kline_features(b.get("board_type", "industry"), code) if code else None
        trend_score = _trend_score_from_features(feat)

        total_score = 0.60 * flow_score + 0.40 * trend_score

        b["flow_score"] = float(flow_score)
        b["trend_score"] = float(trend_score)
        b["total_score"] = float(total_score)
        b["kline"] = feat

    return boards


def _aggregate_boards_to_themes_abc(boards: List[Dict[str, Any]], theme_top_n: int = ABC_THEME_TOP_N_DEFAULT) -> List[Dict[str, Any]]:
    """把 (A)(B) 的原始板块聚合到主题板块，用于ETF匹配。"""
    buckets: Dict[str, Dict[str, Any]] = {}

    for b in boards or []:
        raw_name = b.get("board_name")
        theme = _map_raw_sector_to_theme(raw_name)
        if not theme:
            continue

        inflow = float(b.get("today_net_inflow_yuan", 0.0))
        pct = float(b.get("today_pct", 0.0))
        flow_score = float(b.get("flow_score", 50.0))
        trend_score = float(b.get("trend_score", 50.0))
        total_score = float(b.get("total_score", 50.0))

        if theme not in buckets:
            buckets[theme] = {
                "inflow": 0.0,
                "w_pct": 0.0,
                "w": 0.0,
                "flow_score": 0.0,
                "trend_score": 0.0,
                "total_score": 0.0,
                "n": 0,
                "samples": [],
            }

        w = abs(inflow) if inflow != 0 else 1.0
        buckets[theme]["inflow"] += inflow
        buckets[theme]["w_pct"] += pct * w
        buckets[theme]["w"] += w

        buckets[theme]["flow_score"] += flow_score
        buckets[theme]["trend_score"] += trend_score
        buckets[theme]["total_score"] += total_score
        buckets[theme]["n"] += 1

        if len(buckets[theme]["samples"]) < 6:
            buckets[theme]["samples"].append(
                {
                    "board_name": b.get("board_name"),
                    "board_type": b.get("board_type"),
                    "board_code": b.get("board_code"),
                    "today_net_inflow_yuan": inflow,
                    "today_pct": pct,
                    "flow_score": flow_score,
                    "trend_score": trend_score,
                    "total_score": total_score,
                }
            )

    out: List[Dict[str, Any]] = []
    for theme, b in buckets.items():
        n = float(b.get("n") or 1)
        w = float(b.get("w") or 1.0)
        out.append(
            {
                "sector": theme,
                "today_net_inflow": float(b.get("inflow", 0.0)),
                "today_pct": float(b.get("w_pct", 0.0) / w),
                "flow_score": float(b.get("flow_score", 0.0) / n),
                "trend_score": float(b.get("trend_score", 0.0) / n),
                "total_score": float(b.get("total_score", 0.0) / n),
                "raw_sector": theme,
                "samples": b.get("samples", []),
            }
        )

    out.sort(key=lambda x: (x.get("total_score", 0.0), x.get("today_net_inflow", 0.0)), reverse=True)
    return out[: max(1, int(theme_top_n))]


def _normalize_sector_flow(df, top_n: int = 8) -> List[Dict[str, Any]]:
    """把不同接口返回的 df 统一映射成：
    {"sector": 名称, "today_net_inflow": 净流入(数值), "today_pct": 涨跌幅(可选)}
    """
    sector_col = _pick_col(df, ["行业", "板块", "概念", "名称", "题材"])
    inflow_col = _pick_col(df, ["主力净流入", "净流入", "资金净流入", "今日净流入"])
    pct_col = _pick_col(df, ["涨跌幅", "涨跌"])

    if not sector_col or not inflow_col:
        print("[market_scanner] 资金流 df 列名无法识别：", list(df.columns))
        return []

    out: List[Dict[str, Any]] = []
    tmp = df.copy()
    tmp["_inflow"] = tmp[inflow_col].apply(_safe_float)

    def _parse_inflow(v):
        if v is None:
            return 0.0
        s = str(v).strip().replace(",", "").replace(" ", "")
        if not s or s in {"--", "-", "nan", "None"}:
            return 0.0
        s = s.replace("亿元", "亿").replace("万元", "万")
        try:
            if "亿" in s:
                num = float(s.replace("亿", ""))
                return num * 1e8
            if "万" in s:
                num = float(s.replace("万", ""))
                return num * 1e4
            return float(s)
        except Exception:
            return _safe_float(v)

    tmp["_inflow"] = tmp[inflow_col].apply(_parse_inflow)

    if pct_col:
        tmp["_pct"] = tmp[pct_col].apply(_safe_float)
    else:
        tmp["_pct"] = 0.0

    tmp = tmp.sort_values("_inflow", ascending=False).head(top_n)

    def _clean_sector_name(x) -> str:
        t = str(x).strip()
        t = t.replace("\u200b", "").replace("\ufeff", "").strip("'\" ")
        if not t:
            return ""
        if t in {",", "，", ".", "。", "-", "--", "nan", "None"}:
            return ""
        only_punc = True
        for ch in t:
            if ch.isalnum() or ("\u4e00" <= ch <= "\u9fff"):
                only_punc = False
                break
        if only_punc:
            return ""
        return t

    for _, row in tmp.iterrows():
        sec_name = _clean_sector_name(row[sector_col])
        if not sec_name:
            continue
        out.append(
            {
                "sector": sec_name,
                "today_net_inflow": float(row["_inflow"]),
                "today_pct": float(row["_pct"]),
            }
        )

    return out


def _get_etf_spot_df() -> Any:
    """获取 ETF 实时行情 df（默认不使用东财）。

    行为：
    - MARKET_ETF_SOURCE=em    -> 强制使用东财（不推荐）
    - 默认/ MARKET_ETF_SOURCE=sina -> 只尝试新浪；失败就抛错（上层会跳过 ETF 扫描）
    """
    # 强制东财（你要不用东财就别开）
    if MARKET_ETF_SOURCE == "em":
        return ak.fund_etf_spot_em()

    # 默认/强制新浪：只走新浪，不允许回退东财
    fn_sina = getattr(ak, "fund_etf_spot_sina", None)
    if fn_sina is None:
        raise RuntimeError(
            "akshare 未提供 fund_etf_spot_sina；已禁用东财回退，所以无法获取 ETF 实时行情。"
            "（解决：升级 akshare 或换其它数据源实现 ETF 行情）"
        )

    df = fn_sina()
    if df is None or len(getattr(df, "columns", [])) == 0:
        raise RuntimeError("fund_etf_spot_sina 返回空数据")

    return df


def scan_hot_sectors_by_flow(top_n: int = 8) -> List[Dict[str, Any]]:
    """ABC 版本（默认启用）。失败自动降级到 ETF 热度近似 fallback。"""

    now = time.time()

    if _FLOW_CACHE["data"] is not None and (now - float(_FLOW_CACHE["ts"])) <= FLOW_CACHE_TTL:
        cached = []
        for x in _FLOW_CACHE["data"]:
            if not isinstance(x, dict):
                continue
            sec = str(x.get("sector", "")).strip()
            if not sec:
                continue
            cached.append(x)
        return cached[:top_n]

    # 2) ABC：资金流TopN + 趋势打分
    try:
        raw_boards = _scan_boards_abc(raw_top_n=max(ABC_RAW_TOP_N_DEFAULT, int(top_n) * 6))
        if raw_boards:
            themes = _aggregate_boards_to_themes_abc(raw_boards, theme_top_n=top_n)
            if themes:
                _FLOW_CACHE["ts"] = now
                _FLOW_CACHE["data"] = themes
                record_today_sector_flow_snapshot(themes)
                return themes[:top_n]
    except Exception as e:
        print(f"[market_scanner] ABC 获取资金流/趋势失败，降级为 ETF 热度近似：{e}")

    # 3) fallback：ETF 热度近似
    try:
        df = _get_etf_spot_df()
    except Exception as e:
        print(f"[market_scanner] 跳过 ETF 扫描（未获取到 ETF 实时行情）：{e}")
        return []

    name_col = None
    pct_col = None
    amount_col = None
    index_col = None

    for c in df.columns:
        if c in ("名称", "基金简称", "fund_name"):
            name_col = c
        if "涨跌幅" in str(c):
            pct_col = c
        if "成交额" in str(c) or "成交量" in str(c):
            amount_col = c
        if "跟踪" in str(c) or "指数" in str(c):
            index_col = c

    if not name_col:
        print("[market_scanner] 无法识别 ETF 名称列, df.columns =", df.columns)
        return []

    df[name_col] = df[name_col].astype(str)
    if index_col:
        df[index_col] = df[index_col].astype(str)
    else:
        df[index_col] = ""

    if amount_col:
        df["_amt"] = df[amount_col].apply(_safe_float)
    else:
        df["_amt"] = 1.0

    if pct_col:
        df["_pct"] = df[pct_col].apply(_safe_float)
    else:
        df["_pct"] = 0.0

    sector_list: List[Dict[str, Any]] = []

    for sector_name, kws in SECTOR_KEYWORDS.items():
        mask = False
        for kw in kws:
            mask = (
                mask
                | df[name_col].str.contains(kw, case=False, na=False)
                | df[index_col].str.contains(kw, case=False, na=False)
            )

        sub = df[mask].copy()
        if sub.empty:
            continue

        sub["_heat"] = sub["_amt"] * sub["_pct"].abs()

        total_heat = float(sub["_heat"].sum())
        avg_pct = float(sub["_pct"].mean()) if len(sub) > 0 else 0.0

        sector_list.append(
            {
                "sector": sector_name,
                "today_net_inflow": total_heat,
                "today_pct": avg_pct,
                "flow_score": None,
                "trend_score": None,
                "total_score": None,
                "samples": [],
            }
        )

    sector_list.sort(key=lambda x: x["today_net_inflow"], reverse=True)

    _FLOW_CACHE["ts"] = now
    _FLOW_CACHE["data"] = sector_list
    record_today_sector_flow_snapshot(sector_list)
    return sector_list[:top_n]


def scan_market_etf_candidates(
    top_sectors: List[Dict[str, Any]],
    max_per_sector: int = 3,
) -> List[Dict[str, Any]]:
    """
    根据热点行业列表，从 ETF 实时行情中筛出匹配的 ETF/基金。
    返回结构尽量贴近 run_fund_daily 里的 summary，方便直接交给 ai_picker。
    """
    if not top_sectors:
        return []

    try:
        df_etf = _get_etf_spot_df()
    except Exception as e:
        print(f"[market_scanner] ETF 全市场行情不可用，启用候选池+腾讯行情兜底：{e}")
        df_etf = None

    # === 兜底分支：不依赖 AkShare ETF 全市场列表（避免东财 / 也不要求 fund_etf_spot_sina） ===
    if df_etf is None:
        sector_to_codes: Dict[str, List[str]] = {}
        all_codes: List[str] = []

        for sec in top_sectors:
            sec_name = sec.get("sector")
            if not sec_name:
                continue
            codes = _get_etf_candidates_from_pool(sec_name, max_per_sector=max_per_sector)
            if not codes:
                continue
            sector_to_codes[sec_name] = codes
            all_codes.extend(codes)

        if not all_codes:
            return []

        quotes = _fetch_tencent_quotes(all_codes)
        candidates: List[Dict[str, Any]] = []

        for sec in top_sectors:
            sec_name = sec.get("sector")
            if not sec_name:
                continue
            for code in (sector_to_codes.get(sec_name) or []):
                q = quotes.get(code) or {}
                price = _safe_float(q.get("price"), 0.0)
                pct = _safe_float(q.get("pct"), 0.0)

                candidates.append(
                    {
                        "code": str(code),
                        "name": f"ETF_{code}",  # 想显示全名：后面我再给你加一个 code->name 映射表
                        "sector": sec_name,
                        "latest": {"price": price, "pct": pct, "time": None, "source": "tencent_quote"},
                        "quant": {},
                        "sector_view": {"score": None, "level": None, "comment": None},
                        "ai_decision": {},
                        "fund_profile": {"risk": "unknown"},
                        "meta": {
                            "from_market_scan": True,
                            "sector_today_net_inflow": sec.get("today_net_inflow"),
                            "sector_today_pct": sec.get("today_pct"),
                            "sector_flow_score": sec.get("flow_score"),
                            "sector_trend_score": sec.get("trend_score"),
                            "sector_total_score": sec.get("total_score"),
                            "sector_samples": sec.get("samples", []),
                            "sector_raw_name": sec.get("raw_sector", sec_name),
                            "market_tendency": build_market_tendency_context(days=3),
                        },
                    }
                )

        return candidates

    code_col = None
    name_col = None
    price_col = None
    pct_col = None
    index_col = None

    for c in df_etf.columns:
        if c in ("代码", "基金代码", "fund_code"):
            code_col = c
        if c in ("名称", "基金简称", "fund_name"):
            name_col = c
        if "最新价" in str(c) or "现价" in str(c):
            price_col = c
        if "涨跌幅" in str(c):
            pct_col = c
        if "跟踪" in str(c) or "指数" in str(c):
            index_col = c

    if not code_col or not name_col or not price_col:
        print("[market_scanner] ETF 列名不匹配，df_etf.columns=", df_etf.columns)
        return []

    df_etf[name_col] = df_etf[name_col].astype(str)
    if index_col:
        df_etf[index_col] = df_etf[index_col].astype(str)
    else:
        df_etf[index_col] = ""

    candidates: List[Dict[str, Any]] = []

    for sec in top_sectors:
        sec_name = sec["sector"]
        sec_keywords = SECTOR_KEYWORDS.get(sec_name, [sec_name])

        mask = False
        for kw in sec_keywords:
            mask = (
                mask
                | df_etf[name_col].str.contains(kw, case=False, na=False)
                | df_etf[index_col].str.contains(kw, case=False, na=False)
            )

        sub = df_etf[mask].copy()
        if sub.empty:
            continue

        amount_col = None
        for c in sub.columns:
            if "成交额" in str(c) or "成交量" in str(c):
                amount_col = c
                break
        if amount_col:
            sub["_amt"] = sub[amount_col].apply(_safe_float)
            sub = sub.sort_values("_amt", ascending=False)
        elif pct_col:
            sub["_pct_abs"] = sub[pct_col].apply(lambda x: abs(_safe_float(x)))
            sub = sub.sort_values("_pct_abs", ascending=False)

        sub = sub.head(max_per_sector)

        for _, row in sub.iterrows():
            code = str(row[code_col])
            name = str(row[name_col])
            price = _safe_float(row[price_col])
            pct = _safe_float(row[pct_col]) if pct_col else 0.0

            cand = {
                "code": code,
                "name": name,
                "sector": sec_name,
                "latest": {
                    "price": price,
                    "pct": pct,
                    "time": None,
                    "source": "etf_spot",
                },
                "quant": {},
                "sector_view": {
                    "score": None,
                    "level": None,
                    "comment": None,
                },
                "ai_decision": {},
                "fund_profile": {
                    "risk": "unknown",
                },
                "meta": {
                    "from_market_scan": True,
                    "sector_today_net_inflow": sec["today_net_inflow"],
                    "sector_today_pct": sec["today_pct"],
                    "sector_flow_score": sec.get("flow_score"),
                    "sector_trend_score": sec.get("trend_score"),
                    "sector_total_score": sec.get("total_score"),
                    "sector_samples": sec.get("samples", []),
                    "sector_raw_name": sec.get("raw_sector", sec_name),
                    "market_tendency": build_market_tendency_context(days=3),
                },
            }
            candidates.append(cand)

    return candidates


def scan_market_for_tomorrow(max_sectors: int = 8, max_funds_per_sector: int = 3) -> List[Dict[str, Any]]:
    """
    一键扫描：
      1. 找出今日资金流前 N 的行业
      2. 从 ETF 中筛出对应基金
    """
    raw_sectors = scan_hot_sectors_by_flow(top_n=max_sectors)
    if not raw_sectors:
        return []

    already_themes = True
    for x in raw_sectors:
        if not isinstance(x, dict) or "sector" not in x:
            already_themes = False
            break

    top_sectors = raw_sectors

    if not already_themes:
        themed_sectors = _aggregate_sector_flows_to_themes(raw_sectors, top_n=max_sectors)
        top_sectors = themed_sectors if themed_sectors else raw_sectors

    return scan_market_etf_candidates(top_sectors, max_per_sector=max_funds_per_sector)


if __name__ == "__main__":
    sectors = scan_hot_sectors_by_flow()
    print("热点行业：", sectors)
    cands = scan_market_for_tomorrow()
    print(f"候选基金数量：{len(cands)}")
    for c in cands[:10]:
        print(c["code"], c["name"], c["sector"], c["latest"]["pct"])