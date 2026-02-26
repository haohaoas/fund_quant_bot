# sector_news.py
"""
板块新闻引擎（简化版）：
- 为每个板块抓一点新闻标题（这里先用东财的搜索接口做简单实现）
- 做一个非常简单的“情绪打分”（关键词规则）
- 带有内存缓存，避免重复请求

你可以在以后：
- 换成更复杂爬虫
- 或使用搜索引擎 + LLM 做摘要与情绪评分
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

import requests


@dataclass
class NewsItem:
    title: str
    url: str
    pub_time: datetime | None = None
    source: str | None = None


@dataclass
class NewsCache:
    data: Dict[Tuple[str, str], List[NewsItem]] = field(default_factory=dict)
    last_updated: Dict[Tuple[str, str], datetime] = field(default_factory=dict)


_news_cache = NewsCache()


def _eastmoney_search(keyword: str, days: int = 3) -> List[NewsItem]:
    """
    非严谨东财搜索示例，仅做 demo：
    - 实际 HTML/接口可能变化，必要时你自己再改。
    - 这里只尝试请求，不保证结构完全稳定。
    """
    base_url = "https://search-api-web.eastmoney.com/search/jsonp"
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    params = {
        "cb": "cb",
        "param": f"{{'uid':'','keyword':'{keyword}','type':1,'client':'web','pageindex':1,'pagesize':20}}"
    }

    try:
        resp = requests.get(base_url, params=params, timeout=5)
        text = resp.text

        # 粗暴地从 JSONP 中截取 JSON 部分
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1:
            return []

        import json
        data = json.loads(text[start:end+1])
        # 结构示例：data-> "data" -> "result" 列表
        items = []
        for row in data.get("data", {}).get("result", []):
            title = row.get("title", "")
            url = row.get("url", "")
            news_item = NewsItem(title=title, url=url, source="eastmoney", pub_time=None)
            items.append(news_item)
        return items
    except Exception as e:
        print(f"[news] 东财搜索异常 keyword={keyword}: {e}")
        return []


def fetch_sector_news(sector: str, keywords: List[str], days: int = 3, use_cache: bool = True) -> List[NewsItem]:
    """
    为某个板块按关键词抓新闻标题（简化版，多关键词合并+去重）
    """
    cache_key = (sector, str(days))
    now = datetime.now()

    # 简单缓存：如果 30 分钟内请求过，就直接返回旧缓存
    if use_cache and cache_key in _news_cache.data:
        last_ts = _news_cache.last_updated.get(cache_key)
        if last_ts and (now - last_ts) < timedelta(minutes=30):
            return _news_cache.data[cache_key]

    all_items: Dict[str, NewsItem] = {}
    for kw in keywords:
        items = _eastmoney_search(kw, days=days)
        for it in items:
            # 用标题去重
            key = it.title.strip()
            if key and key not in all_items:
                all_items[key] = it

    news_list = list(all_items.values())
    _news_cache.data[cache_key] = news_list
    _news_cache.last_updated[cache_key] = now

    return news_list


# ====== 简单情绪打分（关键词规则） ======


POSITIVE_WORDS = ["利好", "超预期", "大涨", "创新高", "突破", "提振", "景气", "订单增长", "高景气"]
NEGATIVE_WORDS = ["利空", "暴跌", "下滑", "受挫", "监管", "被查", "终止", "下行", "低迷"]


def score_news_sentiment(news_list: List[NewsItem]) -> float:
    """
    简单情绪得分：
    - 每条标题中：正面词 +1，负面词 -1
    - 最后平均归一化到 [-1, 1]
    """
    if not news_list:
        return 0.0

    total = 0
    count = 0
    for it in news_list:
        title = it.title
        if not title:
            continue
        s = 0
        for w in POSITIVE_WORDS:
            if w in title:
                s += 1
        for w in NEGATIVE_WORDS:
            if w in title:
                s -= 1
        total += s
        count += 1

    if count == 0:
        return 0.0

    avg = total / count
    # 粗暴 clip 到 [-2, 2] 再归一化
    avg = max(-2.0, min(2.0, avg))
    return avg / 2.0  # 映射到 [-1, 1]


# ====== 预留：用 LLM 做情绪分析 ======

def llm_sentiment_summary(news_list: List[NewsItem]) -> str:
    """
    这里预留一个位置给 DeepSeek / 其他 LLM：
    - 把最近若干新闻标题拼成 prompt
    - 让大模型给你输出一句话情绪判断

    目前为了防止你本地没配置 API，这里直接返回空字符串，
    你以后可以自己改。
    """
    if not news_list:
        return "最近缺少相关新闻，情绪信号为中性。"

    titles = [f"- {it.title}" for it in news_list[:10]]
    joined = "\n".join(titles)
    summary = (
        "最近板块相关的重要新闻标题如下：\n"
        f"{joined}\n"
        "总体来看情绪大致为：中性偏多。"
    )
    return summary