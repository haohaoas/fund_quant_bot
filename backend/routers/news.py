from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import news_sentiment

router = APIRouter()

_NEWS_CACHE_LOCK = threading.Lock()
_NEWS_CACHE: Dict[str, Any] = {"ts": 0.0, "limit": 0, "data": None}
_NEWS_CACHE_TTL_SECONDS = int(os.getenv("NEWS_CACHE_TTL_SECONDS", "60"))
_NEWS_FETCH_TIMEOUT_SECONDS = float(os.getenv("NEWS_FETCH_TIMEOUT_SECONDS", "6"))


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "title": str(raw.get("title") or "").strip(),
        "summary": str(raw.get("summary") or "").strip(),
        "url": str(raw.get("url") or "").strip(),
        "ctime": str(raw.get("ctime") or "").strip(),
        "media_name": str(raw.get("media_name") or "").strip(),
    }


@router.get("/api/news")
def get_news_list(limit: int = 40):
    size = max(1, min(int(limit), 100))
    now = time.time()
    with _NEWS_CACHE_LOCK:
        cached = _NEWS_CACHE.get("data")
        ts = float(_NEWS_CACHE.get("ts") or 0.0)
        cached_limit = int(_NEWS_CACHE.get("limit") or 0)
        if (
            isinstance(cached, dict)
            and (now - ts) <= _NEWS_CACHE_TTL_SECONDS
            and cached_limit >= size
        ):
            out = dict(cached)
            items = list(out.get("items") or [])[:size]
            out["items"] = items
            out["cached"] = True
            return JSONResponse(
                content=out,
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(news_sentiment.fetch_finance_news, limit=size)
            rows = fut.result(timeout=_NEWS_FETCH_TIMEOUT_SECONDS) or []
        items: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            item = _normalize_item(row)
            if not item["title"]:
                continue
            items.append(item)

        payload = {
            "ok": True,
            "generated_at": _now_str(),
            "source": str(getattr(news_sentiment, "LAST_NEWS_SOURCE", "-")),
            "items": items,
            "cached": False,
        }
        with _NEWS_CACHE_LOCK:
            _NEWS_CACHE["ts"] = now
            _NEWS_CACHE["limit"] = max(size, int(_NEWS_CACHE.get("limit") or 0))
            _NEWS_CACHE["data"] = payload
        return JSONResponse(content=payload, headers={"Content-Type": "application/json; charset=utf-8"})
    except FuturesTimeoutError:
        with _NEWS_CACHE_LOCK:
            cached = _NEWS_CACHE.get("data")
        if isinstance(cached, dict):
            out = dict(cached)
            out["cached"] = True
            out["warning"] = "新闻源响应超时，返回缓存数据。"
            out["items"] = list(out.get("items") or [])[:size]
            return JSONResponse(content=out, headers={"Content-Type": "application/json; charset=utf-8"})
        return JSONResponse(
            content={
                "ok": False,
                "generated_at": _now_str(),
                "source": str(getattr(news_sentiment, "LAST_NEWS_SOURCE", "-")),
                "items": [],
                "error": "news fetch timeout",
                "cached": False,
            },
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
    except Exception as exc:
        return JSONResponse(
            content={
                "ok": False,
                "generated_at": _now_str(),
                "source": str(getattr(news_sentiment, "LAST_NEWS_SOURCE", "-")),
                "items": [],
                "error": str(exc),
                "cached": False,
            },
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
