from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import news_sentiment

router = APIRouter()


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
    try:
        rows = news_sentiment.fetch_finance_news(limit=size) or []
        items: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            item = _normalize_item(row)
            if not item["title"]:
                continue
            items.append(item)

        return JSONResponse(
            content={
                "ok": True,
                "generated_at": _now_str(),
                "source": str(getattr(news_sentiment, "LAST_NEWS_SOURCE", "-")),
                "items": items,
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
            },
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
