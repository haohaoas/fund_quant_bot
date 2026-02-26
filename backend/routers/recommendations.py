# backend/routers/recommendations.py
"""
每日基金推荐路由 - 连接 run_fund_daily.py 的输出
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, date
from functools import lru_cache
import time

from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse

router = APIRouter()


# ============ 全局缓存 ============

_CACHE = {
    "data": None,
    "timestamp": 0.0,
    "date": None,
    "computing": False,
}

CACHE_TTL_SECONDS = 300  # 5分钟缓存


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _is_cache_valid() -> bool:
    """检查缓存是否有效"""
    if _CACHE["data"] is None:
        return False
    
    # 检查是否是今天的数据
    if _CACHE["date"] != date.today():
        return False
    
    # 检查是否在TTL内
    elapsed = time.time() - _CACHE["timestamp"]
    return elapsed < CACHE_TTL_SECONDS


def _compute_recommendations() -> Dict[str, Any]:
    """
    计算每日推荐（耗时操作）
    """
    global _CACHE
    
    # 防止并发计算
    if _CACHE["computing"]:
        # 如果正在计算，返回旧数据或空数据
        if _CACHE["data"]:
            return _CACHE["data"]
        return {
            "news": None,
            "funds": [],
            "market_picker": None,
            "_computing": True,
        }
    
    try:
        _CACHE["computing"] = True
        
        # 添加项目根目录到路径
        import sys
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        
        # 导入并执行
        from run_fund_daily import compute_daily_result
        
        print(f"[recommendations] 开始计算每日推荐...")
        start = time.time()
        
        result = compute_daily_result()
        
        elapsed = time.time() - start
        print(f"[recommendations] 计算完成，耗时 {elapsed:.2f}秒")
        
        # 更新缓存
        _CACHE["data"] = result
        _CACHE["timestamp"] = time.time()
        _CACHE["date"] = date.today()
        
        return result
        
    except Exception as e:
        print(f"[recommendations] 计算失败: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "news": None,
            "funds": [],
            "market_picker": None,
            "_error": str(e),
        }
    finally:
        _CACHE["computing"] = False


def _get_recommendations_data() -> Dict[str, Any]:
    """
    获取推荐数据（优先使用缓存）
    """
    # 1. 检查缓存
    if _is_cache_valid():
        print(f"[recommendations] 使用缓存数据")
        return _CACHE["data"]
    
    # 2. 缓存失效，重新计算
    print(f"[recommendations] 缓存失效，重新计算...")
    return _compute_recommendations()


def _background_refresh():
    """后台刷新缓存"""
    try:
        _compute_recommendations()
    except Exception as e:
        print(f"[recommendations] 后台刷新失败: {e}")


@router.get("/api/recommendations")
def get_recommendations(
    background_tasks: BackgroundTasks,
    force_refresh: bool = False
):
    """
    获取今日基金推荐
    
    参数:
        force_refresh: 是否强制刷新（忽略缓存）
    
    返回格式：
    {
        "generated_at": "2025-01-27 14:30:00",
        "cached": true,  // 是否来自缓存
        "computing": false,  // 是否正在计算中
        "summary": {
            "headline": "今日建议...",
            "risk_notes": ["风险1", "风险2"]
        },
        "actions": [
            {
                "code": "008888",
                "name": "华夏半导体ETF",
                "latest": {"price": 1.52, "time": "..."},
                "signal": "BUY",
                "ai_decision": {"action": "BUY", "reason": "..."}
            }
        ],
        "market": {
            "sentiment": "bullish",
            "hot_sectors": ["半导体", "机器人"]
        }
    }
    """
    try:
        # 强制刷新
        if force_refresh:
            print(f"[recommendations] 强制刷新")
            result = _compute_recommendations()
            cached = False
        else:
            # 优先使用缓存
            result = _get_recommendations_data()
            cached = _is_cache_valid()
        
        # 如果缓存即将过期（还剩30秒），后台刷新
        if cached:
            elapsed = time.time() - _CACHE["timestamp"]
            if elapsed > (CACHE_TTL_SECONDS - 30):
                background_tasks.add_task(_background_refresh)
        
        funds = result.get("funds", [])
        news = result.get("news")
        computing = result.get("_computing", False)
        error = result.get("_error")
        
        # 构建响应
        actions = []
        for fund in funds:
            actions.append({
                "code": fund.get("code"),
                "name": fund.get("name"),
                "latest": fund.get("latest", {}),
                "signal": fund.get("signal", "HOLD"),
                "ai_decision": fund.get("ai_decision", {})
            })
        
        # 市场情绪摘要
        market_summary = {}
        if news:
            market_summary = {
                "sentiment": news.get("market_sentiment", "neutral"),
                "score": news.get("score", 50),
                "hot_sectors": news.get("hot_sectors", []),
                "suggested_style": news.get("suggested_style", "")
            }
        
        # 生成摘要
        summary = {
            "headline": f"今日分析了 {len(funds)} 只基金",
            "risk_notes": [
                "本推荐仅供参考，不构成投资建议",
                "投资有风险，入市需谨慎"
            ]
        }
        
        if news:
            summary["headline"] = f"市场情绪：{news.get('market_sentiment')} | {len(funds)} 只基金分析完成"
        
        if computing:
            summary["headline"] = "正在生成推荐，请稍候刷新..."
            summary["risk_notes"] = ["计算中，预计需要30-60秒"]
        
        if error:
            summary["headline"] = "推荐生成失败"
            summary["risk_notes"] = [f"错误：{error}"]
        
        return JSONResponse(
            content={
                "generated_at": _now_str(),
                "cached": cached,
                "computing": computing,
                "cache_age_seconds": int(time.time() - _CACHE["timestamp"]) if _CACHE["timestamp"] > 0 else None,
                "summary": summary,
                "actions": actions,
                "market": market_summary,
            },
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        
    except Exception as e:
        print(f"[recommendations] 处理请求失败: {e}")
        import traceback
        traceback.print_exc()
        
        # 返回空数据而不是500错误
        return JSONResponse(
            content={
                "generated_at": _now_str(),
                "cached": False,
                "computing": False,
                "summary": {
                    "headline": "推荐生成失败，请稍后重试",
                    "risk_notes": [f"系统错误：{str(e)}"]
                },
                "actions": [],
                "market": {},
            },
            headers={"Content-Type": "application/json; charset=utf-8"},
        )


@router.post("/api/recommendations/refresh")
def refresh_recommendations(background_tasks: BackgroundTasks):
    """
    手动触发后台刷新推荐数据
    """
    if _CACHE["computing"]:
        return {
            "ok": False,
            "message": "已有刷新任务在运行中",
            "computing": True
        }
    
    background_tasks.add_task(_background_refresh)
    
    return {
        "ok": True,
        "message": "后台刷新任务已启动",
        "computing": True
    }


@router.get("/api/recommendations/status")
def get_recommendations_status():
    """
    获取推荐数据状态（不触发计算）
    """
    return {
        "cached": _is_cache_valid(),
        "computing": _CACHE["computing"],
        "cache_age_seconds": int(time.time() - _CACHE["timestamp"]) if _CACHE["timestamp"] > 0 else None,
        "cache_date": str(_CACHE["date"]) if _CACHE["date"] else None,
        "has_data": _CACHE["data"] is not None,
        "ttl_seconds": CACHE_TTL_SECONDS,
    }
