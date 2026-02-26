# data_layer.py
"""
统一数据获取层：提供稳定的数据访问接口，自动降级和缓存
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Callable
from pathlib import Path
import sqlite3
from functools import wraps

import pandas as pd
import requests

try:
    import akshare as ak
except ImportError:
    ak = None


# ============ 持久化缓存 ============

class PersistentCache:
    """基于SQLite的持久化缓存"""
    
    def __init__(self, db_path: str = ".cache/data_cache.db", ttl_seconds: int = 300):
        self.db_path = db_path
        self.ttl_seconds = ttl_seconds
        self._init_db()
    
    def _init_db(self):
        """初始化缓存数据库"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_expires ON cache(expires_at)")
        conn.commit()
        conn.close()
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存，如果过期则返回None"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = time.time()
        cursor.execute(
            "SELECT value FROM cache WHERE key = ? AND expires_at > ?",
            (key, now)
        )
        result = cursor.fetchone()
        conn.close()
        
        if result:
            try:
                return json.loads(result[0])
            except Exception:
                return None
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """设置缓存"""
        ttl = ttl or self.ttl_seconds
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = time.time()
        expires_at = now + ttl
        
        try:
            value_json = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            value_json = str(value)
        
        cursor.execute("""
            INSERT OR REPLACE INTO cache (key, value, created_at, expires_at)
            VALUES (?, ?, ?, ?)
        """, (key, value_json, now, expires_at))
        
        conn.commit()
        conn.close()
    
    def clear_expired(self):
        """清理过期缓存"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM cache WHERE expires_at < ?", (time.time(),))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        return deleted


# ============ 数据源管理 ============

class DataSource:
    """数据源基类"""
    
    def __init__(self, name: str, priority: int = 0):
        self.name = name
        self.priority = priority
        self.fail_count = 0
        self.last_fail_time = 0
        self.circuit_breaker_threshold = 3
        self.circuit_breaker_timeout = 300
    
    def is_available(self) -> bool:
        """判断数据源是否可用（熔断器检查）"""
        if self.fail_count < self.circuit_breaker_threshold:
            return True
        
        if time.time() - self.last_fail_time > self.circuit_breaker_timeout:
            self.fail_count = 0
            return True
        
        return False
    
    def record_success(self):
        self.fail_count = 0
    
    def record_failure(self):
        self.fail_count += 1
        self.last_fail_time = time.time()


class DataSourceRegistry:
    """数据源注册表"""
    
    def __init__(self):
        self.sources: Dict[str, Dict[str, DataSource]] = {}
    
    def register(self, data_type: str, source: DataSource):
        if data_type not in self.sources:
            self.sources[data_type] = {}
        self.sources[data_type][source.name] = source
    
    def get_sources(self, data_type: str) -> List[DataSource]:
        if data_type not in self.sources:
            return []
        
        sources = [s for s in self.sources[data_type].values() if s.is_available()]
        sources.sort(key=lambda x: x.priority, reverse=True)
        return sources


# ============ 统一数据获取器 ============

class DataFetcher:
    """统一数据获取接口"""
    
    def __init__(self, cache_dir: str = ".cache"):
        self.cache = PersistentCache(db_path=f"{cache_dir}/data_cache.db")
        self.registry = DataSourceRegistry()
        self._register_sources()
    
    def _register_sources(self):
        self.registry.register("fund_realtime", DataSource("eastmoney_fundgz", priority=100))
        self.registry.register("fund_realtime", DataSource("akshare", priority=80))
        
        if ak:
            self.registry.register("fund_history", DataSource("akshare", priority=100))
            self.registry.register("board_flow", DataSource("akshare_industry", priority=100))
            self.registry.register("board_flow", DataSource("akshare_concept", priority=90))
        
        self.registry.register("news", DataSource("sina", priority=100))
        self.registry.register("news", DataSource("eastmoney_rss", priority=90))
    
    def _make_cache_key(self, data_type: str, **kwargs) -> str:
        parts = [data_type]
        for k, v in sorted(kwargs.items()):
            parts.append(f"{k}={v}")
        return ":".join(parts)
    
    def fetch_with_fallback(
        self,
        data_type: str,
        fetcher_func: Callable,
        validator: Optional[Callable] = None,
        use_cache: bool = True,
        cache_ttl: Optional[int] = None,
        **kwargs
    ) -> Optional[Any]:
        cache_key = self._make_cache_key(data_type, **kwargs)
        
        if use_cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                if validator is None or validator(cached):
                    return cached
        
        sources = self.registry.get_sources(data_type)
        last_error = None
        
        for source in sources:
            try:
                data = fetcher_func(source.name, **kwargs)
                
                if validator is None or validator(data):
                    source.record_success()
                    
                    if use_cache:
                        self.cache.set(cache_key, data, ttl=cache_ttl)
                    
                    return data
                
            except Exception as e:
                source.record_failure()
                last_error = e
                continue
        
        if last_error:
            print(f"[data_layer] All sources failed for {data_type}: {last_error}")
        
        return None


# ============ 具体实现：基金数据获取 ============

def _fetch_eastmoney_fundgz(code: str) -> Dict[str, Any]:
    """从东方财富fundgz接口获取"""
    ts_ms = int(datetime.now().timestamp() * 1000)
    url = f"https://fundgz.1234567.com.cn/js/{code}.js?rt={ts_ms}"
    
    resp = requests.get(url, timeout=5)
    text = resp.text.strip()
    
    if not text.startswith("jsonpgz(") or not text.endswith(");"):
        raise ValueError("Invalid response format")
    
    json_str = text[len("jsonpgz("):-2]
    data = json.loads(json_str)
    
    price = None
    gsz = data.get("gsz")
    dwjz = data.get("dwjz")
    
    if gsz not in (None, ""):
        price = float(gsz)
    elif dwjz not in (None, ""):
        price = float(dwjz)
    
    if price is None:
        raise ValueError("No price data")
    
    pct = None
    pct_str = data.get("gszzl")
    if pct_str not in (None, ""):
        pct = float(pct_str)
    
    ts = datetime.now()
    gztime = data.get("gztime")
    if isinstance(gztime, str) and gztime:
        try:
            ts = datetime.strptime(gztime, "%Y-%m-%d %H:%M")
        except Exception:
            pass
    
    return {
        "code": code,
        "price": price,
        "pct": pct,
        "time": ts,
        "source": "eastmoney_fundgz",
    }


def _fetch_akshare_latest(code: str) -> Dict[str, Any]:
    """从AkShare获取（降级到历史净值）"""
    if ak is None:
        raise ImportError("akshare not available")
    
    raw = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
    
    if raw is None or raw.empty:
        raise ValueError("No data from akshare")
    
    df = raw.tail(1)
    last_row = df.iloc[0]
    
    return {
        "code": code,
        "price": float(last_row["单位净值"]),
        "pct": None,
        "time": pd.to_datetime(last_row["净值日期"]),
        "source": "akshare_history",
    }


def get_fund_latest_price(code: str) -> Optional[Dict[str, Any]]:
    """
    获取基金最新价格（兼容原有接口）
    
    返回:
        {
            "code": str,
            "price": float,
            "pct": float or None,
            "time": datetime,
            "source": str
        }
    """
    fetcher = get_data_fetcher()
    
    def fetch(source_name: str, code: str) -> Dict[str, Any]:
        if source_name == "eastmoney_fundgz":
            return _fetch_eastmoney_fundgz(code)
        elif source_name == "akshare":
            return _fetch_akshare_latest(code)
        raise ValueError(f"Unknown source: {source_name}")
    
    def validate(data: Dict[str, Any]) -> bool:
        return data and "price" in data and data["price"] is not None
    
    return fetcher.fetch_with_fallback(
        data_type="fund_realtime",
        fetcher_func=fetch,
        validator=validate,
        use_cache=True,
        cache_ttl=60,
        code=code
    )


def get_fund_history(code: str, lookback_days: int = 180) -> Optional[pd.DataFrame]:
    """
    获取基金历史净值（兼容原有接口）
    
    返回:
        DataFrame with columns: [date, close]
    """
    fetcher = get_data_fetcher()
    
    def fetch(source_name: str, code: str, lookback_days: int) -> pd.DataFrame:
        if source_name == "akshare":
            if ak is None:
                raise ImportError("akshare not available")
            
            raw = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
            
            if raw is None or raw.empty:
                raise ValueError("No data")
            
            df = raw.copy()
            df.rename(columns={"净值日期": "date", "单位净值": "close"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df.sort_values("date", inplace=True)
            
            if lookback_days > 0:
                end_date = df["date"].max()
                start_date = end_date - timedelta(days=lookback_days * 2)
                df = df[df["date"] >= start_date].reset_index(drop=True)
            
            df["close"] = df["close"].astype(float)
            return df
        
        raise ValueError(f"Unknown source: {source_name}")
    
    def validate(data: pd.DataFrame) -> bool:
        return data is not None and len(data) > 0
    
    return fetcher.fetch_with_fallback(
        data_type="fund_history",
        fetcher_func=fetch,
        validator=validate,
        use_cache=True,
        cache_ttl=3600,
        code=code,
        lookback_days=lookback_days
    )


# ============ 单例实例 ============

_global_data_fetcher = None

def get_data_fetcher() -> DataFetcher:
    global _global_data_fetcher
    if _global_data_fetcher is None:
        _global_data_fetcher = DataFetcher()
    return _global_data_fetcher


if __name__ == "__main__":
    print("Data layer initialized successfully!")
    
    # 简单测试
    print("\n测试基金数据获取...")
    try:
        price = get_fund_latest_price("008888")
        if price:
            print(f"✓ 获取成功: {price['price']} ({price['source']})")
        else:
            print("✗ 获取失败")
    except Exception as e:
        print(f"✗ 错误: {e}")
