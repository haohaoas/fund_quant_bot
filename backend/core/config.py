# backend/core/config.py
from __future__ import annotations

import os
from functools import lru_cache
from typing import List, Optional

from pydantic import BaseModel


def _try_load_dotenv() -> None:
    """
    Tiny dotenv loader:
    - Loads KEY=VALUE lines into os.environ if key not already set
    - No third-party dependency
    """
    candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.path.dirname(__file__), "..", ".env"),           # backend/.env
        os.path.join(os.path.dirname(__file__), "..", "..", ".env"),     # project root .env
    ]

    for p in candidates:
        try:
            p = os.path.abspath(p)
            if not os.path.exists(p):
                continue
            with open(p, "r", encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and k not in os.environ:
                        os.environ[k] = v
        except Exception:
            # best-effort
            continue


class Settings(BaseModel):
    # CORS
    allow_origins: List[str] = ["*"]

    # Market provider config
    fund_board_provider: str = "akshare"   # akshare|tushare|auto
    tushare_token: str = ""

    # Cache
    sector_cache_ttl_seconds: int = 60


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    _try_load_dotenv()

    # allow origins: comma-separated
    origins_raw = str(os.environ.get("ALLOW_ORIGINS", "")).strip()
    if origins_raw:
        allow_origins = [x.strip() for x in origins_raw.split(",") if x.strip()]
    else:
        allow_origins = ["*"]

    return Settings(
        allow_origins=allow_origins,
        fund_board_provider=str(os.environ.get("FUND_BOARD_PROVIDER", "akshare")).strip().lower(),
        tushare_token=str(os.environ.get("TUSHARE_TOKEN", "")).strip(),
        sector_cache_ttl_seconds=int(os.environ.get("SECTOR_CACHE_TTL_SECONDS", "60")),
    )