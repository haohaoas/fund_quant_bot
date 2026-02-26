# backend/main.py
from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.db import init_db
from backend.core.config import get_settings

# Routers
from backend.routers.auth import router as auth_router
from backend.routers.account import router as account_router
from backend.routers.portfolio import router as portfolio_router
from backend.routers.trades import router as trades_router
from backend.routers.quotes import router as quotes_router
from backend.routers.market import router as market_router
from backend.routers.recommendations import router as recommendations_router  # 新增
from backend.routers.news import router as news_router
from backend.routers.watchlist import router as watchlist_router
from backend.ui_router import router as ui_router


logger = logging.getLogger("fund_quant_bot")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(title="Fund Quant Bot API", version="0.4.1")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # backend/main.py
    @app.on_event("startup")
    def _on_startup():
        init_db()

        # AKShare: configure once at startup (best-effort)
        try:
            import akshare as ak

            logger.info("AKShare version: %s", getattr(ak, "__version__", "unknown"))

            # NOTE: Some AKShare versions do NOT expose `set_base_url`.
            # In that case, skip setting and rely on normal network/proxy/hosts configuration.
            if hasattr(ak, "set_base_url"):
                ak.set_base_url("https://gitee.com/akfamily/akshare/raw/master/")
                logger.info("AKShare base_url set to gitee raw master")
            else:
                logger.warning(
                    "AKShare does not provide `set_base_url` in this installed version; "
                    "skipping base_url override"
                )
        except Exception as e:
            logger.warning("AKShare init failed: %s", e)

    @app.get("/api/health")
    def health():
        return {"ok": True}

    # API routers
    app.include_router(auth_router)
    app.include_router(account_router)
    app.include_router(portfolio_router)
    app.include_router(trades_router)
    app.include_router(quotes_router)
    app.include_router(market_router)
    app.include_router(recommendations_router)
    app.include_router(news_router)
    app.include_router(watchlist_router)
    # UI router (front-end pages / static / etc.)
    app.include_router(ui_router)

    return app


app = create_app()
