from fastapi import APIRouter
from backend.ui.dashboard import router as dashboard_router
from backend.ui.portfolio import router as portfolio_router
from backend.ui.strategy import router as strategy_router
from backend.ui.record import router as record_router

router = APIRouter()
router.include_router(dashboard_router)
router.include_router(portfolio_router)
router.include_router(strategy_router)
router.include_router(record_router)