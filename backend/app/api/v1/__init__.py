# API v1 package
from fastapi import APIRouter

from app.api.v1 import config, limit_up, market, review, statistics, websocket

api_router = APIRouter()

api_router.include_router(limit_up.router, prefix="/limit-up", tags=["涨停"])
api_router.include_router(statistics.router, prefix="/statistics", tags=["统计"])
api_router.include_router(review.router, prefix="/statistics/review", tags=["复盘"])
api_router.include_router(market.router, prefix="/market", tags=["行情"])
api_router.include_router(config.router, prefix="/config", tags=["配置"])
