# API v1 package
from fastapi import APIRouter

from app.api.v1 import config, daily_analysis, intelligence, limit_up, market, review, statistics, tdx_plugins, trading_playbook, tts, websocket

api_router = APIRouter()

api_router.include_router(limit_up.router, prefix="/limit-up", tags=["涨停"])
api_router.include_router(statistics.router, prefix="/statistics", tags=["统计"])
api_router.include_router(review.router, prefix="/statistics/review", tags=["复盘"])
api_router.include_router(daily_analysis.router, prefix="/statistics/daily-analysis", tags=["每日分析"])
api_router.include_router(intelligence.router, prefix="/intelligence", tags=["知识智能"])
api_router.include_router(market.router, prefix="/market", tags=["行情"])
api_router.include_router(config.router, prefix="/config", tags=["配置"])
api_router.include_router(tdx_plugins.router, prefix="/tdx-plugins", tags=["通达信插件"])
api_router.include_router(tts.router, prefix="/tts", tags=["语音播报"])
api_router.include_router(
    trading_playbook.router,
    prefix="/trading-playbook",
    tags=["交易预案"],
)
