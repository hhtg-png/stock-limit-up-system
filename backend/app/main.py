"""
FastAPI应用主入口
"""
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import settings
from app.database import async_session_maker, init_db, close_db
from app.api.v1 import api_router
from app.api.v1.websocket import router as ws_router
from app.core.event_bus import event_bus
from app.utils.logger import setup_logging, logger
from app.services.data_init_service import data_init_service
from app.data_collectors.scheduler import data_scheduler
from app.data_collectors.tencent_api import tencent_api
from app.services.trading_playbook.composition import (
    build_production_trading_playbook_orchestrator,
    load_production_realtime_limit_up,
)
from app.services.trading_playbook.alert_service import (
    TradingPlaybookAlertService,
)
from app.services.trading_playbook.channels import (
    InAppTradingPlanAlertChannel,
)
from app.services.trading_playbook.runtime import trading_playbook_runtime
from app.utils.time_utils import today_cn


def _clear_trading_playbook_runtime(app: FastAPI) -> None:
    trading_playbook_runtime.reset()
    data_scheduler.reset_trading_playbook_services()
    if hasattr(app.state, "trading_playbook_orchestrator"):
        delattr(app.state, "trading_playbook_orchestrator")
    if hasattr(app.state, "trading_playbook_calendar"):
        delattr(app.state, "trading_playbook_calendar")
    if hasattr(app.state, "trading_playbook_alert_service"):
        delattr(app.state, "trading_playbook_alert_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    setup_logging()
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    try:
        await init_db()
        logger.info("Database initialized")

        await event_bus.start()
        logger.info("EventBus started")

        _clear_trading_playbook_runtime(app)
        if settings.TRADING_PLAYBOOK_ENABLED:
            calendar = data_scheduler.get_trading_calendar_service()
            try:
                await calendar.ensure_date(today_cn())
            except Exception as exc:
                logger.error(f"Trading calendar warm-up failed: {exc}")
            orchestrator = build_production_trading_playbook_orchestrator(
                next_trade_date=calendar.next_trade_date,
            )
            data_scheduler.install_trading_playbook_orchestrator(orchestrator)
            alert_service = TradingPlaybookAlertService(
                InAppTradingPlanAlertChannel(),
                session_factory=async_session_maker,
                quote_api=tencent_api,
                realtime_limit_up_loader=load_production_realtime_limit_up,
                trading_calendar=calendar,
            )
            data_scheduler.install_trading_playbook_alert_service(alert_service)
            trading_playbook_runtime.install_orchestrator(orchestrator)
            app.state.trading_playbook_orchestrator = orchestrator
            app.state.trading_playbook_alert_service = alert_service
            app.state.trading_playbook_calendar = calendar

        # 启动定时任务：盘中采集、盘后统计、市场复盘、每日分析
        data_scheduler.start()

        # 自动爬取最近交易日数据（后台任务）
        asyncio.create_task(data_init_service.initialize())

        yield
    finally:
        try:
            data_scheduler.stop()
        except Exception as exc:
            logger.error(f"DataScheduler shutdown failed: {exc}")
        try:
            orchestrator = getattr(
                app.state,
                "trading_playbook_orchestrator",
                None,
            )
            close = getattr(orchestrator, "aclose", None)
            if callable(close):
                await close()
        except Exception as exc:
            logger.error(f"Trading playbook provider cleanup failed: {exc}")
        try:
            await tencent_api.close()
        except Exception as exc:
            logger.error(f"Tencent quote cleanup failed: {exc}")
        try:
            _clear_trading_playbook_runtime(app)
        except Exception as exc:
            logger.error(f"Trading playbook runtime cleanup failed: {exc}")
        try:
            await data_scheduler.get_trading_calendar_service().close()
        except Exception as exc:
            logger.error(f"Trading calendar cleanup failed: {exc}")
        try:
            await event_bus.stop()
        except Exception as exc:
            logger.error(f"EventBus shutdown failed: {exc}")
        try:
            await close_db()
        except Exception as exc:
            logger.error(f"Database shutdown failed: {exc}")
        logger.info("Application shutdown complete")


# 创建FastAPI应用
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="专业的股票涨停统计分析系统，提供实时涨停监控、大单分析、数据可视化等功能",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册API路由
app.include_router(api_router, prefix="/api/v1")
app.include_router(ws_router)


@app.get("/", tags=["根"])
async def root():
    """API根路径"""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health", tags=["健康检查"])
async def health_check():
    """健康检查接口"""
    return {
        "status": "healthy",
        "version": settings.APP_VERSION
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
