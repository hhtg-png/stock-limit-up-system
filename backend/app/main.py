"""
FastAPI应用主入口
"""
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import settings
from app.database import init_db, close_db
from app.api.v1 import api_router
from app.api.v1.websocket import router as ws_router
from app.core.event_bus import event_bus
from app.utils.logger import setup_logging, logger
from app.services.data_init_service import data_init_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    setup_logging()
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    
    # 初始化数据库
    await init_db()
    logger.info("Database initialized")
    
    # 启动事件总线
    await event_bus.start()
    logger.info("EventBus started")
    
    # 自动爬取最近交易日数据（后台任务）
    asyncio.create_task(data_init_service.initialize())
    
    yield
    
    # 关闭时
    await event_bus.stop()
    await close_db()
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
