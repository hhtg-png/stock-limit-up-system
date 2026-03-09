"""
配置API
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models.market_data import UserConfig
from app.schemas.config import UserConfigResponse, UserConfigUpdate

router = APIRouter()


async def get_or_create_config(db: AsyncSession) -> UserConfig:
    """获取或创建用户配置（单用户模式）"""
    query = select(UserConfig).where(UserConfig.id == 1)
    result = await db.execute(query)
    config = result.scalar_one_or_none()
    
    if not config:
        # 创建默认配置
        config = UserConfig(id=1)
        db.add(config)
        await db.commit()
        await db.refresh(config)
    
    return config


@router.get("", response_model=UserConfigResponse, summary="获取用户配置")
async def get_config(db: AsyncSession = Depends(get_db)):
    """获取当前用户配置"""
    config = await get_or_create_config(db)
    return UserConfigResponse.model_validate(config)


@router.put("", response_model=UserConfigResponse, summary="更新配置")
async def update_config(
    config_update: UserConfigUpdate,
    db: AsyncSession = Depends(get_db)
):
    """更新用户配置"""
    config = await get_or_create_config(db)
    
    # 更新字段
    update_data = config_update.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        if value is not None:
            setattr(config, key, value)
    
    await db.commit()
    await db.refresh(config)
    
    return UserConfigResponse.model_validate(config)


@router.get("/watchlist", summary="获取自选股")
async def get_watchlist(db: AsyncSession = Depends(get_db)):
    """获取自选股列表"""
    config = await get_or_create_config(db)
    return {"watchlist": config.watch_list or []}


@router.post("/watchlist/{stock_code}", summary="添加自选股")
async def add_to_watchlist(
    stock_code: str,
    db: AsyncSession = Depends(get_db)
):
    """添加股票到自选股"""
    config = await get_or_create_config(db)
    
    watchlist = config.watch_list or []
    if stock_code not in watchlist:
        watchlist.append(stock_code)
        config.watch_list = watchlist
        await db.commit()
    
    return {"message": "添加成功", "watchlist": config.watch_list}


@router.delete("/watchlist/{stock_code}", summary="删除自选股")
async def remove_from_watchlist(
    stock_code: str,
    db: AsyncSession = Depends(get_db)
):
    """从自选股删除股票"""
    config = await get_or_create_config(db)
    
    watchlist = config.watch_list or []
    if stock_code in watchlist:
        watchlist.remove(stock_code)
        config.watch_list = watchlist
        await db.commit()
    
    return {"message": "删除成功", "watchlist": config.watch_list}


@router.post("/alert/toggle", summary="切换播报开关")
async def toggle_alert(
    alert_type: str,  # limit_up, big_order, sound, desktop
    enabled: bool,
    db: AsyncSession = Depends(get_db)
):
    """切换播报开关"""
    config = await get_or_create_config(db)
    
    field_map = {
        "limit_up": "alert_limit_up_enabled",
        "big_order": "alert_big_order_enabled",
        "sound": "alert_sound_enabled",
        "desktop": "alert_desktop_enabled"
    }
    
    if alert_type not in field_map:
        raise HTTPException(status_code=400, detail="无效的播报类型")
    
    setattr(config, field_map[alert_type], enabled)
    await db.commit()
    
    return {"message": f"{'开启' if enabled else '关闭'}成功"}


@router.get("/table-columns", summary="获取表格列顺序")
async def get_table_columns(db: AsyncSession = Depends(get_db)):
    """获取用户保存的表格列顺序"""
    config = await get_or_create_config(db)
    return {"columns": config.display_columns or []}


@router.put("/table-columns", summary="保存表格列顺序")
async def save_table_columns(
    columns: list[str],
    db: AsyncSession = Depends(get_db)
):
    """保存表格列顺序"""
    config = await get_or_create_config(db)
    config.display_columns = columns
    await db.commit()
    return {"message": "保存成功", "columns": columns}
