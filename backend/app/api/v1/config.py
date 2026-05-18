"""
配置API
"""
import os
import re
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import settings
from app.database import get_db
from app.models.market_data import UserConfig
from app.schemas.config import UserConfigResponse, UserConfigUpdate

router = APIRouter()
ENV_FILE_PATH = Path(".env")


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
    return build_config_response(config)


@router.put("", response_model=UserConfigResponse, summary="更新配置")
async def update_config(
    config_update: UserConfigUpdate,
    db: AsyncSession = Depends(get_db)
):
    """更新用户配置"""
    config = await get_or_create_config(db)
    
    # 更新字段
    update_data = config_update.model_dump(exclude_unset=True)
    deepseek_updates = extract_deepseek_updates(update_data)
    if deepseek_updates:
        save_secret_env_values(deepseek_updates)
        refresh_deepseek_runtime(deepseek_updates)

    for key, value in update_data.items():
        if value is not None:
            setattr(config, key, value)
    
    await db.commit()
    await db.refresh(config)
    
    return build_config_response(config)


def build_config_response(config: UserConfig) -> UserConfigResponse:
    response = UserConfigResponse.model_validate(config)
    return response.model_copy(update={
        "deepseek_api_key_configured": bool(settings.DEEPSEEK_API_KEY),
        "deepseek_base_url": settings.DEEPSEEK_BASE_URL,
        "deepseek_model": settings.DEEPSEEK_MODEL,
    })


def extract_deepseek_updates(update_data: Dict[str, Any]) -> Dict[str, str]:
    field_map = {
        "deepseek_api_key": "DEEPSEEK_API_KEY",
        "deepseek_base_url": "DEEPSEEK_BASE_URL",
        "deepseek_model": "DEEPSEEK_MODEL",
    }
    updates: Dict[str, str] = {}
    for field, env_key in field_map.items():
        raw_value = update_data.pop(field, None)
        if raw_value is None:
            continue
        value = str(raw_value).strip()
        if not value:
            continue
        if "\n" in value or "\r" in value:
            raise HTTPException(status_code=400, detail=f"{field} 不能包含换行")
        updates[env_key] = value
    return updates


def save_secret_env_values(updates: Dict[str, str]) -> None:
    ENV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = ENV_FILE_PATH.read_text(encoding="utf-8").splitlines() if ENV_FILE_PATH.exists() else []
    updated_keys = set()

    for index, line in enumerate(lines):
        match = re.match(r"^\s*([A-Z0-9_]+)\s*=", line)
        if not match:
            continue
        key = match.group(1)
        if key in updates:
            lines[index] = f"{key}={updates[key]}"
            updated_keys.add(key)

    for key, value in updates.items():
        if key not in updated_keys:
            lines.append(f"{key}={value}")

    ENV_FILE_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def refresh_deepseek_runtime(updates: Dict[str, str]) -> None:
    for key, value in updates.items():
        os.environ[key] = value
        setattr(settings, key, value)

    try:
        from app.services.intelligence_service import intelligence_service
    except Exception:
        return

    summary_client = intelligence_service.summary_client
    if "DEEPSEEK_API_KEY" in updates:
        summary_client.api_key = updates["DEEPSEEK_API_KEY"]
    if "DEEPSEEK_BASE_URL" in updates:
        summary_client.base_url = updates["DEEPSEEK_BASE_URL"].rstrip("/")
    if "DEEPSEEK_MODEL" in updates:
        summary_client.model = updates["DEEPSEEK_MODEL"]


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
