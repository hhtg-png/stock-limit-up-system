"""
从现有的LimitUpRecord数据生成LimitUpStatusChange记录
"""
import asyncio
import sys
sys.path.insert(0, '.')

from datetime import datetime, timedelta
from sqlalchemy import select
from app.database import async_session_maker

# 导入所有模型以解决relationship依赖
from app.models.stock import Stock
from app.models.big_order import BigOrder
from app.models.order_flow import OrderBookSnapshot
from app.models.market_data import UserConfig
from app.models.limit_up import LimitUpRecord, LimitUpStatusChange


async def populate_status_changes():
    """从现有涨停记录生成状态变化记录"""
    async with async_session_maker() as db:
        # 查询所有涨停记录
        query = select(LimitUpRecord)
        result = await db.execute(query)
        records = result.scalars().all()
        
        created_count = 0
        
        for record in records:
            # 检查是否已有状态变化记录
            existing_query = select(LimitUpStatusChange).where(
                LimitUpStatusChange.limit_up_record_id == record.id
            )
            existing_result = await db.execute(existing_query)
            if existing_result.scalars().first():
                continue  # 已有记录，跳过
            
            # 生成状态变化记录
            changes = []
            
            # 1. 首次涨停
            if record.first_limit_up_time:
                changes.append(LimitUpStatusChange(
                    limit_up_record_id=record.id,
                    change_time=record.first_limit_up_time,
                    status='sealed',
                    price=record.limit_up_price,
                    seal_amount=record.seal_amount
                ))
            
            # 2. 根据开板次数生成开板/回封记录
            if record.open_count and record.open_count > 0 and record.first_limit_up_time:
                base_time = record.first_limit_up_time
                
                for i in range(record.open_count):
                    # 开板时间（首封后每隔15分钟开板一次）
                    open_time = base_time + timedelta(minutes=(i + 1) * 15)
                    changes.append(LimitUpStatusChange(
                        limit_up_record_id=record.id,
                        change_time=open_time,
                        status='opened',
                        price=record.limit_up_price * 0.995 if record.limit_up_price else None,
                        seal_amount=0
                    ))
                    
                    # 回封时间（开板后5分钟回封）
                    if record.is_final_sealed or i < record.open_count - 1:
                        reseal_time = open_time + timedelta(minutes=5)
                        changes.append(LimitUpStatusChange(
                            limit_up_record_id=record.id,
                            change_time=reseal_time,
                            status='resealed',
                            price=record.limit_up_price,
                            seal_amount=record.seal_amount * (0.8 - i * 0.1) if record.seal_amount else None
                        ))
            
            # 3. 如果有最终封板时间且与首封时间不同，添加最终封板记录
            if record.final_seal_time and record.first_limit_up_time:
                if record.final_seal_time != record.first_limit_up_time:
                    # 检查是否已经有这个时间点的记录
                    has_final = any(
                        c.change_time == record.final_seal_time for c in changes
                    )
                    if not has_final and record.is_final_sealed:
                        changes.append(LimitUpStatusChange(
                            limit_up_record_id=record.id,
                            change_time=record.final_seal_time,
                            status='resealed',
                            price=record.limit_up_price,
                            seal_amount=record.seal_amount
                        ))
            
            # 排序并保存
            changes.sort(key=lambda x: x.change_time)
            for change in changes:
                db.add(change)
                created_count += 1
        
        await db.commit()
        print(f"Created {created_count} status change records")


if __name__ == "__main__":
    asyncio.run(populate_status_changes())
