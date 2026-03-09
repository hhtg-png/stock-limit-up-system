"""
导入测试数据脚本
"""
import asyncio
import sys
sys.path.insert(0, 'd:/code/stock-limit-up-system/backend')

from datetime import datetime, date, timedelta
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

# 导入模型
from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord
from app.models.market_data import DailyStatistics
from app.models.big_order import BigOrder
from app.models.order_flow import OrderBookSnapshot
from app.database import Base

DATABASE_URL = "sqlite+aiosqlite:///./data/stock_limit_up.db"

# 测试数据 - 模拟上一交易日的涨停数据
TEST_STOCKS = [
    {"code": "000001", "name": "平安银行", "market": "SZ", "industry": "银行"},
    {"code": "000002", "name": "万科A", "market": "SZ", "industry": "房地产"},
    {"code": "000063", "name": "中兴通讯", "market": "SZ", "industry": "通信设备"},
    {"code": "000100", "name": "TCL科技", "market": "SZ", "industry": "消费电子"},
    {"code": "000157", "name": "中联重科", "market": "SZ", "industry": "工程机械"},
    {"code": "000333", "name": "美的集团", "market": "SZ", "industry": "家电"},
    {"code": "000338", "name": "潍柴动力", "market": "SZ", "industry": "汽车配件"},
    {"code": "000408", "name": "藏格矿业", "market": "SZ", "industry": "矿业"},
    {"code": "000425", "name": "徐工机械", "market": "SZ", "industry": "工程机械"},
    {"code": "000538", "name": "云南白药", "market": "SZ", "industry": "医药"},
    {"code": "000568", "name": "泸州老窖", "market": "SZ", "industry": "白酒"},
    {"code": "000625", "name": "长安汽车", "market": "SZ", "industry": "汽车"},
    {"code": "000651", "name": "格力电器", "market": "SZ", "industry": "家电"},
    {"code": "000725", "name": "京东方A", "market": "SZ", "industry": "面板"},
    {"code": "000768", "name": "中航西飞", "market": "SZ", "industry": "军工"},
    {"code": "000776", "name": "广发证券", "market": "SZ", "industry": "证券"},
    {"code": "000858", "name": "五粮液", "market": "SZ", "industry": "白酒"},
    {"code": "000895", "name": "双汇发展", "market": "SZ", "industry": "食品"},
    {"code": "000938", "name": "紫光股份", "market": "SZ", "industry": "计算机"},
    {"code": "000977", "name": "浪潮信息", "market": "SZ", "industry": "服务器"},
    {"code": "002049", "name": "紫光国微", "market": "SZ", "industry": "半导体"},
    {"code": "002129", "name": "TCL中环", "market": "SZ", "industry": "光伏"},
    {"code": "002230", "name": "科大讯飞", "market": "SZ", "industry": "人工智能"},
    {"code": "002352", "name": "顺丰控股", "market": "SZ", "industry": "物流"},
    {"code": "002415", "name": "海康威视", "market": "SZ", "industry": "安防"},
    {"code": "002460", "name": "赣锋锂业", "market": "SZ", "industry": "锂电"},
    {"code": "002475", "name": "立讯精密", "market": "SZ", "industry": "消费电子"},
    {"code": "002594", "name": "比亚迪", "market": "SZ", "industry": "新能源车"},
    {"code": "002714", "name": "牧原股份", "market": "SZ", "industry": "畜牧业"},
    {"code": "300059", "name": "东方财富", "market": "SZ", "industry": "互联网金融"},
]

# 涨停原因分类
LIMIT_UP_REASONS = [
    ("AI算力需求爆发，服务器订单大增", "人工智能", 3),
    ("锂电池材料价格企稳，产业链回暖", "新能源", 2),
    ("半导体国产替代加速，订单饱满", "半导体", 2),
    ("军工订单超预期，业绩高增长", "军工", 1),
    ("消费复苏，白酒销售旺季", "消费", 1),
    ("光伏装机量创新高，龙头受益", "新能源", 2),
    ("机器人概念爆发，减速器龙头", "人工智能", 4),
    ("数据中心建设提速，IDC龙头", "数字经济", 1),
    ("汽车智能化加速，零部件受益", "消费", 1),
    ("创新药获批，业绩拐点确立", "医药医疗", 1),
]


async def import_test_data():
    """导入测试数据"""
    engine = create_async_engine(DATABASE_URL, echo=True)
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        # 使用上周五作为测试日期（模拟最近交易日）
        today = date.today()
        # 找到最近的周五
        days_since_friday = (today.weekday() - 4) % 7
        if days_since_friday == 0 and today.weekday() != 4:
            days_since_friday = 7
        last_friday = today - timedelta(days=days_since_friday if days_since_friday > 0 else 7)
        
        print(f"导入测试数据，日期: {last_friday}")
        
        # 导入股票基础信息
        stock_ids = {}
        for stock_data in TEST_STOCKS:
            # 检查是否已存在
            result = await session.execute(
                select(Stock).where(Stock.stock_code == stock_data["code"])
            )
            stock = result.scalar_one_or_none()
            
            if not stock:
                stock = Stock(
                    stock_code=stock_data["code"],
                    stock_name=stock_data["name"],
                    market=stock_data["market"],
                    industry=stock_data["industry"],
                    is_st=False,
                    is_kc=stock_data["code"].startswith("688"),
                    is_cy=stock_data["code"].startswith("300"),
                )
                session.add(stock)
                await session.flush()
            
            stock_ids[stock_data["code"]] = stock.id
        
        # 导入涨停记录（选取部分股票作为涨停股）
        import random
        limit_up_stocks = random.sample(TEST_STOCKS, 15)  # 随机选15只作为涨停
        
        for i, stock_data in enumerate(limit_up_stocks):
            reason_data = LIMIT_UP_REASONS[i % len(LIMIT_UP_REASONS)]
            
            # 检查是否已存在
            result = await session.execute(
                select(LimitUpRecord).where(
                    LimitUpRecord.stock_id == stock_ids[stock_data["code"]],
                    LimitUpRecord.trade_date == last_friday
                )
            )
            existing = result.scalar_one_or_none()
            if existing:
                continue
            
            # 随机生成涨停时间（9:30-14:30）
            hour = random.randint(9, 14)
            minute = random.randint(0, 59) if hour > 9 else random.randint(30, 59)
            second = random.randint(0, 59)
            limit_up_time = datetime.combine(last_friday, datetime.min.time().replace(
                hour=hour, minute=minute, second=second
            ))
            
            record = LimitUpRecord(
                stock_id=stock_ids[stock_data["code"]],
                trade_date=last_friday,
                first_limit_up_time=limit_up_time,
                limit_up_reason=reason_data[0],
                reason_category=reason_data[1],
                continuous_limit_up_days=reason_data[2],
                open_count=random.randint(0, 3),
                is_final_sealed=random.random() > 0.2,  # 80%封板
                seal_amount=random.uniform(5000, 50000),  # 封单金额（万）
                seal_volume=random.randint(100000, 1000000),
                limit_up_price=round(random.uniform(10, 100), 2),
                turnover_rate=round(random.uniform(5, 25), 2),
                amount=round(random.uniform(10000, 100000), 2),  # 成交额（万）
                data_source="TEST",
            )
            session.add(record)
        
        # 导入每日统计
        result = await session.execute(
            select(DailyStatistics).where(DailyStatistics.trade_date == last_friday)
        )
        existing_stats = result.scalar_one_or_none()
        
        if not existing_stats:
            stats = DailyStatistics(
                trade_date=last_friday,
                total_limit_up=15,
                new_limit_up=8,
                continuous_2=4,
                continuous_3=2,
                continuous_4_plus=1,
                break_count=3,
                break_rate=20.0,
                average_seal_time="10:25",
                early_seal_count=5,
                strongest_sector="人工智能",
                total_stocks=5000,
                up_count=2800,
                down_count=1800,
                limit_down_count=5,
            )
            session.add(stats)
        
        await session.commit()
        print(f"测试数据导入完成！共导入 {len(limit_up_stocks)} 条涨停记录")


if __name__ == "__main__":
    asyncio.run(import_test_data())
