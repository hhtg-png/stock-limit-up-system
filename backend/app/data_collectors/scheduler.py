"""
任务调度器
"""
import asyncio
from datetime import datetime, time, date
from typing import List, Dict, Tuple
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.config import settings
from app.utils.time_utils import is_trading_time, get_market_status


class DataScheduler:
    """数据采集任务调度器"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._is_running = False
        # 监控股票缓存
        self._monitored_stocks: List[Dict] = []
        self._stocks_cache_time: datetime = datetime.min
        self._STOCKS_CACHE_TTL = 300  # 5分钟刷新一次监控列表
    
    def start(self):
        """启动调度器"""
        if self._is_running:
            return
        
        # 盘中Level-2数据采集（每3秒）
        self.scheduler.add_job(
            self._collect_l2_data,
            IntervalTrigger(seconds=settings.L2_COLLECT_INTERVAL),
            id="l2_collect",
            name="Level-2数据采集",
            max_instances=1
        )
        
        # 同花顺爬虫（每5分钟）
        self.scheduler.add_job(
            self._crawl_ths_data,
            IntervalTrigger(seconds=settings.CRAWLER_INTERVAL_THS),
            id="ths_crawl",
            name="同花顺数据爬取",
            max_instances=1
        )
        
        # 开盘啦爬虫（每10分钟）
        self.scheduler.add_job(
            self._crawl_kpl_data,
            IntervalTrigger(seconds=settings.CRAWLER_INTERVAL_KPL),
            id="kpl_crawl",
            name="开盘啦数据爬取",
            max_instances=1
        )
        
        # 盘后统计（每天15:30）
        self.scheduler.add_job(
            self._calculate_daily_stats,
            CronTrigger(hour=15, minute=30),
            id="daily_stats",
            name="每日统计计算"
        )
        
        # 每日缓存清理（每天16:00）
        self.scheduler.add_job(
            self._clear_daily_cache,
            CronTrigger(hour=16, minute=0),
            id="clear_cache",
            name="每日缓存清理"
        )
        
        self.scheduler.start()
        self._is_running = True
        logger.info("DataScheduler started")
    
    def stop(self):
        """停止调度器"""
        if self._is_running:
            self.scheduler.shutdown()
            self._is_running = False
            logger.info("DataScheduler stopped")
    
    async def _get_monitored_stocks(self, db) -> List[Dict]:
        """获取需要监控的股票列表（当日涨停股票），带缓存"""
        now = datetime.now()
        if (now - self._stocks_cache_time).total_seconds() < self._STOCKS_CACHE_TTL and self._monitored_stocks:
            return self._monitored_stocks
        
        from app.models.limit_up import LimitUpRecord
        from app.models.stock import Stock
        from sqlalchemy import select
        
        today = date.today()
        query = (
            select(Stock.id, Stock.stock_code, Stock.stock_name, Stock.market)
            .join(LimitUpRecord, LimitUpRecord.stock_id == Stock.id)
            .where(LimitUpRecord.trade_date == today)
        )
        result = await db.execute(query)
        rows = result.all()
        
        self._monitored_stocks = [
            {"id": r.id, "stock_code": r.stock_code, "stock_name": r.stock_name, "market": r.market}
            for r in rows
        ]
        self._stocks_cache_time = now
        
        if self._monitored_stocks:
            logger.info(f"Monitored stocks refreshed: {len(self._monitored_stocks)} limit-up stocks")
        
        return self._monitored_stocks
    
    async def _collect_l2_data(self):
        """采集Level-2数据：盘口快照 + 逐笔成交大单分析"""
        if not is_trading_time():
            return
        
        try:
            from app.data_collectors.tdx_collector import tdx_collector
            from app.database import async_session_maker
            from app.models.order_flow import OrderBookSnapshot
            from app.models.stock import Stock
            from app.analyzers.big_order_analyzer import big_order_analyzer
            from app.analyzers.limit_up_analyzer import limit_up_analyzer
            from app.utils.stock_utils import calculate_limit_up_price
            
            async with async_session_maker() as db:
                # 获取当日涨停股票列表
                stocks = await self._get_monitored_stocks(db)
                
                if not stocks:
                    return
                
                # 批量获取行情
                stock_list = [(s["stock_code"], s["market"]) for s in stocks]
                quotes = await tdx_collector.get_quotes_batch(stock_list)
                
                if not quotes:
                    return
                
                # 构建 stock_code -> stock_info 映射
                stock_map = {s["stock_code"]: s for s in stocks}
                
                # 1. 保存盘口快照
                now = datetime.now()
                for quote in quotes:
                    code = quote.get("stock_code")
                    stock_info = stock_map.get(code)
                    if not stock_info:
                        continue
                    
                    snapshot = OrderBookSnapshot(
                        stock_id=stock_info["id"],
                        snapshot_time=now,
                        current_price=quote.get("current_price"),
                        pre_close=quote.get("pre_close"),
                        open_price=quote.get("open_price"),
                        high_price=quote.get("high_price"),
                        low_price=quote.get("low_price"),
                        bid_prices=quote.get("bid_prices"),
                        bid_volumes=quote.get("bid_volumes"),
                        ask_prices=quote.get("ask_prices"),
                        ask_volumes=quote.get("ask_volumes"),
                        volume=quote.get("volume"),
                        amount=quote.get("amount"),
                        buy_volume=quote.get("buy_volume"),
                        sell_volume=quote.get("sell_volume"),
                    )
                    db.add(snapshot)
                
                await db.commit()
                
                # 2. 采集逐笔成交 + 大单分析
                # 构建 quote 映射用于盘口数据
                quote_map = {q["stock_code"]: q for q in quotes}
                
                for stock_info in stocks:
                    code = stock_info["stock_code"]
                    market = stock_info["market"]
                    quote = quote_map.get(code)
                    if not quote:
                        continue
                    
                    try:
                        transactions = await tdx_collector.get_transaction_data(code, market, start=0, count=100)
                        if not transactions:
                            continue
                        
                        # 构建盘口信息供大单分析器使用
                        bid_prices = quote.get("bid_prices", [0])
                        ask_prices = quote.get("ask_prices", [0])
                        pre_close = quote.get("pre_close", 0)
                        limit_up_price = calculate_limit_up_price(pre_close, code, stock_info.get("stock_name", ""))
                        
                        orderbook_data = {
                            "bid1_price": bid_prices[0] if bid_prices else 0,
                            "ask1_price": ask_prices[0] if ask_prices else 0,
                            "limit_up_price": limit_up_price,
                        }
                        
                        # 创建临时 Stock 对象供 analyzer 使用
                        stock_obj = Stock(
                            id=stock_info["id"],
                            stock_code=code,
                            stock_name=stock_info["stock_name"],
                            market=market
                        )
                        
                        for txn in transactions:
                            await big_order_analyzer.analyze_transaction(stock_obj, txn, orderbook_data, db)
                    
                    except Exception as e:
                        logger.debug(f"Transaction analysis error for {code}: {e}")
                        continue
        
        except Exception as e:
            logger.error(f"L2 data collection error: {e}")
    
    async def _crawl_ths_data(self):
        """爬取同花顺数据"""
        if not is_trading_time():
            return
        
        try:
            from app.crawlers.tonghuashun_crawler import ths_crawler
            from app.database import async_session_maker
            from app.models.stock import Stock
            from app.analyzers.limit_up_analyzer import limit_up_analyzer
            from sqlalchemy import select
            
            # 爬取涨停数据
            data_list = await ths_crawler.crawl()
            
            if not data_list:
                return
            
            async with async_session_maker() as db:
                for data in data_list:
                    stock_code = data.get("stock_code")
                    if not stock_code:
                        continue
                    
                    # 查找或创建股票记录
                    query = select(Stock).where(Stock.stock_code == stock_code)
                    result = await db.execute(query)
                    stock = result.scalar_one_or_none()
                    
                    if not stock:
                        # 创建新股票记录
                        stock = Stock(
                            stock_code=stock_code,
                            stock_name=data.get("stock_name", ""),
                            market="SH" if stock_code.startswith("6") else "SZ"
                        )
                        db.add(stock)
                        await db.commit()
                        await db.refresh(stock)
                    
                    # 保存涨停记录
                    await limit_up_analyzer.save_limit_up_record(stock, data, db)
            
            logger.info(f"THS crawl completed: {len(data_list)} stocks")
        
        except Exception as e:
            logger.error(f"THS crawl error: {e}")
    
    async def _crawl_kpl_data(self):
        """爬取开盘啦数据"""
        if not is_trading_time():
            return
        
        try:
            from app.crawlers.kaipanla_crawler import kpl_crawler
            
            # 爬取涨停数据（用于验证）
            data_list = await kpl_crawler.crawl()
            
            if data_list:
                logger.info(f"KPL crawl completed: {len(data_list)} stocks")
                # TODO: 数据验证逻辑
        
        except Exception as e:
            logger.error(f"KPL crawl error: {e}")
    
    async def _calculate_daily_stats(self):
        """计算每日统计数据"""
        try:
            from app.database import async_session_maker
            from app.models.limit_up import LimitUpRecord
            from app.models.market_data import DailyStatistics
            from sqlalchemy import select, func
            from datetime import date
            
            today = date.today()
            
            async with async_session_maker() as db:
                # 统计涨停数据
                query = select(
                    func.count(LimitUpRecord.id).label('total'),
                    func.sum(func.case((LimitUpRecord.continuous_limit_up_days == 1, 1), else_=0)).label('new'),
                    func.sum(func.case((LimitUpRecord.continuous_limit_up_days == 2, 1), else_=0)).label('c2'),
                    func.sum(func.case((LimitUpRecord.continuous_limit_up_days == 3, 1), else_=0)).label('c3'),
                    func.sum(func.case((LimitUpRecord.continuous_limit_up_days >= 4, 1), else_=0)).label('c4plus'),
                    func.sum(func.case((LimitUpRecord.open_count > 0, 1), else_=0)).label('breaks')
                ).where(LimitUpRecord.trade_date == today)
                
                result = await db.execute(query)
                stats = result.one()
                
                # 计算炸板率
                total = stats.total or 0
                breaks = stats.breaks or 0
                break_rate = round(breaks / total * 100, 2) if total > 0 else 0
                
                # 保存统计数据
                daily_stats = DailyStatistics(
                    trade_date=today,
                    total_limit_up=total,
                    new_limit_up=stats.new or 0,
                    continuous_2=stats.c2 or 0,
                    continuous_3=stats.c3 or 0,
                    continuous_4_plus=stats.c4plus or 0,
                    break_count=breaks,
                    break_rate=break_rate
                )
                
                db.add(daily_stats)
                await db.commit()
                
                logger.info(f"Daily stats calculated: {total} limit up stocks")
        
        except Exception as e:
            logger.error(f"Calculate daily stats error: {e}")
    
    async def _clear_daily_cache(self):
        """清理每日缓存"""
        try:
            from app.analyzers.limit_up_analyzer import limit_up_analyzer
            from app.analyzers.big_order_analyzer import big_order_analyzer
            
            limit_up_analyzer.clear_cache()
            big_order_analyzer.clear_cache()
            
            logger.info("Daily cache cleared")
        
        except Exception as e:
            logger.error(f"Clear cache error: {e}")


# 全局调度器实例
data_scheduler = DataScheduler()
