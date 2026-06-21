"""
任务调度器
"""
import asyncio
from datetime import datetime, time, date, timedelta
from typing import List, Dict, Tuple, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.config import settings
from app.services.intelligence_service import intelligence_service
from app.services.market_review_pipeline_service import market_review_pipeline_service
from app.utils.time_utils import CN_TZ, get_market_status, is_trading_time, today_cn


class TradingCalendarLookupError(RuntimeError):
    """Raised when the China trading calendar cannot be loaded reliably."""


def _normalize_trade_calendar_date(raw_value) -> Optional[date]:
    if isinstance(raw_value, date):
        return raw_value
    if hasattr(raw_value, "date"):
        return raw_value.date()
    if isinstance(raw_value, str):
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    return None


def _get_cn_trading_dates(start_date: date, end_date: date) -> List[date]:
    if end_date < start_date:
        return []

    try:
        import akshare as ak

        calendar_df = ak.tool_trade_date_hist_sina()
    except Exception as exc:
        raise TradingCalendarLookupError(
            f"Unable to resolve China trading calendar for market review work: {exc}"
        ) from exc

    if "trade_date" not in calendar_df:
        raise TradingCalendarLookupError(
            "China trading calendar missing trade_date column for market review work"
        )

    trading_dates: List[date] = []
    for raw_value in calendar_df["trade_date"].tolist():
        trade_date = _normalize_trade_calendar_date(raw_value)
        if trade_date is None:
            continue
        if start_date <= trade_date <= end_date:
            trading_dates.append(trade_date)

    return trading_dates


def _resolve_cn_trade_date_for_market_review(current_date: Optional[date] = None) -> Optional[date]:
    resolved_date = current_date or today_cn()
    trading_dates = _get_cn_trading_dates(resolved_date, resolved_date)
    if not trading_dates:
        return None
    return resolved_date


def _resolve_latest_cn_trade_date_for_market_review(
    current_date: Optional[date] = None,
    lookback_days: int = 10,
) -> Optional[date]:
    resolved_date = current_date or today_cn()
    start_date = resolved_date - timedelta(days=lookback_days)
    trading_dates = _get_cn_trading_dates(start_date, resolved_date)
    if not trading_dates:
        return None
    return max(trading_dates)


def _should_run_after_close_catchup(now: Optional[datetime] = None) -> bool:
    current = now or datetime.now(CN_TZ)
    build_time = time(settings.MARKET_REVIEW_BUILD_HOUR, settings.MARKET_REVIEW_BUILD_MINUTE)
    return current.time() >= build_time


def _daily_analysis_after_close_time() -> time:
    build_at = datetime.combine(
        date(2000, 1, 1),
        time(settings.MARKET_REVIEW_BUILD_HOUR, settings.MARKET_REVIEW_BUILD_MINUTE),
    )
    return (build_at + timedelta(minutes=1)).time()


class DataScheduler:
    """数据采集任务调度器"""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._is_running = False
        # 监控股票缓存
        self._monitored_stocks: List[Dict] = []
        self._stocks_cache_time: datetime = datetime.min
        self._STOCKS_CACHE_TTL = 10  # 10秒刷新一次监控列表，优先跟随实时涨停池
    
    def start(self):
        """启动调度器"""
        if self._is_running:
            return
        
        # 盘中Level-2数据采集。线上TDX连接不稳定，默认关闭，按需用环境变量启用。
        if settings.L2_COLLECT_ENABLED:
            self.scheduler.add_job(
                self._collect_l2_data,
                IntervalTrigger(seconds=settings.L2_COLLECT_INTERVAL),
                id="l2_collect",
                name="Level-2数据采集",
                max_instances=1
            )
        else:
            logger.info("Level-2 data collection disabled")
        
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

        # 每个交易日9:00主动刷新通达信涨停播报实时池，避免早盘继续沿用昨日兜底数据。
        self.scheduler.add_job(
            self._refresh_tdx_limit_up_broadcast,
            CronTrigger(hour=9, minute=0, timezone=CN_TZ),
            id="tdx_limit_up_broadcast_refresh",
            name="通达信涨停播报开盘刷新",
            max_instances=1,
        )

        # 收盘后每日分析月表：晚于市场复盘 1 分钟，避免读取到盘中快照
        daily_analysis_after_close_time = _daily_analysis_after_close_time()
        self.scheduler.add_job(
            self._calculate_daily_analysis,
            CronTrigger(
                hour=daily_analysis_after_close_time.hour,
                minute=daily_analysis_after_close_time.minute,
                timezone=CN_TZ,
            ),
            id="daily_analysis",
            name="每日分析月表生成",
            max_instances=1
        )

        self.scheduler.add_job(
            self._archive_limit_up_classification,
            CronTrigger(
                hour=daily_analysis_after_close_time.hour,
                minute=daily_analysis_after_close_time.minute,
                timezone=CN_TZ,
            ),
            id="limit_up_classification_archive",
            name="涨停分类日终归档",
            max_instances=1,
        )

        # 盘中每日分析月表（每个交易日14:50先刷新市场复盘事实，再生成盘中版）
        self.scheduler.add_job(
            self._calculate_intraday_daily_analysis,
            CronTrigger(
                hour=settings.DAILY_ANALYSIS_INTRADAY_HOUR,
                minute=settings.DAILY_ANALYSIS_INTRADAY_MINUTE,
                timezone=CN_TZ,
            ),
            id="daily_analysis_intraday",
            name="每日分析盘中版生成",
            max_instances=1
        )
        
        # 每日缓存清理（每天16:00）
        self.scheduler.add_job(
            self._clear_daily_cache,
            CronTrigger(hour=16, minute=0),
            id="clear_cache",
            name="每日缓存清理"
        )

        if settings.MARKET_REVIEW_ENABLED:
            self.scheduler.add_job(
                self._build_market_review,
                CronTrigger(
                    hour=settings.MARKET_REVIEW_BUILD_HOUR,
                    minute=settings.MARKET_REVIEW_BUILD_MINUTE,
                    timezone=CN_TZ,
                ),
                id="market_review_build",
                name="市场复盘构建",
                max_instances=1,
            )

            if settings.MARKET_REVIEW_REPAIR_ENABLED:
                self.scheduler.add_job(
                    self._repair_market_review,
                    CronTrigger(
                        hour=settings.MARKET_REVIEW_REPAIR_HOUR,
                        minute=settings.MARKET_REVIEW_REPAIR_MINUTE,
                        timezone=CN_TZ,
                    ),
                    id="market_review_repair",
                    name="市场复盘修复",
                    max_instances=1,
                )

        if settings.INTELLIGENCE_ENABLED:
            self.scheduler.add_job(
                self._probe_intelligence,
                IntervalTrigger(seconds=settings.INTELLIGENCE_PROBE_INTERVAL_SECONDS),
                id="intelligence_probe",
                name="知识库轻量探测",
                max_instances=1,
            )

            for hour, minute in ((8, 45), (11, 45), (15, 20), (20, 30)):
                self.scheduler.add_job(
                    self._sync_intelligence,
                    CronTrigger(hour=hour, minute=minute, timezone=CN_TZ),
                    id=f"intelligence_sync_{hour:02d}{minute:02d}",
                    name=f"知识库增量同步 {hour:02d}:{minute:02d}",
                    max_instances=1,
                )

            self.scheduler.add_job(
                self._sync_intelligence,
                DateTrigger(
                    run_date=datetime.now(CN_TZ) + timedelta(seconds=8),
                    timezone=CN_TZ,
                ),
                id="intelligence_startup_sync",
                name="知识库启动补跑",
                max_instances=1,
                replace_existing=True,
            )

        self.scheduler.add_job(
            self._run_after_close_catchup,
            DateTrigger(
                run_date=datetime.now(CN_TZ) + timedelta(seconds=5),
                timezone=CN_TZ,
            ),
            id="after_close_catchup",
            name="收盘后启动补跑",
            max_instances=1,
            replace_existing=True,
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
        """获取需要监控的股票列表（优先实时涨停池，其次数据库），带缓存"""
        now = datetime.now()
        if (now - self._stocks_cache_time).total_seconds() < self._STOCKS_CACHE_TTL and self._monitored_stocks:
            return self._monitored_stocks

        try:
            from app.services.realtime_limit_up_service import realtime_limit_up_service

            realtime_stocks = await realtime_limit_up_service.get_monitored_stocks(db)
            if realtime_stocks:
                self._monitored_stocks = realtime_stocks
                self._stocks_cache_time = now
                logger.info(f"Monitored stocks refreshed from realtime pool: {len(self._monitored_stocks)} stocks")
                return self._monitored_stocks
        except Exception as e:
            logger.warning(f"Load monitored stocks from realtime pool failed, fallback to database: {e}")
        
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

    async def _calculate_daily_analysis(self, trade_date: Optional[date] = None):
        """生成每日分析月表数据"""
        try:
            from app.database import async_session_maker
            from app.services.daily_analysis_service import daily_analysis_service

            resolved_trade_date = trade_date or _resolve_cn_trade_date_for_market_review(today_cn())
            if resolved_trade_date is None:
                logger.info("Skipping daily analysis build because current China date is not a trading day")
                return

            async with async_session_maker() as db:
                await daily_analysis_service.build_for_date(db, resolved_trade_date, session="after_close")

            logger.info(f"Daily analysis calculated: {resolved_trade_date}")
        except Exception as e:
            logger.error(f"Calculate daily analysis error: {e}")

    async def _refresh_tdx_limit_up_broadcast(self):
        """交易日早盘主动刷新通达信涨停播报实时池。"""
        try:
            from app.services.realtime_limit_up_service import realtime_limit_up_service

            current_date = today_cn()
            resolved_trade_date = _resolve_cn_trade_date_for_market_review(current_date)
            if resolved_trade_date is None:
                logger.info("Skipping TDX limit-up broadcast refresh because current China date is not a trading day")
                return

            items = await realtime_limit_up_service.get_fast_limit_up_pool(
                resolved_trade_date,
                wait_for_refresh=True,
                max_cache_age=0,
            )
            logger.info(f"TDX limit-up broadcast refreshed: {resolved_trade_date}, {len(items)} items")
        except Exception as e:
            logger.error(f"TDX limit-up broadcast refresh error: {e}")

    async def _calculate_intraday_daily_analysis(self):
        """生成每日分析盘中版数据。"""
        try:
            from app.database import async_session_maker
            from app.services.daily_analysis_service import daily_analysis_service

            today = today_cn()
            resolved_trade_date = _resolve_cn_trade_date_for_market_review(today)
            if resolved_trade_date is None:
                logger.info("Skipping intraday daily analysis build because current China date is not a trading day")
                return

            if settings.MARKET_REVIEW_ENABLED:
                await market_review_pipeline_service.run_for_date(resolved_trade_date, calc_version=0)

            async with async_session_maker() as db:
                await daily_analysis_service.build_for_date(db, resolved_trade_date, session="intraday")

            logger.info(f"Intraday daily analysis calculated: {resolved_trade_date}")
        except Exception as e:
            logger.error(f"Calculate intraday daily analysis error: {e}")

    async def _sync_intelligence(self):
        """增量同步知识库并刷新每日资讯/杰哥交易模式。"""
        if not settings.INTELLIGENCE_ENABLED:
            return

        try:
            from app.database import async_session_maker

            async with async_session_maker() as db:
                await intelligence_service.sync_all(db)
            logger.info("Knowledge intelligence sync completed")
        except Exception as e:
            logger.error(f"Knowledge intelligence sync error: {e}")

    async def _probe_intelligence(self):
        """轻量探测每日资讯知识库更新，发现变化后后台同步。"""
        if not settings.INTELLIGENCE_ENABLED:
            return

        try:
            from app.database import async_session_maker

            async with async_session_maker() as db:
                result = await intelligence_service.probe_daily_source(db)
            if result.get("changed"):
                intelligence_service.queue_background_sync(force_daily=False, reason="scheduled_probe")
                logger.info(f"Knowledge intelligence update detected: {result}")
            else:
                logger.debug(f"Knowledge intelligence probe unchanged: {result}")
        except Exception as e:
            logger.error(f"Knowledge intelligence probe error: {e}")

    async def _run_after_close_catchup(self):
        """补跑服务启动时错过的收盘后任务。"""
        if not _should_run_after_close_catchup():
            return

        trade_date = _resolve_latest_cn_trade_date_for_market_review(today_cn())
        if trade_date is None:
            logger.info("Skipping after-close catchup because no recent China trading day was found")
            return

        logger.info(f"Running after-close catchup for {trade_date}")
        if settings.MARKET_REVIEW_ENABLED:
            try:
                await self._build_market_review(trade_date)
            except Exception as e:
                logger.error(f"After-close market review catchup failed: {e}")

        await self._calculate_daily_analysis(trade_date)
        await self._archive_limit_up_classification(trade_date)

    async def _archive_limit_up_classification(self, trade_date: Optional[date] = None):
        """归档收盘后的涨停分类快照。"""
        try:
            from app.services.ths_limit_up_classification_service import ths_limit_up_classification_service

            resolved_trade_date = trade_date or _resolve_cn_trade_date_for_market_review(today_cn())
            if resolved_trade_date is None:
                logger.info("Skipping limit-up classification archive because current China date is not a trading day")
                return

            archive = await ths_limit_up_classification_service.archive_daily_classification(resolved_trade_date)
            logger.info(
                "Limit-up classification archived: "
                f"{archive.trade_date}, {archive.total_count} stocks, {archive.group_count} groups"
            )
        except Exception as e:
            logger.error(f"Limit-up classification archive error: {e}")
    
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

    async def _build_market_review(self, trade_date: Optional[date] = None):
        """构建当日市场复盘数据"""
        try:
            resolved_trade_date = trade_date or _resolve_cn_trade_date_for_market_review()
            if resolved_trade_date is None:
                logger.info("Skipping market review build because current China date is not a trading day")
                return
            await market_review_pipeline_service.run_for_date(resolved_trade_date, calc_version=1)
            logger.info("Market review build completed")
        except Exception as e:
            logger.error(f"Market review build error: {e}")
            raise

    async def _repair_market_review(self):
        """修复当日市场复盘数据"""
        try:
            trade_date = _resolve_cn_trade_date_for_market_review()
            if trade_date is None:
                logger.info("Skipping market review repair because current China date is not a trading day")
                return
            await market_review_pipeline_service.run_for_date(trade_date, calc_version=2)
            logger.info("Market review repair completed")
        except Exception as e:
            logger.error(f"Market review repair error: {e}")
            raise


# 全局调度器实例
data_scheduler = DataScheduler()
