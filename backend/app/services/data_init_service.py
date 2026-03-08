"""
数据初始化服务 - 启动时自动爬取最近交易日数据
支持多数据源融合：开盘啦（涨停原因）+ 同花顺（涨停原因）+ 东方财富（基础数据）
"""
import asyncio
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from loguru import logger

from app.database import async_session_maker
from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord
from app.crawlers.eastmoney_crawler import em_crawler
from app.crawlers.tonghuashun_crawler import ths_crawler
from app.crawlers.kaipanla_crawler import kpl_crawler
from app.utils.real_turnover import calc_real_turnover_rate


class DataInitService:
    """数据初始化服务"""
    
    def __init__(self):
        self.is_running = False
    
    async def check_data_exists(self, db: AsyncSession, trade_date: date = None) -> bool:
        """检查指定日期是否有数据"""
        if trade_date is None:
            trade_date = date.today()
        
        result = await db.execute(
            select(func.count(LimitUpRecord.id))
            .where(LimitUpRecord.trade_date == trade_date)
        )
        count = result.scalar()
        return count > 0
    
    async def get_latest_trade_date(self) -> date:
        """获取最近的交易日（排除周末）"""
        today = date.today()
        
        # 如果今天是周末，回退到周五
        weekday = today.weekday()
        if weekday == 5:  # 周六
            return today - timedelta(days=1)
        elif weekday == 6:  # 周日
            return today - timedelta(days=2)
        
        # 如果是交易日但还没开盘（9:30前），使用上一个交易日
        now = datetime.now()
        if now.hour < 9 or (now.hour == 9 and now.minute < 30):
            if weekday == 0:  # 周一
                return today - timedelta(days=3)  # 回退到周五
            else:
                return today - timedelta(days=1)
        
        return today
    
    async def fetch_and_save_data(self, trade_date: date = None) -> int:
        """爬取并保存涨停数据（多数据源融合）"""
        if trade_date is None:
            trade_date = await self.get_latest_trade_date()
        
        logger.info(f"开始爬取 {trade_date} 的涨停数据...")
        
        try:
            # 数据源优先级：开盘啦/同花顺（涨停原因）> 东方财富（基础数据）
            
            kpl_data = []
            ths_data = []
            em_data = []
            
            # 1. 从开盘啦获取数据（涨停原因最准确）
            try:
                logger.info("尝试从开盘啦获取涨停数据...")
                kpl_data = await kpl_crawler.crawl()
                if kpl_data:
                    logger.info(f"开盘啦返回 {len(kpl_data)} 条数据")
            except Exception as e:
                logger.warning(f"开盘啦爬取失败: {e}")
            finally:
                await kpl_crawler.close_client()
            
            # 2. 从同花顺获取数据（涨停原因备选）
            try:
                logger.info("尝试从同花顺获取涨停数据...")
                ths_data = await ths_crawler.crawl()
                if ths_data:
                    logger.info(f"同花顺返回 {len(ths_data)} 条数据")
            except Exception as e:
                logger.warning(f"同花顺爬取失败: {e}")
            finally:
                await ths_crawler.close_client()
            
            # 3. 从东方财富获取数据（作为基础数据源或备用）
            try:
                logger.info("从东方财富获取涨停数据...")
                em_data = await em_crawler.crawl(trade_date)
                if em_data:
                    logger.info(f"东方财富返回 {len(em_data)} 条数据")
            except Exception as e:
                logger.warning(f"东方财富爬取失败: {e}")
            finally:
                await em_crawler.close_client()
            
            # 4. 融合数据（优先使用开盘啦/同花顺的涨停原因）
            merged_data = self._merge_data_sources(kpl_data, ths_data, em_data)
            
            if not merged_data:
                logger.warning("未爬取到任何数据")
                return 0
            
            logger.info(f"融合后共 {len(merged_data)} 条涨停数据")
            
            # 保存到数据库
            saved_count = await self._save_to_database(merged_data, trade_date)
            
            logger.info(f"成功保存 {saved_count} 条涨停记录")
            
            # 计算真实换手率（成交量/自由流通股本）
            await self._update_real_turnover_rates(trade_date)
            
            return saved_count
            
        except Exception as e:
            logger.error(f"爬取数据失败: {e}")
            return 0
    
    def _merge_data_sources(self, kpl_data: List[Dict], ths_data: List[Dict], em_data: List[Dict]) -> List[Dict]:
        """
        融合多个数据源的数据
        
        策略：
        - 以东方财富为基础数据（更稳定）
        - 用开盘啦的涨停原因覆盖（最准确）
        - 同花顺作为开盘啦的备选
        """
        # 如果东方财富没数据，尝试使用其他数据源
        if not em_data:
            if kpl_data:
                return kpl_data
            if ths_data:
                return ths_data
            return []
        
        # 创建开盘啦数据索引（按股票代码）
        kpl_map: Dict[str, Dict] = {}
        for item in kpl_data:
            code = item.get("stock_code", "")
            if code:
                kpl_map[code] = item
        
        # 创建同花顺数据索引（按股票代码）
        ths_map: Dict[str, Dict] = {}
        for item in ths_data:
            code = item.get("stock_code", "")
            if code:
                ths_map[code] = item
        
        # 融合数据
        merged = []
        for em_item in em_data:
            code = em_item.get("stock_code", "")
            if not code:
                continue
            
            # 优先使用开盘啦的涨停原因
            kpl_item = kpl_map.get(code)
            ths_item = ths_map.get(code)
            
            reason = ""
            category = "其他"
            source = "EM"
            
            if kpl_item:
                kpl_reason = kpl_item.get("limit_up_reason", "")
                kpl_category = kpl_item.get("reason_category", "")
                if kpl_reason:
                    reason = kpl_reason
                    category = kpl_category if kpl_category and kpl_category != "其他" else category
                    source = "KPL+EM"
            
            if not reason and ths_item:
                ths_reason = ths_item.get("limit_up_reason", "")
                ths_category = ths_item.get("reason_category", "")
                if ths_reason:
                    reason = ths_reason
                    category = ths_category if ths_category and ths_category != "其他" else category
                    source = "THS+EM"
            
            # 更新涨停原因
            if reason:
                em_item["limit_up_reason"] = reason
            if category != "其他":
                em_item["reason_category"] = category
            em_item["data_source"] = source
            
            # 优先使用同花顺的换手率（数据源直接提供，基于流通股本计算）
            if ths_item:
                ths_turnover = ths_item.get("turnover_rate", 0)
                if ths_turnover and ths_turnover > 0:
                    em_item["turnover_rate"] = ths_turnover
            
            merged.append(em_item)
        
        return merged
    
    def _validate_turnover_rate(self, rate) -> Optional[float]:
        """
        校验换手率合理性
        
        统一换手率格式为百分比值，如 5.23 表示 5.23%
        - 输入可能是 5.23 (百分比) 或 0.0523 (小数) 或 523 (放大100倍)
        - 输出统一为 5.23 这样的百分比格式
        """
        if rate is None:
            return None
        
        try:
            rate = float(rate)
        except (TypeError, ValueError):
            return None
        
        if rate <= 0:
            return None
        
        # 如果 > 100，可能是放大了100倍（如523表示5.23%）
        if rate > 100:
            rate = rate / 100
        
        # 如果 < 0.5，可能是小数形式（如0.0523表示5.23%）
        # 正常股票换手率很少低于0.5%
        if rate < 0.5:
            rate = rate * 100
        
        # 最终校验范围（0-100%）
        if rate < 0 or rate > 100:
            return None
        
        return round(rate, 2)
    
    async def _save_to_database(self, data_list: List[Dict], trade_date: date) -> int:
        """保存数据到数据库"""
        saved_count = 0
        
        async with async_session_maker() as db:
            for item in data_list:
                try:
                    stock_code = item.get("stock_code", "")
                    stock_name = item.get("stock_name", "")
                    
                    if not stock_code:
                        continue
                    
                    # 获取或创建股票记录
                    stock = await self._get_or_create_stock(db, stock_code, stock_name, item)
                    
                    # 检查是否已存在涨停记录
                    existing_result = await db.execute(
                        select(LimitUpRecord).where(
                            LimitUpRecord.stock_id == stock.id,
                            LimitUpRecord.trade_date == trade_date
                        )
                    )
                    existing_record = existing_result.scalar_one_or_none()
                    
                    # 校验换手率
                    turnover_rate = self._validate_turnover_rate(item.get("turnover_rate"))
                    
                    if existing_record:
                        # 更新现有记录的换手率（如果有新的有效值）
                        if turnover_rate and turnover_rate > 0:
                            existing_record.turnover_rate = turnover_rate
                        # 更新其他可能变化的字段
                        if item.get("limit_up_reason"):
                            existing_record.limit_up_reason = item.get("limit_up_reason")
                        if item.get("reason_category") and item.get("reason_category") != "其他":
                            existing_record.reason_category = item.get("reason_category")
                        saved_count += 1
                        continue
                    
                    # 创建涨停记录
                    record = LimitUpRecord(
                        stock_id=stock.id,
                        trade_date=trade_date,
                        first_limit_up_time=item.get("first_limit_up_time"),
                        final_seal_time=item.get("final_seal_time"),
                        limit_up_reason=item.get("limit_up_reason", ""),
                        reason_category=item.get("reason_category", "其他"),
                        continuous_limit_up_days=item.get("continuous_limit_up_days", 1),
                        open_count=item.get("open_count", 0),
                        is_final_sealed=item.get("is_final_sealed", True),
                        seal_amount=item.get("seal_amount"),
                        limit_up_price=item.get("limit_up_price"),
                        turnover_rate=turnover_rate,
                        amount=item.get("amount"),
                        data_source=item.get("data_source", "EM"),
                    )
                    db.add(record)
                    saved_count += 1
                    
                except Exception as e:
                    logger.warning(f"保存记录失败 {item.get('stock_code')}: {e}")
                    continue
            
            await db.commit()
        
        return saved_count
    
    async def _get_or_create_stock(self, db: AsyncSession, code: str, name: str, item: Dict) -> Stock:
        """获取或创建股票记录"""
        result = await db.execute(
            select(Stock).where(Stock.stock_code == code)
        )
        stock = result.scalar_one_or_none()
        
        if not stock:
            # 判断市场
            if code.startswith("6"):
                market = "SH"
            else:
                market = "SZ"
            
            stock = Stock(
                stock_code=code,
                stock_name=name,
                market=market,
                is_st="ST" in name or "*ST" in name,
                is_kc=code.startswith("688"),
                is_cy=code.startswith("300"),
            )
            db.add(stock)
            await db.flush()
        
        return stock
    
    async def _update_real_turnover_rates(self, trade_date: date):
        """计算并更新真实换手率（成交量/自由流通股本）"""
        import time
        logger.info("开始计算真实换手率...")
        
        try:
            async with async_session_maker() as db:
                query = (
                    select(LimitUpRecord, Stock)
                    .join(Stock, LimitUpRecord.stock_id == Stock.id)
                    .where(LimitUpRecord.trade_date == trade_date)
                )
                result = await db.execute(query)
                records = result.all()
                
                if not records:
                    return
                
                updated = 0
                for record, stock in records:
                    real_tr, circ_shares = calc_real_turnover_rate(
                        stock.stock_code, stock.market
                    )
                    if real_tr is not None:
                        record.turnover_rate = real_tr
                        if circ_shares:
                            stock.circulating_shares = circ_shares
                        updated += 1
                    time.sleep(0.2)
                
                await db.commit()
                logger.info(f"真实换手率更新完成: {updated}/{len(records)} 条")
        except Exception as e:
            logger.error(f"更新真实换手率失败: {e}")
    
    async def initialize(self):
        """初始化数据 - 启动时调用"""
        if self.is_running:
            return
        
        self.is_running = True
        logger.info("数据初始化服务启动...")
        
        try:
            # 获取最近交易日
            trade_date = await self.get_latest_trade_date()
            logger.info(f"最近交易日: {trade_date}")
            
            # 检查是否有数据
            async with async_session_maker() as db:
                has_data = await self.check_data_exists(db, trade_date)
            
            if has_data:
                logger.info(f"{trade_date} 已有数据，跳过爬取")
            else:
                logger.info(f"{trade_date} 无数据，开始自动爬取...")
                
                # 尝试爬取当天数据，如果失败则向前尝试几天
                count = 0
                for days_back in range(0, 10):  # 最多往前尝试10天
                    try_date = trade_date - timedelta(days=days_back)
                    # 跳过周末
                    if try_date.weekday() >= 5:
                        continue
                    
                    logger.info(f"尝试爬取 {try_date} 的数据...")
                    count = await self.fetch_and_save_data(try_date)
                    if count > 0:
                        logger.info(f"成功爬取 {try_date} 的数据，共 {count} 条记录")
                        break
                
                if count == 0:
                    logger.warning("自动爬取未获取到数据，可能是API限制或网络问题")
        
        except Exception as e:
            logger.error(f"数据初始化失败: {e}")
        finally:
            self.is_running = False


# 全局实例
data_init_service = DataInitService()
