"""
涨停相关API - 支持非交易时段自动回退到最近交易日
"""
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, update
from typing import Optional, List, Dict
from datetime import date, datetime
from loguru import logger

from app.database import get_db, async_session_maker
from app.models.stock import Stock
from app.models.limit_up import LimitUpRecord, LimitUpStatusChange
from app.schemas.limit_up import (
    LimitUpRealtime, LimitUpRecord as LimitUpRecordSchema,
    LimitUpDetail, LimitUpHistoryQuery, LimitUpReasonStats,
    LimitUpRealtimeResponse
)
from app.services.realtime_limit_up_service import realtime_limit_up_service
from app.services.ths_limit_up_classification_service import ths_limit_up_classification_service
from app.utils.time_utils import today_cn
from app.utils.trade_date import get_trade_date_with_fallback

router = APIRouter()


def _positive_int_or_default(value, default: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _format_hms(value) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M:%S")
    return str(value)


def _filter_and_sort_limit_up_list(
    limit_up_list: List[LimitUpRealtime],
    continuous_days: Optional[int],
    reason_category: Optional[str],
    sort_by: str,
) -> List[LimitUpRealtime]:
    if continuous_days is not None:
        limit_up_list = [
            item for item in limit_up_list
            if _positive_int_or_default(item.continuous_limit_up_days) >= continuous_days
        ]
    if reason_category:
        limit_up_list = [
            item for item in limit_up_list
            if item.reason_category == reason_category
        ]

    if sort_by == "time":
        limit_up_list.sort(key=lambda x: x.first_limit_up_time or "99:99:99")
    elif sort_by == "seal_amount":
        limit_up_list.sort(key=lambda x: x.seal_amount or 0, reverse=True)
    elif sort_by == "continuous_days":
        limit_up_list.sort(key=lambda x: x.continuous_limit_up_days or 0, reverse=True)

    return limit_up_list


def _record_to_realtime(
    record: LimitUpRecord,
    stock: Stock,
    continuous_days_override: Optional[int] = None,
) -> LimitUpRealtime:
    is_final_sealed = getattr(record, "is_final_sealed", True)
    if is_final_sealed is None:
        is_final_sealed = True
    current_status = getattr(record, "current_status", None)
    if not current_status or current_status == "unknown":
        current_status = "sealed" if is_final_sealed else "opened"

    return LimitUpRealtime(
        stock_code=stock.stock_code,
        stock_name=stock.stock_name,
        trade_date=record.trade_date,
        first_limit_up_time=_format_hms(record.first_limit_up_time),
        final_seal_time=_format_hms(getattr(record, "final_seal_time", None)),
        limit_up_reason=getattr(record, "limit_up_reason", None),
        reason_category=getattr(record, "reason_category", None),
        continuous_limit_up_days=_positive_int_or_default(
            continuous_days_override
            if continuous_days_override is not None
            else getattr(record, "continuous_limit_up_days", None)
        ),
        open_count=_positive_int_or_default(getattr(record, "open_count", 0), default=0),
        is_sealed=is_final_sealed,
        current_status=current_status,
        seal_amount=getattr(record, "seal_amount", None),
        seal_volume=getattr(record, "seal_volume", None),
        limit_up_price=getattr(record, "limit_up_price", None) or 0,
        current_price=(
            getattr(record, "close_price", None)
            or getattr(record, "limit_up_price", None)
            or 0
        ),
        turnover_rate=getattr(record, "turnover_rate", None),
        amount=getattr(record, "amount", None),
        tradable_market_value=None,
        market=stock.market,
        industry=stock.industry,
    )


async def _calculate_strict_database_continuous_days(
    db: AsyncSession,
    actual_date: date,
    rows,
    lookback_days: int = 30,
) -> Dict[int, int]:
    stock_ids = [getattr(record, "stock_id", None) for record, _ in rows]
    stock_ids = [stock_id for stock_id in stock_ids if stock_id is not None]
    if not stock_ids:
        return {}

    date_result = await db.execute(
        select(LimitUpRecord.trade_date)
        .where(LimitUpRecord.trade_date <= actual_date)
        .group_by(LimitUpRecord.trade_date)
        .order_by(LimitUpRecord.trade_date.desc())
        .limit(lookback_days)
    )
    trade_dates = [row[0] for row in date_result.all()]
    if not trade_dates:
        return {}

    streak_result = await db.execute(
        select(
            LimitUpRecord.stock_id,
            LimitUpRecord.trade_date,
            LimitUpRecord.is_final_sealed,
        )
        .where(and_(
            LimitUpRecord.stock_id.in_(stock_ids),
            LimitUpRecord.trade_date.in_(trade_dates),
        ))
    )
    sealed_dates_by_stock: Dict[int, set] = {}
    for stock_id, trade_day, is_final_sealed in streak_result.all():
        if is_final_sealed:
            sealed_dates_by_stock.setdefault(stock_id, set()).add(trade_day)

    strict_days: Dict[int, int] = {}
    for record, _ in rows:
        stock_id = getattr(record, "stock_id", None)
        if stock_id is None:
            continue
        if not getattr(record, "is_final_sealed", True):
            strict_days[stock_id] = 1
            continue

        sealed_dates = sealed_dates_by_stock.get(stock_id, set())
        streak = 0
        for trade_day in trade_dates:
            if trade_day in sealed_dates:
                streak += 1
            else:
                break
        strict_days[stock_id] = max(streak, 1)

    return strict_days


async def _get_database_limit_up_response(
    db: AsyncSession,
    trade_date: date,
    continuous_days: Optional[int],
    reason_category: Optional[str],
    sort_by: str,
) -> LimitUpRealtimeResponse:
    actual_date, is_fallback = await get_trade_date_with_fallback(db, trade_date)
    query = (
        select(LimitUpRecord, Stock)
        .join(Stock, LimitUpRecord.stock_id == Stock.id)
        .where(LimitUpRecord.trade_date == actual_date)
    )
    result = await db.execute(query)
    rows = result.all()
    strict_days = await _calculate_strict_database_continuous_days(db, actual_date, rows)
    limit_up_list = [
        _record_to_realtime(
            record,
            stock,
            continuous_days_override=strict_days.get(getattr(record, "stock_id", None)),
        )
        for record, stock in rows
    ]

    return LimitUpRealtimeResponse(
        trade_date=actual_date,
        is_fallback=is_fallback,
        data=_filter_and_sort_limit_up_list(
            limit_up_list,
            continuous_days,
            reason_category,
            sort_by,
        ),
    )


@router.get("/realtime", response_model=LimitUpRealtimeResponse, summary="获取实时涨停列表")
async def get_realtime_limit_up(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    continuous_days: Optional[int] = Query(None, description="连板天数筛选"),
    reason_category: Optional[str] = Query(None, description="原因分类筛选"),
    sort_by: str = Query("time", description="排序字段(time/seal_amount/continuous_days)"),
    db: AsyncSession = Depends(get_db)
):
    """获取实时涨停列表（东方财富数据+同花顺涨停原因）"""
    current_date = today_cn()
    if trade_date is None:
        trade_date = current_date

    if trade_date != current_date:
        return await _get_database_limit_up_response(
            db,
            trade_date,
            continuous_days,
            reason_category,
            sort_by,
        )

    raw_data = await realtime_limit_up_service.get_realtime_limit_up_list(trade_date)
    
    if not raw_data:
        return await _get_database_limit_up_response(
            db,
            trade_date,
            continuous_days,
            reason_category,
            sort_by,
        )
    
    # 处理实时数据
    limit_up_list = []
    for item in raw_data:
        continuous_limit_up_days = _positive_int_or_default(item.get("continuous_limit_up_days"))
        
        is_sealed = item.get("is_sealed", item.get("is_final_sealed", True))
        current_status = item.get("current_status", "sealed" if is_sealed else "opened")
        
        # 格式化时间
        first_time = item.get("first_limit_up_time")
        final_time = item.get("final_seal_time")
        first_time_str = _format_hms(first_time)
        final_time_str = _format_hms(final_time)
        
        code = item.get("stock_code", "")
        market = item.get("market", "SH" if code.startswith("6") else "SZ")
        
        limit_up_list.append(LimitUpRealtime(
            stock_code=code,
            stock_name=item.get("stock_name", ""),
            trade_date=trade_date,
            first_limit_up_time=first_time_str,
            final_seal_time=final_time_str,
            limit_up_reason=item.get("limit_up_reason", ""),
            reason_category=item.get("reason_category", "其他"),
            continuous_limit_up_days=continuous_limit_up_days,
            open_count=item.get("open_count", 0),
            is_sealed=is_sealed,
            current_status=current_status,
            seal_amount=item.get("seal_amount", 0),
            seal_volume=None,
            limit_up_price=item.get("limit_up_price", 0),
            current_price=item.get("current_price", item.get("limit_up_price", 0)),
            turnover_rate=item.get("turnover_rate", 0),
            amount=item.get("amount", 0),
            tradable_market_value=item.get("tradable_market_value"),
            market=market,
            industry=item.get("industry")
        ))
    
    return LimitUpRealtimeResponse(
        trade_date=trade_date,
        is_fallback=False,
        data=_filter_and_sort_limit_up_list(
            limit_up_list,
            continuous_days,
            reason_category,
            sort_by,
        )
    )


@router.get("/classification", summary="获取同花顺涨停原因板块分类")
async def get_limit_up_classification(
    background_tasks: BackgroundTasks,
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    force_ai: bool = Query(False, description="是否强制重新生成 DeepSeek 分类"),
    db: AsyncSession = Depends(get_db)
):
    """按同花顺涨停原因做板块分类，展示首封和回封时间。"""
    if trade_date is None:
        trade_date = date.today()
    payload = await ths_limit_up_classification_service.get_classification(
        trade_date,
        db=db,
        use_archive=not force_ai,
    )
    if force_ai:
        background_tasks.add_task(ths_limit_up_classification_service.rebuild_ai_classification_cache, trade_date)
        payload.setdefault("source_status", {})["ai_classification"] = "refresh_scheduled"
    return payload


@router.get("/{stock_code}", response_model=LimitUpDetail, summary="获取涨停详情")
async def get_limit_up_detail(
    stock_code: str,
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db)
):
    """获取单只股票的涨停详情（优先从实时数据获取）"""
    if trade_date is None:
        trade_date = date.today()
    realtime_record = await realtime_limit_up_service.get_realtime_limit_up_item(stock_code, trade_date)
    
    if realtime_record:
        # 从实时数据构建响应
        code = realtime_record.get("stock_code", "")
        market = realtime_record.get("market", "SH" if code.startswith("6") else "SZ")
        
        first_time = realtime_record.get("first_limit_up_time")
        final_time = realtime_record.get("final_seal_time")
        
        return LimitUpDetail(
            id=0,
            stock_code=code,
            stock_name=realtime_record.get("stock_name", ""),
            trade_date=trade_date,
            first_limit_up_time=first_time,
            final_seal_time=final_time,
            limit_up_reason=realtime_record.get("limit_up_reason", ""),
            reason_category=realtime_record.get("reason_category", "其他"),
            continuous_limit_up_days=realtime_record.get("continuous_limit_up_days", 1),
            open_count=realtime_record.get("open_count", 0),
            is_final_sealed=realtime_record.get("is_final_sealed", True),
            current_status=realtime_record.get(
                "current_status",
                "sealed" if realtime_record.get("is_final_sealed", True) else "opened"
            ),
            seal_amount=realtime_record.get("seal_amount", 0),
            limit_up_price=realtime_record.get("limit_up_price", 0),
            turnover_rate=realtime_record.get("turnover_rate", 0),
            amount=realtime_record.get("amount", 0),
            tradable_market_value=realtime_record.get("tradable_market_value"),
            data_source="EM+THS+Tencent",
            market=market,
            industry=realtime_record.get("industry"),
            status_changes=[]
        )
    
    # 回退到数据库查询
    stock_query = select(Stock).where(Stock.stock_code == stock_code)
    stock_result = await db.execute(stock_query)
    stock = stock_result.scalar_one_or_none()
    
    if not stock:
        raise HTTPException(status_code=404, detail="股票不存在")
    
    # 查询涨停记录
    record_query = (
        select(LimitUpRecord)
        .where(and_(
            LimitUpRecord.stock_id == stock.id,
            LimitUpRecord.trade_date <= trade_date
        ))
        .order_by(LimitUpRecord.trade_date.desc())
        .limit(1)
    )
    record_result = await db.execute(record_query)
    record = record_result.scalar_one_or_none()
    
    if not record:
        raise HTTPException(status_code=404, detail=f"{trade_date} 没有涨停记录")
    
    # 查询状态变化
    changes_query = (
        select(LimitUpStatusChange)
        .where(LimitUpStatusChange.limit_up_record_id == record.id)
        .order_by(LimitUpStatusChange.change_time)
    )
    changes_result = await db.execute(changes_query)
    changes = changes_result.scalars().all()
    
    return LimitUpDetail(
        id=record.id,
        stock_code=stock.stock_code,
        stock_name=stock.stock_name,
        trade_date=record.trade_date,
        first_limit_up_time=record.first_limit_up_time,
        final_seal_time=getattr(record, 'final_seal_time', None),
        limit_up_reason=record.limit_up_reason,
        reason_category=record.reason_category,
        continuous_limit_up_days=record.continuous_limit_up_days,
        open_count=record.open_count,
        is_final_sealed=record.is_final_sealed,
        current_status=getattr(record, 'current_status', None) or ("sealed" if record.is_final_sealed else "opened"),
        seal_amount=record.seal_amount,
        limit_up_price=record.limit_up_price,
        turnover_rate=record.turnover_rate,
        amount=record.amount,
        tradable_market_value=None,
        data_source=record.data_source,
        market=stock.market,
        industry=stock.industry,
        status_changes=[
            {
                "change_time": c.change_time,
                "status": c.status,
                "price": c.price,
                "seal_amount": c.seal_amount
            }
            for c in changes
        ]
    )


@router.get("/history", response_model=List[LimitUpRecordSchema], summary="获取历史涨停")
async def get_limit_up_history(
    stock_code: Optional[str] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    reason_category: Optional[str] = Query(None),
    min_continuous_days: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db)
):
    """获取历史涨停记录"""
    query = select(LimitUpRecord, Stock).join(Stock, LimitUpRecord.stock_id == Stock.id)
    
    # 添加筛选条件
    conditions = []
    
    if stock_code:
        conditions.append(Stock.stock_code == stock_code)
    
    if start_date:
        conditions.append(LimitUpRecord.trade_date >= start_date)
    
    if end_date:
        conditions.append(LimitUpRecord.trade_date <= end_date)
    
    if reason_category:
        conditions.append(LimitUpRecord.reason_category == reason_category)
    
    if min_continuous_days:
        conditions.append(LimitUpRecord.continuous_limit_up_days >= min_continuous_days)
    
    if conditions:
        query = query.where(and_(*conditions))
    
    # 分页
    query = query.order_by(LimitUpRecord.trade_date.desc(), LimitUpRecord.first_limit_up_time)
    query = query.offset((page - 1) * page_size).limit(page_size)
    
    result = await db.execute(query)
    records = result.all()
    
    return [
        LimitUpRecordSchema(
            id=record.id,
            stock_code=stock.stock_code,
            stock_name=stock.stock_name,
            trade_date=record.trade_date,
            first_limit_up_time=record.first_limit_up_time,
            limit_up_reason=record.limit_up_reason,
            reason_category=record.reason_category,
            continuous_limit_up_days=record.continuous_limit_up_days,
            open_count=record.open_count,
            is_final_sealed=record.is_final_sealed,
            seal_amount=record.seal_amount,
            limit_up_price=record.limit_up_price,
            turnover_rate=record.turnover_rate,
            amount=record.amount,
            data_source=record.data_source
        )
        for record, stock in records
    ]


@router.get("/reasons/statistics", response_model=List[LimitUpReasonStats], summary="获取涨停原因统计")
async def get_limit_up_reasons_statistics(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    db: AsyncSession = Depends(get_db)
):
    """获取涨停原因分类统计"""
    if trade_date is None:
        trade_date = date.today()
    
    # 获取实际有数据的交易日期
    actual_date, _ = await get_trade_date_with_fallback(db, trade_date)
    
    # 统计各分类数量
    query = (
        select(
            LimitUpRecord.reason_category,
            func.count(LimitUpRecord.id).label('count')
        )
        .where(LimitUpRecord.trade_date == actual_date)
        .group_by(LimitUpRecord.reason_category)
    )
    
    result = await db.execute(query)
    stats = result.all()
    
    # 计算总数
    total = sum(s.count for s in stats)
    
    # 获取每个分类的股票列表
    reason_stats = []
    for stat in stats:
        if not stat.reason_category:
            continue
        
        # 查询该分类的股票
        stocks_query = (
            select(Stock.stock_code)
            .join(LimitUpRecord, LimitUpRecord.stock_id == Stock.id)
            .where(and_(
                LimitUpRecord.trade_date == actual_date,
                LimitUpRecord.reason_category == stat.reason_category
            ))
        )
        stocks_result = await db.execute(stocks_query)
        stocks = [s[0] for s in stocks_result.all()]
        
        reason_stats.append(LimitUpReasonStats(
            reason_category=stat.reason_category,
            count=stat.count,
            percentage=round(stat.count / total * 100, 2) if total > 0 else 0,
            stocks=stocks
        ))
    
    # 按数量排序
    reason_stats.sort(key=lambda x: x.count, reverse=True)
    
    return reason_stats


async def _refresh_limit_up_data(trade_date: date):
    """后台刷新涨停数据任务"""
    from app.crawlers.kaipanla_crawler import kpl_crawler
    from app.crawlers.tonghuashun_crawler import ths_crawler
    from loguru import logger
    
    logger.info(f"开始刷新 {trade_date} 的涨停原因和状态...")
    
    try:
        # 获取开盘啦和同花顺数据
        kpl_data = []
        ths_data = []
        
        try:
            kpl_data = await kpl_crawler.crawl()
            logger.info(f"开盘啦返回 {len(kpl_data)} 条数据")
        except Exception as e:
            logger.warning(f"开盘啦爬取失败: {e}")
        finally:
            await kpl_crawler.close_client()
        
        try:
            ths_data = await ths_crawler.crawl()
            logger.info(f"同花顺返回 {len(ths_data)} 条数据")
        except Exception as e:
            logger.warning(f"同花顺爬取失败: {e}")
        finally:
            await ths_crawler.close_client()
        
        # 合并数据索引
        data_map = {}
        for item in ths_data:
            code = item.get("stock_code", "")
            if code:
                data_map[code] = item
        for item in kpl_data:  # 开盘啦优先覆盖
            code = item.get("stock_code", "")
            if code:
                data_map[code] = item
        
        if not data_map:
            logger.warning("未获取到任何更新数据")
            return
        
        # 更新数据库
        async with async_session_maker() as db:
            # 查询当天的涨停记录
            query = (
                select(LimitUpRecord, Stock)
                .join(Stock, LimitUpRecord.stock_id == Stock.id)
                .where(LimitUpRecord.trade_date == trade_date)
            )
            result = await db.execute(query)
            records = result.all()
            
            updated_count = 0
            for record, stock in records:
                new_data = data_map.get(stock.stock_code)
                if new_data:
                    # 更新涨停原因
                    reason = new_data.get("limit_up_reason", "")
                    category = new_data.get("reason_category", "")
                    
                    if reason:
                        record.limit_up_reason = reason
                    if category and category != "其他":
                        record.reason_category = category
                    
                    # 更新状态和封单相关字段
                    is_sealed = new_data.get("is_final_sealed")
                    open_count = new_data.get("open_count")
                    seal_amount = new_data.get("seal_amount")
                    
                    if is_sealed is not None:
                        record.is_final_sealed = is_sealed
                        record.current_status = "sealed" if is_sealed else "opened"
                    
                    if open_count is not None:
                        record.open_count = open_count
                        if is_sealed is None:
                            record.current_status = "sealed" if record.is_final_sealed else "opened"

                    if seal_amount is not None:
                        record.seal_amount = seal_amount
                    
                    # 更新最后封板时间
                    final_seal_time = new_data.get("final_seal_time")
                    if final_seal_time is not None:
                        record.final_seal_time = final_seal_time
                    
                    # 更新数据来源
                    source = new_data.get("data_source", "")
                    if source:
                        record.data_source = source
                    
                    updated_count += 1
            
            await db.commit()
            logger.info(f"成功更新 {updated_count} 条涨停记录")
            
    except Exception as e:
        logger.error(f"刷新涨停数据失败: {e}")


@router.post("/refresh", summary="刷新涨停原因和状态")
async def refresh_limit_up_data(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db)
):
    """
    手动刷新涨停原因和状态
    
    从开盘啦和同花顺重新获取数据，更新：
    - 涨停原因
    - 涨停分类
    - 封板/开板状态
    - 开板次数
    """
    if trade_date is None:
        trade_date = date.today()
    
    # 检查是否有数据
    count_query = select(func.count(LimitUpRecord.id)).where(LimitUpRecord.trade_date == trade_date)
    result = await db.execute(count_query)
    count = result.scalar()
    
    if count == 0:
        raise HTTPException(status_code=404, detail=f"{trade_date} 没有涨停数据，请先爬取")
    
    # 后台执行刷新
    if background_tasks:
        background_tasks.add_task(_refresh_limit_up_data, trade_date)
        return {
            "code": 0,
            "message": f"已开始后台刷新 {trade_date} 的涨停数据（共 {count} 条）",
            "data": {"trade_date": str(trade_date), "total_records": count}
        }
    else:
        # 同步执行
        await _refresh_limit_up_data(trade_date)
        return {
            "code": 0,
            "message": f"刷新完成",
            "data": {"trade_date": str(trade_date), "total_records": count}
        }


@router.post("/refetch", summary="重新爬取涨停数据")
async def refetch_limit_up_data(
    trade_date: Optional[date] = Query(None, description="交易日期，默认今天"),
    background_tasks: BackgroundTasks = None
):
    """
    重新爬取涨停数据（会覆盖现有数据）
    
    从所有数据源重新获取并保存涨停数据
    """
    from app.services.data_init_service import data_init_service
    
    if trade_date is None:
        trade_date = date.today()
    
    # 后台执行爬取
    if background_tasks:
        background_tasks.add_task(data_init_service.fetch_and_save_data, trade_date)
        return {
            "code": 0,
            "message": f"已开始后台爬取 {trade_date} 的涨停数据",
            "data": {"trade_date": str(trade_date)}
        }
    else:
        count = await data_init_service.fetch_and_save_data(trade_date)
        return {
            "code": 0,
            "message": f"爬取完成，共 {count} 条记录",
            "data": {"trade_date": str(trade_date), "saved_count": count}
        }
