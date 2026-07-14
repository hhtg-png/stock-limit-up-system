"""
任务调度器
"""
import asyncio
import uuid
from datetime import datetime, time, date, timedelta
from typing import Any, Awaitable, Callable, List, Dict, Tuple, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.config import settings
from app.services.intelligence_service import intelligence_service
from app.services.market_review_pipeline_service import market_review_pipeline_service
from app.services.trading_playbook.calendar_service import (
    TradingCalendarLookupError,
    TradingCalendarService,
)
from app.services.trading_playbook.job_claim_service import (
    TradingPlaybookJobClaimService,
)
from app.utils.time_utils import CN_TZ, get_market_status, is_trading_time, today_cn


_PLAYBOOK_NOTIFICATION_RETRY_BATCH_SIZE = 100


class TradingPlaybookClaimLost(RuntimeError):
    """Raised when a running phase no longer owns its database lease."""


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
    
    def __init__(
        self,
        *,
        trading_playbook_orchestrator: Any = None,
        trading_playbook_alert_service: Any = None,
        trading_playbook_review_service: Any = None,
        session_factory: Optional[Callable[[], Any]] = None,
        now_provider: Optional[Callable[[], datetime]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        calendar_service: Optional[TradingCalendarService] = None,
        job_claim_service: Optional[TradingPlaybookJobClaimService] = None,
        claim_owner: Optional[str] = None,
        scheduler_factory: Optional[Callable[[], Any]] = None,
        monotonic: Optional[Callable[[], float]] = None,
    ):
        self._scheduler_factory = scheduler_factory or AsyncIOScheduler
        self.scheduler = self._scheduler_factory()
        self._is_running = False
        self._trading_playbook_orchestrator = trading_playbook_orchestrator
        self._trading_playbook_alert_service = trading_playbook_alert_service
        self._trading_playbook_review_service = trading_playbook_review_service
        self._playbook_session_factory = session_factory
        self._playbook_now_provider = now_provider or (
            lambda: datetime.now(CN_TZ)
        )
        self._playbook_sleep = sleep or asyncio.sleep
        self._playbook_monotonic = monotonic
        self._playbook_calendar = calendar_service or TradingCalendarService(
            loader=lambda start, end: _get_cn_trading_dates(start, end),
            refresh_timeout_seconds=(
                settings.TRADING_PLAYBOOK_CALENDAR_REFRESH_TIMEOUT_SECONDS
            ),
            retry_interval_seconds=(
                settings.TRADING_PLAYBOOK_CALENDAR_RETRY_INTERVAL_SECONDS
            ),
        )
        self._playbook_upgrade_lock = asyncio.Lock()
        self._playbook_job_claims = (
            job_claim_service
            or TradingPlaybookJobClaimService(
                lease_seconds=settings.TRADING_PLAYBOOK_JOB_LEASE_SECONDS
            )
        )
        self._playbook_claim_owner = claim_owner or uuid.uuid4().hex
        # 监控股票缓存
        self._monitored_stocks: List[Dict] = []
        self._stocks_cache_time: datetime = datetime.min
        self._STOCKS_CACHE_TTL = 10  # 10秒刷新一次监控列表，优先跟随实时涨停池
    
    def start(self):
        """Register and start all jobs, leaving a clean scheduler on failure."""
        if self._is_running or self._scheduler_is_running(self.scheduler):
            self._is_running = True
            return
        try:
            self._start_registered_scheduler()
        except BaseException:
            self._replace_scheduler()
            raise

    def _start_registered_scheduler(self):
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

        if settings.TRADING_PLAYBOOK_ENABLED:
            self._register_trading_playbook_jobs()

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

    def _register_trading_playbook_jobs(self) -> None:
        jobs = (
            (
                self._build_trading_playbook_preclose,
                CronTrigger(hour=14, minute=40, timezone=CN_TZ),
                "trading_playbook_preclose",
                "交易作战手册14:40预案",
                19 * 60,
            ),
            (
                self._review_trading_playbook,
                CronTrigger(hour=15, minute=10, timezone=CN_TZ),
                "trading_playbook_review",
                "交易作战手册15:10复盘",
                19 * 60,
            ),
            (
                self._build_trading_playbook_after_close,
                CronTrigger(hour=15, minute=30, timezone=CN_TZ),
                "trading_playbook_after_close",
                "交易作战手册15:30定稿",
                30 * 60,
            ),
            (
                self._build_trading_playbook_overnight,
                CronTrigger(hour=8, minute=50, timezone=CN_TZ),
                "trading_playbook_overnight",
                "交易作战手册08:50隔夜刷新",
                35 * 60,
            ),
            (
                self._build_trading_playbook_auction,
                CronTrigger(hour=9, minute=26, timezone=CN_TZ),
                "trading_playbook_auction",
                "交易作战手册09:26竞价确认",
                3 * 60,
            ),
            (
                self._monitor_trading_playbook,
                IntervalTrigger(
                    seconds=settings.TRADING_PLAYBOOK_MONITOR_INTERVAL_SECONDS,
                    timezone=CN_TZ,
                ),
                "trading_playbook_monitor",
                "交易作战手册盘中监控",
                self._calendar_misfire_seconds(),
            ),
        )
        for func, trigger, job_id, name, misfire_seconds in jobs:
            self.scheduler.add_job(
                func,
                trigger,
                id=job_id,
                name=name,
                max_instances=1,
                misfire_grace_time=misfire_seconds,
                coalesce=True,
            )
        self.scheduler.add_job(
            self._run_trading_playbook_catchup,
            DateTrigger(
                run_date=datetime.now(CN_TZ) + timedelta(seconds=6),
                timezone=CN_TZ,
            ),
            id="trading_playbook_startup_catchup",
            name="交易作战手册启动补跑",
            max_instances=1,
            replace_existing=True,
            misfire_grace_time=60,
            coalesce=True,
        )
    
    def stop(self):
        """停止调度器"""
        was_running = self._is_running or self._scheduler_is_running(
            self.scheduler
        )
        self._replace_scheduler()
        if was_running:
            logger.info("DataScheduler stopped")

    @staticmethod
    def _scheduler_is_running(scheduler: Any) -> bool:
        try:
            return bool(getattr(scheduler, "running", False))
        except Exception:
            return False

    def _replace_scheduler(self) -> None:
        scheduler = self.scheduler
        remove_all_jobs = getattr(scheduler, "remove_all_jobs", None)
        if callable(remove_all_jobs):
            try:
                remove_all_jobs()
            except Exception as exc:
                logger.warning("Unable to clear scheduler jobs: {}", exc)
        if self._is_running or self._scheduler_is_running(scheduler):
            shutdown = getattr(scheduler, "shutdown", None)
            if callable(shutdown):
                try:
                    shutdown()
                except Exception as exc:
                    logger.warning("Unable to shutdown scheduler: {}", exc)
        self._is_running = False
        self.scheduler = self._scheduler_factory()

    def install_trading_playbook_orchestrator(self, orchestrator: Any) -> None:
        if not callable(getattr(orchestrator, "build_stage", None)):
            raise TypeError("orchestrator must provide build_stage")
        self._trading_playbook_orchestrator = orchestrator

    def install_trading_playbook_alert_service(self, service: Any) -> None:
        if getattr(service, "durable_delivery", None) is not True:
            raise TypeError(
                "alert service must declare durable_delivery=True"
            )
        if not (
            callable(getattr(service, "notify_plan_ready", None))
            or callable(getattr(service, "monitor", None))
        ):
            raise TypeError(
                "alert service must provide notify_plan_ready or monitor"
            )
        self._trading_playbook_alert_service = service

    def install_trading_playbook_review_service(self, service: Any) -> None:
        if not callable(getattr(service, "build", None)):
            raise TypeError("review service must provide build")
        self._trading_playbook_review_service = service

    def reset_trading_playbook_services(self) -> None:
        self._trading_playbook_orchestrator = None
        self._trading_playbook_alert_service = None
        self._trading_playbook_review_service = None

    def get_trading_playbook_orchestrator(self) -> Any:
        return self._trading_playbook_orchestrator

    def _playbook_sessions(self):
        if self._playbook_session_factory is not None:
            return self._playbook_session_factory()
        from app.database import async_session_maker

        return async_session_maker()

    def _playbook_now(self) -> datetime:
        value = self._playbook_now_provider()
        if not isinstance(value, datetime):
            raise TypeError("trading playbook clock must return a datetime")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("trading playbook clock must be timezone-aware")
        return value.astimezone(CN_TZ)

    @staticmethod
    def _calendar_misfire_seconds() -> int:
        return max(settings.TRADING_PLAYBOOK_MONITOR_INTERVAL_SECONDS * 3, 15)

    async def _ensure_playbook_calendar(self, value: date) -> bool:
        await self._playbook_calendar.ensure_date(value)
        return self._is_cn_trading_day(value)

    def _is_cn_trading_day(self, value: date) -> bool:
        return self._playbook_calendar.is_trading_day(value)

    def _next_cn_trading_date(self, value: date) -> date:
        return self._playbook_calendar.next_trade_date(value)

    def get_trading_calendar_service(self) -> TradingCalendarService:
        return self._playbook_calendar

    async def _build_trading_playbook_plan(
        self,
        stage: str,
        *,
        degraded: bool = False,
        degradation_reason: Optional[str] = None,
        send_notifications: bool = True,
    ):
        now = self._playbook_now()
        source_trade_date = now.date()
        if not await self._ensure_playbook_calendar(source_trade_date):
            logger.info(
                "Skipping trading playbook {} because {} is not a China trading day",
                stage,
                source_trade_date,
            )
            return None
        orchestrator = self._trading_playbook_orchestrator
        if orchestrator is None:
            logger.warning(
                "Skipping trading playbook {} because orchestrator is not installed",
                stage,
            )
            return None
        target_trade_date = (
            self._next_cn_trading_date(source_trade_date)
            if stage in {"preclose", "after_close"}
            else source_trade_date
        )
        generation_key = degradation_reason or (
            "forced" if degraded else "ready"
        )
        job_key = (
            f"playbook:build:{source_trade_date}:{target_trade_date}:"
            f"{stage}:{generation_key}"
        )
        async with self._playbook_sessions() as db:
            token = await self._playbook_job_claims.claim(
                db,
                job_key=job_key,
                job_type="stage",
                phase="build",
                owner=self._playbook_claim_owner,
                now=self._claim_now(),
                source_trade_date=source_trade_date,
                target_trade_date=target_trade_date,
                stage=stage,
                generation_key=generation_key,
            )
            if token is None:
                plan = await self._latest_stage_plan_in_session(
                    db,
                    target_trade_date,
                    stage,
                )
            else:
                try:
                    build_kwargs = {"degraded": degraded}
                    if degradation_reason is not None:
                        build_kwargs["degradation_reason"] = degradation_reason
                    plan = await self._run_with_playbook_claim(
                        token,
                        lambda: orchestrator.build_stage(
                            db,
                            source_trade_date,
                            stage,
                            now,
                            **build_kwargs,
                        ),
                    )
                except asyncio.CancelledError as exc:
                    await self._rollback_playbook_session(db)
                    await self._fail_playbook_claim_fresh(token, exc)
                    raise
                except Exception as exc:
                    await self._rollback_playbook_session(db)
                    await self._fail_playbook_claim_fresh(token, exc)
                    if isinstance(exc, TradingPlaybookClaimLost):
                        logger.error("{}", exc)
                        return None
                    raise
                completed = await self._playbook_job_claims.complete(
                    db,
                    token,
                    now=self._claim_now(),
                )
                if not completed:
                    logger.error("Trading playbook build claim lost before completion")
                    return None
        if send_notifications and plan is not None:
            await self._notify_trading_playbook_plan(plan)
        return plan

    def _claim_now(self) -> datetime:
        return self._playbook_now().replace(tzinfo=None)

    @staticmethod
    async def _rollback_playbook_session(db) -> None:
        try:
            await db.rollback()
        except Exception as exc:
            logger.error("Trading playbook session rollback failed: {}", exc)

    async def _fail_playbook_claim_fresh(self, token, error) -> bool:
        """Mark retry without relying on a possibly broken business session."""
        try:
            async with self._playbook_sessions() as fail_db:
                return await self._playbook_job_claims.fail(
                    fail_db,
                    token,
                    now=self._claim_now(),
                    error=error,
                )
        except Exception as fail_error:
            logger.error(
                "Trading playbook fresh claim failure update failed for {}: {}",
                token.job_key,
                fail_error,
            )
            return False

    async def _run_with_playbook_claim(self, token, operation):
        """Run work behind a renewable lease and cancel immediately if fenced."""
        interval = max(
            min(float(self._playbook_job_claims.lease_seconds) / 3.0, 30.0),
            0.05,
        )
        work_task = asyncio.create_task(operation())

        async def heartbeat():
            while True:
                await asyncio.sleep(interval)
                async with self._playbook_sessions() as heartbeat_db:
                    renewed = await self._playbook_job_claims.renew(
                        heartbeat_db,
                        token,
                        now=self._claim_now(),
                    )
                if not renewed:
                    raise TradingPlaybookClaimLost(
                        f"playbook claim lost: {token.job_key}"
                    )

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            done, _pending = await asyncio.wait(
                {work_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                error = heartbeat_task.exception()
                work_task.cancel()
                await asyncio.gather(work_task, return_exceptions=True)
                raise error or TradingPlaybookClaimLost(
                    f"playbook claim heartbeat stopped: {token.job_key}"
                )
            result = await work_task
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            async with self._playbook_sessions() as fence_db:
                fenced = await self._playbook_job_claims.renew(
                    fence_db,
                    token,
                    now=self._claim_now(),
                )
            if not fenced:
                raise TradingPlaybookClaimLost(
                    f"playbook claim lost before completion: {token.job_key}"
                )
            return result
        finally:
            for task in (work_task, heartbeat_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                work_task,
                heartbeat_task,
                return_exceptions=True,
            )

    @staticmethod
    def _plan_value(plan: Any, key: str) -> Any:
        if isinstance(plan, dict):
            return plan.get(key)
        return getattr(plan, key, None)

    @staticmethod
    def _coerce_date(value: Any, field_name: str) -> date:
        if isinstance(value, datetime):
            return value.date()
        if type(value) is date:
            return value
        if isinstance(value, str):
            try:
                return date.fromisoformat(value.strip())
            except ValueError as exc:
                raise ValueError(f"invalid {field_name}: {value!r}") from exc
        raise ValueError(f"invalid {field_name}: {value!r}")

    @staticmethod
    async def _latest_stage_plan_in_session(db, target_trade_date, stage):
        from sqlalchemy import select
        from app.models.trading_playbook import TradingPlanVersion

        return (
            await db.execute(
                select(TradingPlanVersion)
                .where(
                    TradingPlanVersion.target_trade_date == target_trade_date,
                    TradingPlanVersion.stage == stage,
                )
                .order_by(
                    TradingPlanVersion.version_no.desc(),
                    TradingPlanVersion.id.desc(),
                )
                .limit(1)
            )
        ).scalar_one_or_none()

    async def _notify_trading_playbook_plan(self, plan: Any):
        service = self._trading_playbook_alert_service
        notify = getattr(service, "notify_plan_ready", None)
        if not callable(notify):
            return None
        plan_id = self._plan_value(plan, "id")
        if not isinstance(plan_id, int):
            logger.error("Skipping playbook notification without plan id")
            return None
        raw_source_date = self._plan_value(plan, "source_trade_date")
        raw_target_date = self._plan_value(plan, "target_trade_date")
        date_error = None
        try:
            source_trade_date = (
                self._coerce_date(raw_source_date, "source_trade_date")
                if raw_source_date is not None
                else None
            )
            target_trade_date = (
                self._coerce_date(raw_target_date, "target_trade_date")
                if raw_target_date is not None
                else None
            )
        except ValueError as exc:
            source_trade_date = None
            target_trade_date = None
            date_error = exc
        async with self._playbook_sessions() as db:
            claim_kwargs = {
                "job_key": f"playbook:notify:plan:{plan_id}",
                "job_type": "plan",
                "phase": "notify",
                "owner": self._playbook_claim_owner,
                "now": self._claim_now(),
                "source_trade_date": source_trade_date,
                "target_trade_date": target_trade_date,
                "stage": self._plan_value(plan, "stage"),
                "generation_key": str(plan_id),
            }
            try:
                token = await self._playbook_job_claims.claim(db, **claim_kwargs)
            except Exception as claim_error:
                await db.rollback()
                logger.error(
                    "Trading playbook notification claim failed: {}",
                    claim_error,
                )
                # A second metadata-free attempt preserves a retry marker when
                # optional payload fields caused the original claim failure.
                claim_kwargs["source_trade_date"] = None
                claim_kwargs["target_trade_date"] = None
                try:
                    token = await self._playbook_job_claims.claim(
                        db,
                        **claim_kwargs,
                    )
                except Exception:
                    return None
                if token is not None:
                    await self._fail_playbook_claim_fresh(
                        token,
                        claim_error,
                    )
                return None
            if token is None:
                return None
            if date_error is not None:
                await self._fail_playbook_claim_fresh(token, date_error)
                logger.error("Trading playbook notification payload invalid: {}", date_error)
                return None
            try:
                result = await self._run_with_playbook_claim(
                    token,
                    lambda: notify(db, plan, send=True),
                )
            except asyncio.CancelledError as exc:
                await self._rollback_playbook_session(db)
                await self._fail_playbook_claim_fresh(token, exc)
                raise
            except Exception as exc:
                await self._rollback_playbook_session(db)
                await self._fail_playbook_claim_fresh(token, exc)
                logger.error("Trading playbook notification failed: {}", exc)
                return None
            completed = await self._playbook_job_claims.complete(
                db,
                token,
                now=self._claim_now(),
            )
            return result if completed else None

    async def _build_trading_playbook_preclose(self):
        return await self._build_trading_playbook_plan("preclose")

    async def _build_trading_playbook_overnight(self):
        return await self._build_trading_playbook_plan("overnight")

    async def _build_trading_playbook_auction(self):
        return await self._build_trading_playbook_plan("auction")

    async def _review_trading_playbook(self):
        now = self._playbook_now()
        if not await self._ensure_playbook_calendar(now.date()):
            logger.info("Skipping trading playbook review on non-trading day")
            return None
        return await self._run_trading_playbook_review_phase(
            now.date(),
            finalized=False,
        )

    async def _finalize_trading_playbook_review(
        self,
        trade_date: Optional[date] = None,
        plan_version_id: Optional[int] = None,
    ):
        now = self._playbook_now()
        review_date = trade_date or now.date()
        if not await self._ensure_playbook_calendar(review_date):
            logger.info("Skipping trading playbook final review on non-trading day")
            return None
        service = self._trading_playbook_review_service
        if not callable(getattr(service, "build", None)):
            logger.info("Trading playbook review service is not installed")
            return None
        return await self._run_trading_playbook_review_phase(
            review_date,
            finalized=True,
            plan_version_id=plan_version_id,
        )

    async def _run_trading_playbook_review_phase(
        self,
        review_date: date,
        *,
        finalized: bool,
        plan_version_id: Optional[int] = None,
    ):
        service = self._trading_playbook_review_service
        build = getattr(service, "build", None)
        if not callable(build):
            logger.info("Trading playbook review service is not installed")
            return None
        phase = "finalize" if finalized else "initial_review"
        async with self._playbook_sessions() as db:
            generation = getattr(service, "generation_key", None)
            if callable(generation):
                generation_key = await generation(
                    db,
                    review_date,
                    plan_version_id=plan_version_id,
                )
                if (
                    not isinstance(generation_key, str)
                    or not generation_key
                    or len(generation_key) > 120
                ):
                    raise ValueError(
                        "review generation key must contain 1 to 120 characters"
                    )
            else:
                selection = (
                    plan_version_id
                    if plan_version_id is not None
                    else "all"
                )
                generation_key = f"legacy:{review_date.isoformat()}:{selection}"
            token = await self._playbook_job_claims.claim(
                db,
                job_key=f"playbook:{phase}:{review_date}:{generation_key}",
                job_type="review",
                phase=phase,
                owner=self._playbook_claim_owner,
                now=self._claim_now(),
                source_trade_date=review_date,
                generation_key=generation_key,
            )
            if token is None:
                return None
            try:
                build_kwargs = {"finalized": finalized}
                if plan_version_id is not None:
                    build_kwargs["plan_version_id"] = plan_version_id
                result = await self._run_with_playbook_claim(
                    token,
                    lambda: build(db, review_date, **build_kwargs),
                )
            except asyncio.CancelledError as exc:
                await self._rollback_playbook_session(db)
                await self._fail_playbook_claim_fresh(token, exc)
                raise
            except Exception as exc:
                await self._rollback_playbook_session(db)
                await self._fail_playbook_claim_fresh(token, exc)
                logger.error("Trading playbook {} failed: {}", phase, exc)
                return None
            completed = await self._playbook_job_claims.complete(
                db,
                token,
                now=self._claim_now(),
            )
            return result if completed else None

    async def _monitor_trading_playbook(self):
        now = self._playbook_now()
        monitor_result = None
        service = self._trading_playbook_alert_service
        monitor = getattr(service, "monitor", None)
        if callable(monitor):
            try:
                async with self._playbook_sessions() as db:
                    monitor_result = await monitor(db, now)
            except Exception as exc:
                logger.error("Trading playbook alert monitor failed: {}", exc)
        else:
            logger.debug("Trading playbook alert monitor is not installed")
        next_trade_date = None
        notification_earliest_date = now.date()
        notification_latest_date = None
        try:
            if not await self._ensure_playbook_calendar(now.date()):
                return monitor_result
            next_trade_date = self._next_cn_trading_date(now.date())
            notification_earliest_date = now.date()
            notification_latest_date = next_trade_date
        except Exception as exc:
            logger.error("Trading playbook calendar refresh failed: {}", exc)
        try:
            await self._upgrade_forced_trading_playbook_after_close(
                send_notifications=True,
                trade_date=now.date(),
                next_trade_date=next_trade_date,
            )
        except Exception as exc:
            logger.error(
                "Trading playbook forced after-close upgrade failed: {}",
                exc,
            )
        try:
            await self._retry_incomplete_playbook_notifications(
                notification_earliest_date,
                notification_latest_date,
            )
        except Exception as exc:
            logger.error(
                "Trading playbook notification compensation failed: {}",
                exc,
            )
        try:
            await self._compensate_trading_playbook_phases(
                now.date(),
                next_trade_date,
                send_notifications=True,
            )
        except Exception as exc:
            logger.error("Trading playbook phase compensation failed: {}", exc)
        return monitor_result

    async def _retry_incomplete_playbook_notifications(
        self,
        earliest_target_date: Optional[date],
        latest_target_date: Optional[date],
    ) -> None:
        service = self._trading_playbook_alert_service
        if not callable(getattr(service, "notify_plan_ready", None)):
            return
        from sqlalchemy import or_, select, update
        from app.models.trading_playbook import (
            TradingPlanVersion,
            TradingPlaybookJobClaim,
        )

        now = self._claim_now()
        effective_earliest_target_date = (
            earliest_target_date or self._playbook_now().date()
        )
        async with self._playbook_sessions() as db:
            claims = list(
                (
                    await db.execute(
                        select(TradingPlaybookJobClaim)
                        .where(
                            TradingPlaybookJobClaim.job_type == "plan",
                            TradingPlaybookJobClaim.phase == "notify",
                            TradingPlaybookJobClaim.status != "completed",
                            or_(
                                TradingPlaybookJobClaim.status == "retry",
                                TradingPlaybookJobClaim.lease_expires_at.is_(None),
                                TradingPlaybookJobClaim.lease_expires_at <= now,
                            ),
                        )
                        .order_by(
                            TradingPlaybookJobClaim.updated_at.asc(),
                            TradingPlaybookJobClaim.id.asc(),
                        )
                        .limit(_PLAYBOOK_NOTIFICATION_RETRY_BATCH_SIZE)
                    )
                )
                .scalars()
                .all()
            )
            if not claims:
                return
            plan_id_by_claim_id = {}
            plan_ids = set()
            retirement_reason_by_claim_id = {}
            for claim in claims:
                try:
                    plan_id = int(claim.generation_key)
                except (TypeError, ValueError):
                    retirement_reason_by_claim_id[claim.id] = (
                        "invalid generation key"
                    )
                    continue
                if plan_id < 1:
                    retirement_reason_by_claim_id[claim.id] = (
                        "invalid generation key"
                    )
                    continue
                plan_id_by_claim_id[claim.id] = plan_id
                plan_ids.add(plan_id)
            plans = []
            if plan_ids:
                plans = list(
                    (
                        await db.execute(
                            select(TradingPlanVersion).where(
                                TradingPlanVersion.id.in_(plan_ids)
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            plan_by_id = {plan.id: plan for plan in plans}
            eligible_plans = []
            deferred_claim_ids = set()
            for claim in claims:
                if claim.id in retirement_reason_by_claim_id:
                    continue
                plan = plan_by_id.get(plan_id_by_claim_id[claim.id])
                if plan is None:
                    retirement_reason_by_claim_id[claim.id] = "plan missing"
                    continue
                if plan.status not in ("draft", "confirmed", "active"):
                    retirement_reason_by_claim_id[claim.id] = (
                        f"plan status {plan.status!r} is not notifiable"
                    )
                    continue
                if plan.target_trade_date < effective_earliest_target_date:
                    retirement_reason_by_claim_id[claim.id] = (
                        "stale target date "
                        f"{plan.target_trade_date.isoformat()} before "
                        f"{effective_earliest_target_date.isoformat()}"
                    )
                    continue
                if (
                    latest_target_date is not None
                    and plan.target_trade_date > latest_target_date
                ):
                    deferred_claim_ids.add(claim.id)
                    continue
                eligible_plans.append(plan)

            for claim in claims:
                reason = retirement_reason_by_claim_id.get(claim.id)
                values = None
                if reason is not None:
                    values = {
                        "status": "completed",
                        "completed_at": now,
                        "lease_expires_at": None,
                        "last_error": (
                            f"notification retry retired: {reason}"
                        )[:2000],
                        "updated_at": now,
                    }
                elif claim.id in deferred_claim_ids:
                    values = {
                        "status": "retry",
                        "lease_expires_at": now,
                        "last_error": (
                            "notification retry deferred: target date "
                            "outside current window"
                        ),
                        "updated_at": now,
                    }
                if values is None:
                    continue
                await db.execute(
                    update(TradingPlaybookJobClaim)
                    .where(
                        TradingPlaybookJobClaim.id == claim.id,
                        TradingPlaybookJobClaim.status == claim.status,
                        TradingPlaybookJobClaim.owner == claim.owner,
                        TradingPlaybookJobClaim.attempt_no == claim.attempt_no,
                        TradingPlaybookJobClaim.updated_at == claim.updated_at,
                        TradingPlaybookJobClaim.status != "completed",
                    )
                    .values(**values)
                )
            await db.commit()
        for plan in eligible_plans:
            await self._notify_trading_playbook_plan(plan)

    async def _compensate_trading_playbook_phases(
        self,
        trade_date: date,
        next_trade_date: date,
        *,
        send_notifications: bool,
    ) -> None:
        if self._playbook_now().time().replace(tzinfo=None) < time(15, 30):
            return
        plan = await self._latest_relevant_after_close_plan(
            trade_date,
            next_trade_date,
        )
        if plan is None:
            return
        if send_notifications:
            await self._notify_trading_playbook_plan(plan)
        await self._finalize_trading_playbook_review(
            trade_date,
        )

    async def _latest_relevant_after_close_plan(
        self,
        trade_date: date,
        next_trade_date: Optional[date] = None,
    ):
        from sqlalchemy import select
        from app.models.trading_playbook import TradingPlanVersion

        if next_trade_date is None:
            if not await self._ensure_playbook_calendar(trade_date):
                return None
            next_trade_date = self._next_cn_trading_date(trade_date)
        target_trade_date = next_trade_date
        async with self._playbook_sessions() as db:
            result = await db.execute(
                select(TradingPlanVersion)
                .where(
                    TradingPlanVersion.stage == "after_close",
                    TradingPlanVersion.source_trade_date == trade_date,
                    TradingPlanVersion.target_trade_date == target_trade_date,
                )
                .order_by(
                    TradingPlanVersion.version_no.desc(),
                    TradingPlanVersion.id.desc(),
                )
                .limit(1)
            )
            return result.scalar_one_or_none()

    @staticmethod
    def _is_forced_degraded_plan(plan: Any) -> bool:
        quality = getattr(plan, "data_quality_json", None)
        if not isinstance(quality, dict):
            return False
        if "forced_degraded" in quality:
            return quality.get("forced_degraded") is True
        warnings = quality.get("warnings")
        if not isinstance(warnings, (list, tuple)):
            return False
        return "force_degraded requested" in warnings

    async def _upgrade_forced_trading_playbook_after_close(
        self,
        *,
        send_notifications: bool,
        trade_date: Optional[date] = None,
        next_trade_date: Optional[date] = None,
    ):
        now = self._playbook_now()
        current_time = now.time().replace(tzinfo=None)
        if current_time < time(15, 30):
            return None
        source_trade_date = trade_date or now.date()
        target_trade_date = next_trade_date
        if target_trade_date is None:
            if not await self._ensure_playbook_calendar(source_trade_date):
                return None
            target_trade_date = self._next_cn_trading_date(source_trade_date)
        orchestrator = self._trading_playbook_orchestrator
        if orchestrator is None:
            return None

        async with self._playbook_upgrade_lock:
            predecessor = await self._latest_relevant_after_close_plan(
                source_trade_date,
                target_trade_date,
            )
            if not self._is_forced_degraded_plan(predecessor):
                return None
            if not await self._trading_playbook_data_ready_once(
                source_trade_date
            ):
                return None
            plan = await self._build_trading_playbook_plan(
                "after_close",
                degraded=False,
                send_notifications=send_notifications,
            )
            if plan is None:
                return None
            await self._finalize_trading_playbook_review(
                source_trade_date,
            )
            return plan

    async def _wait_for_trading_playbook_data(
        self,
        trade_date: date,
        *,
        timeout_seconds: float = 180,
        poll_seconds: float = 10,
    ) -> bool:
        clock = self._playbook_monotonic
        if clock is None:
            clock = asyncio.get_running_loop().time
        deadline = clock() + max(float(timeout_seconds), 0.0)
        while True:
            remaining = deadline - clock()
            if remaining <= 0:
                break
            try:
                ready = await asyncio.wait_for(
                    self._trading_playbook_data_ready_once(trade_date),
                    timeout=min(10.0, remaining),
                )
                if ready:
                    return True
            except asyncio.TimeoutError:
                logger.warning(
                    "Trading playbook data-ready poll timed out for {}",
                    trade_date,
                )
            except Exception as exc:
                logger.warning(
                    "Trading playbook data-ready poll failed for {}: {}",
                    trade_date,
                    exc,
                )
            remaining = deadline - clock()
            wait_seconds = min(float(poll_seconds), remaining, 10.0)
            if wait_seconds <= 0:
                break
            await self._playbook_sleep(wait_seconds)
        return False

    async def _trading_playbook_data_ready_once(
        self,
        trade_date: date,
    ) -> bool:
        from sqlalchemy import select
        from app.models.market_review import MarketReviewDailyMetric

        cutoff = datetime.combine(trade_date, time(15, 0), tzinfo=CN_TZ)
        async with self._playbook_sessions() as db:
            result = await db.execute(
                select(MarketReviewDailyMetric)
                .where(MarketReviewDailyMetric.trade_date == trade_date)
                .limit(1)
            )
            metric = result.scalar_one_or_none()
        updated_at = getattr(metric, "updated_at", None)
        if isinstance(updated_at, datetime):
            if updated_at.tzinfo is None or updated_at.utcoffset() is None:
                updated_at = updated_at.replace(tzinfo=CN_TZ)
            else:
                updated_at = updated_at.astimezone(CN_TZ)
            if updated_at > cutoff:
                return True
        return False

    async def _build_trading_playbook_after_close(
        self,
        *,
        send_notifications: bool = True,
    ):
        now = self._playbook_now()
        if not await self._ensure_playbook_calendar(now.date()):
            logger.info("Skipping trading playbook after-close on non-trading day")
            return None
        ready = await self._wait_for_trading_playbook_data(now.date())
        plan = await self._build_trading_playbook_plan(
            "after_close",
            degraded=not ready,
            degradation_reason=(
                None if ready else "after_close_barrier_timeout"
            ),
            send_notifications=send_notifications,
        )
        if plan is not None:
            await self._finalize_trading_playbook_review()
        return plan

    async def _playbook_stage_exists(
        self,
        target_trade_date: date,
        stage: str,
    ) -> bool:
        from sqlalchemy import select
        from app.models.trading_playbook import TradingPlanVersion

        async with self._playbook_sessions() as db:
            result = await db.execute(
                select(TradingPlanVersion.id)
                .where(
                    TradingPlanVersion.target_trade_date == target_trade_date,
                    TradingPlanVersion.stage == stage,
                )
                .limit(1)
            )
            return result.scalar_one_or_none() is not None

    async def _playbook_review_exists(
        self,
        trade_date: date,
        *,
        finalized: bool = False,
        plan_version_id: Optional[int] = None,
    ) -> bool:
        from sqlalchemy import select
        from app.models.trading_playbook import TradingExecutionReview

        async with self._playbook_sessions() as db:
            query = select(TradingExecutionReview.id).where(
                TradingExecutionReview.trade_date == trade_date
            )
            if finalized:
                query = query.where(
                    TradingExecutionReview.finalized_at.is_not(None)
                )
            if isinstance(plan_version_id, int) and not finalized:
                query = query.where(
                    TradingExecutionReview.plan_version_id == plan_version_id
                )
            result = await db.execute(query.limit(1))
            return result.scalar_one_or_none() is not None

    async def _run_trading_playbook_catchup(
        self,
        now: Optional[datetime] = None,
    ) -> None:
        current = now or self._playbook_now()
        if current.tzinfo is None or current.utcoffset() is None:
            raise ValueError("trading playbook catch-up time must be timezone-aware")
        current = current.astimezone(CN_TZ)
        today = current.date()
        if not await self._ensure_playbook_calendar(today):
            logger.info("Skipping trading playbook catch-up on non-trading day")
            return
        next_trade_date = self._next_cn_trading_date(today)
        current_time = current.time().replace(tzinfo=None)

        if time(8, 50) <= current_time < time(9, 26):
            if not await self._playbook_stage_exists(today, "overnight"):
                await self._build_trading_playbook_plan(
                    "overnight",
                    send_notifications=False,
                )
        elif time(9, 26) <= current_time < time(15, 0):
            if not await self._playbook_stage_exists(today, "auction"):
                await self._build_trading_playbook_plan(
                    "auction",
                    degraded=True,
                    send_notifications=False,
                )

        if time(14, 40) <= current_time < time(15, 0):
            if not await self._playbook_stage_exists(
                next_trade_date,
                "preclose",
            ):
                await self._build_trading_playbook_plan(
                    "preclose",
                    send_notifications=False,
                )

        if current_time >= time(15, 10):
            # The phase claim is the completeness gate.  One persisted row is
            # not proof that every applicable plan review was written while
            # the preliminary-review window is still open.  From 15:30 the
            # final reconciliation below covers every applicable plan.
            if (
                current_time < time(15, 30)
                or not await self._playbook_review_exists(today)
            ):
                await self._review_trading_playbook()
        if current_time >= time(15, 30):
            if not await self._playbook_stage_exists(
                next_trade_date,
                "after_close",
            ):
                await self._build_trading_playbook_after_close(
                    send_notifications=False,
                )
        try:
            await self._upgrade_forced_trading_playbook_after_close(
                send_notifications=False,
                trade_date=today,
                next_trade_date=next_trade_date,
            )
        except Exception as exc:
            logger.error(
                "Trading playbook startup forced upgrade failed: {}",
                exc,
            )
        try:
            await self._compensate_trading_playbook_phases(
                today,
                next_trade_date,
                send_notifications=False,
            )
        except Exception as exc:
            logger.error(
                "Trading playbook startup phase compensation failed: {}",
                exc,
            )
        try:
            await self._retry_incomplete_playbook_notifications(
                today,
                next_trade_date,
            )
        except Exception as exc:
            logger.error(
                "Trading playbook startup notification compensation failed: {}",
                exc,
            )
    
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

    async def _calculate_daily_analysis(self):
        """生成每日分析月表数据"""
        try:
            from app.database import async_session_maker
            from app.services.daily_analysis_service import daily_analysis_service

            today = today_cn()
            resolved_trade_date = _resolve_cn_trade_date_for_market_review(today)
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

        trade_date = _resolve_cn_trade_date_for_market_review(today_cn())
        if trade_date is None:
            logger.info("Skipping after-close catchup because current China date is not a trading day")
            return

        logger.info(f"Running after-close catchup for {trade_date}")
        if settings.MARKET_REVIEW_ENABLED:
            try:
                await self._build_market_review()
            except Exception as e:
                logger.error(f"After-close market review catchup failed: {e}")

        await self._calculate_daily_analysis()
        await self._archive_limit_up_classification()

    async def _archive_limit_up_classification(self):
        """归档收盘后的涨停分类快照。"""
        try:
            from app.services.ths_limit_up_classification_service import ths_limit_up_classification_service

            today = today_cn()
            resolved_trade_date = _resolve_cn_trade_date_for_market_review(today)
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

    async def _build_market_review(self):
        """构建当日市场复盘数据"""
        try:
            trade_date = _resolve_cn_trade_date_for_market_review()
            if trade_date is None:
                logger.info("Skipping market review build because current China date is not a trading day")
                return
            await market_review_pipeline_service.run_for_date(trade_date, calc_version=1)
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
