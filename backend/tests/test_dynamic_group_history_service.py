"""Real SQLite tests: fixed baskets must keep their denominator and provenance."""
import asyncio
import gc
import importlib.util
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

if importlib.util.find_spec("app.services.dynamic_group_history_service"):
    from app.services.dynamic_group_history_service import DynamicGroupHistoryService
    from app.models.dynamic_group_history import (
        DynamicGroupHistoryJob, DynamicGroupHistoryRevision,
        DynamicGroupStockHistoryRevision, DynamicGroupStockHistoryCache,
    )
else:
    DynamicGroupHistoryService = None


class DynamicGroupHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertIsNotNone(DynamicGroupHistoryService, "historical reconstruction service is not implemented")
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = create_async_engine("sqlite+aiosqlite:///" + str(Path(self.tmp.name) / "history.db"),
                                          poolclass=AsyncAdaptedQueuePool)
        self._session_close_tasks = set()

        class TrackedSession(AsyncSession):
            async def __aexit__(session, *exc):
                task = asyncio.create_task(session.close())
                self._session_close_tasks.add(task)
                try:
                    await asyncio.shield(task)
                finally:
                    if task.done():
                        self._session_close_tasks.discard(task)

        self.session_class = TrackedSession
        self.sessions = async_sessionmaker(self.engine, class_=TrackedSession, expire_on_commit=False)
        from app.database import Base
        async with self.engine.begin() as conn:
            from app.models.dynamic_group import (DynamicGroup, DynamicGroupSelection,
                DynamicGroupSelectionItem, DynamicGroupEvent)
            await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=[
                DynamicGroupStockHistoryRevision.__table__, DynamicGroupStockHistoryCache.__table__,
                DynamicGroupHistoryJob.__table__, DynamicGroupHistoryRevision.__table__,
                DynamicGroup.__table__, DynamicGroupSelection.__table__,
                DynamicGroupSelectionItem.__table__, DynamicGroupEvent.__table__,
            ]))
        self.services = []
        self.now = datetime(2026, 10, 30, 13, 0)
        self.payloads = {
            "000001": {"2026-10-26": 10, "2026-10-27": 11, "2026-10-28": 12, "2026-10-29": 13},
            "600000": {"2026-10-26": 20, "2026-10-27": 24, "2026-10-28": 22, "2026-10-29": 26},
        }

    async def asyncTearDown(self):
        for service in getattr(self, "services", []):
            await service.close()
        while getattr(self, "_session_close_tasks", None):
            closing = tuple(self._session_close_tasks)
            await asyncio.gather(*closing)
            self._session_close_tasks.difference_update(closing)
        if hasattr(self, "engine"):
            await self.engine.dispose()
            # SQLite exception tracebacks can retain failed cursor cycles after
            # a forced unique-key race; release those before Windows unlinks DB.
            gc.collect()
            self.tmp.cleanup()

    async def fetch(self, code, start, end):
        return {"source": "tencent_fqkline", "adjustment": "qfq", "prices": dict(self.payloads[code])}

    def service(self, **kwargs):
        service = DynamicGroupHistoryService(session_factory=self.sessions,
            history_fetcher=kwargs.pop("history_fetcher", self.fetch), now_provider=lambda: self.now,
            calendar_loader=lambda start, end: [date(2026, 10, day) for day in range(26, 31)
                                                if start <= date(2026, 10, day) <= end],
            retry_delay=0, refresh_seconds=1, **kwargs)
        self.services.append(service)
        return service

    def items(self, *codes):
        return [{"stock_code": code} for code in (codes or ("000001", "600000"))]

    async def test_history_precedes_selection_and_display_crop_never_rebases(self):
        service = self.service()
        await service.ensure_series(1, self.items())
        result = await service.get_series(1, window_days=0)
        self.assertEqual(result["base_date"], "2026-10-26")
        self.assertEqual([p["value"] for p in result["points"]], [1000, 1150, 1150, 1300])
        self.assertEqual(result["expected_count"], 2)
        cropped = await service.get_series(1, window_days=2)
        self.assertEqual([p["value"] for p in cropped["points"]], [1150, 1300])
        self.assertEqual(cropped["base_date"], "2026-10-26")
        # A fresh instance with an unusable source proves reads only use durable data.
        async def forbidden(*args):
            raise AssertionError("history reads must not call a provider")
        reloaded = await self.service(history_fetcher=forbidden).get_series(1)
        self.assertEqual(reloaded["points"], result["points"])
        self.assertEqual(len(reloaded["provenance"]), 2)

    async def test_missing_constituent_is_gap_with_fixed_denominator(self):
        del self.payloads["600000"]["2026-10-27"]
        service = self.service()
        await service.ensure_series(2, self.items())
        result = await service.get_series(2)
        self.assertEqual(result["status"], "partial")
        self.assertIsNone(result["points"][1]["value"])
        self.assertEqual(result["points"][1]["covered_count"], 1)
        self.assertEqual(result["expected_count"], 2)
        self.assertEqual(result["points"][2]["value"], 1150)
        self.assertIsNone(result["points"][2]["change_pct"])

    async def test_new_listing_uses_first_common_valid_date(self):
        del self.payloads["600000"]["2026-10-26"]
        service = self.service()
        await service.ensure_series(3, self.items())
        result = await service.get_series(3)
        self.assertEqual(result["base_date"], "2026-10-27")
        self.assertEqual(result["points"][0]["value"], 1000)
        self.assertTrue(result["warnings"])

    async def test_revision_refresh_rebuilds_whole_adjusted_series_and_preserves_old_revision(self):
        service = self.service()
        await service.ensure_series(4, self.items())
        before = await service.get_series(4)
        # Uniform qfq adjustment halves prices including the historical baseline.
        self.payloads["000001"] = {day: price / 2 for day, price in self.payloads["000001"].items()}
        self.payloads["000001"]["2026-10-30"] = 7
        self.payloads["600000"]["2026-10-30"] = 28
        self.now = datetime(2026, 10, 30, 16, 0)
        await service.ensure_series(4, self.items(), refresh=True)
        after = await service.get_series(4)
        self.assertNotEqual(after["revision_id"], before["revision_id"])
        self.assertEqual(after["points"][-1]["value"], 1400)
        async with self.sessions() as session:
            old = await session.get(DynamicGroupHistoryRevision, before["revision_id"])
            self.assertEqual(old.points, before["points"])
        await service.ensure_series(4, self.items(), refresh=True)
        self.assertEqual((await service.get_series(4))["revision_id"], after["revision_id"])

    async def test_retry_success_and_failed_refresh_preserves_successful_data(self):
        attempts = 0
        broken = False
        async def source(code, start, end):
            nonlocal attempts
            attempts += 1
            if attempts == 1 or broken:
                raise TimeoutError("upstream unavailable")
            return await self.fetch(code, start, end)
        service = self.service(history_fetcher=source)
        await service.ensure_series(5, self.items("000001"))
        first = await service.get_series(5)
        self.assertEqual(first["status"], "ready")
        broken = True
        await service.ensure_series(5, self.items("000001"), refresh=True)
        failed = await service.get_series(5)
        self.assertEqual(failed["status"], "stale")
        self.assertEqual(failed["points"], first["points"])
        self.assertTrue(failed["warnings"])
        broken = False
        await service.ensure_series(5, self.items("000001"), refresh=True)
        self.assertEqual((await service.get_series(5))["status"], "ready")

    async def test_cache_is_shared_and_provider_work_is_globally_bounded(self):
        running = maximum = calls = 0
        async def source(code, start, end):
            nonlocal running, maximum, calls
            running += 1
            calls += 1
            maximum = max(maximum, running)
            await asyncio.sleep(0.01)
            running -= 1
            return await self.fetch(code, start, end)
        service = self.service(history_fetcher=source, concurrency=1)
        await asyncio.gather(*(service.ensure_series(i, self.items()) for i in range(10, 15)))
        self.assertEqual(maximum, 1)
        self.assertEqual(calls, 2)
        for i in range(10, 15):
            self.assertEqual((await service.get_series(i))["points"][-1]["value"], 1300)

    async def test_restart_recovers_pending_job_and_old_selections_continue_fairly(self):
        service = self.service()
        await service.ensure_series(21, self.items("000001"))
        await service.ensure_series(22, self.items("600000"))
        await service.close()
        self.now += timedelta(seconds=2)
        self.payloads["000001"]["2026-10-30"] = 15
        self.payloads["600000"]["2026-10-30"] = 30
        self.now = datetime(2026, 10, 30, 16, 0)
        recovered = self.service()
        await recovered.tick(limit=1)
        await recovered.wait_idle()
        await recovered.tick(limit=1)
        await recovered.wait_idle()
        self.assertEqual((await recovered.get_series(21))["points"][-1]["value"], 1500)
        self.assertEqual((await recovered.get_series(22))["points"][-1]["value"], 1500)

    async def test_request_build_is_background_and_invalid_basket_cannot_mutate_archive(self):
        service = self.service()
        service.request_build(30, self.items("000001"))
        await service.wait_idle()
        self.assertEqual((await service.get_series(30))["status"], "ready")
        with self.assertRaises(ValueError):
            await service.ensure_series(30, self.items("600000"))
        self.assertEqual((await service.get_series(30))["expected_count"], 1)

    async def test_raw_fallback_invalid_prices_and_future_dates_are_rejected(self):
        async def source(*args):
            return {"source": "tencent_fqkline", "adjustment": "raw", "prices": {"2026-10-26": 10}}
        service = self.service(history_fetcher=source)
        await service.ensure_series(40, self.items("000001"))
        result = await service.get_series(40)
        self.assertEqual(result["status"], "retrying")
        self.assertEqual(result["points"], [])
        async with self.sessions() as session:
            count = await session.scalar(select(func.count()).select_from(DynamicGroupStockHistoryRevision))
            self.assertEqual(count, 0)

    async def test_all_constituents_missing_a_real_session_produce_a_gap(self):
        del self.payloads["000001"]["2026-10-27"]
        del self.payloads["600000"]["2026-10-27"]
        service = self.service()
        await service.ensure_series(41, self.items())
        result = await service.get_series(41)
        self.assertEqual(result["points"][1], {
            "date": "2026-10-27", "value": None, "change_pct": None, "covered_count": 0})
        self.assertIsNone(result["points"][2]["change_pct"])

    async def test_independent_workers_cannot_claim_the_same_selection(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def slow(code, start, end):
            entered.set()
            await release.wait()
            return await self.fetch(code, start, end)
        service = self.service(history_fetcher=slow)
        first = asyncio.create_task(service.ensure_series(42, self.items("000001")))
        await entered.wait()
        second = self.service()
        concurrent = await second.ensure_series(42, self.items("000001"))
        self.assertEqual(concurrent["status"], "building")
        release.set()
        await first
        self.assertEqual((await second.get_series(42))["status"], "ready")

    async def test_refresh_cannot_silently_move_existing_baseline(self):
        service = self.service()
        await service.ensure_series(43, self.items())
        before = await service.get_series(43)
        del self.payloads["000001"]["2026-10-26"]
        await service.ensure_series(43, self.items(), refresh=True)
        after = await service.get_series(43)
        self.assertEqual(after["status"], "stale")
        self.assertEqual(after["revision_id"], before["revision_id"])
        self.assertEqual(after["base_date"], "2026-10-26")

    async def test_tick_discovers_committed_archived_selection_without_registered_job(self):
        from app.models.dynamic_group import DynamicGroup, DynamicGroupSelection, DynamicGroupSelectionItem
        async with self.sessions() as session:
            session.add(DynamicGroup(id=1, name="archived", archived=True))
            session.add(DynamicGroupSelection(id=51, group_id=1, version=1,
                trade_date=date(2026, 10, 30), selected_at=self.now, member_count=1,
                kind="changed", batch_key="archive", definition={}))
            session.add(DynamicGroupSelectionItem(selection_id=51, stock_code="000001",
                entry_event_id=1, payload={"stock_code": "000001"}))
            await session.commit()
        service = self.service()
        await service.tick()
        await service.wait_idle()
        self.assertEqual((await service.get_series(51))["status"], "ready")

    async def test_provider_uses_supported_count_and_discards_partial_current_session(self):
        self.now = datetime(2026, 10, 30, 13, 0)
        def response(request):
            count = int(request.url.params["param"].split(",")[4])
            # Tencent silently falls back to a shorter default above 800.
            rows = [["2023-10-31", "10", "10"], ["2026-10-29", "11", "11"],
                    ["2026-10-30", "99", "99"]]
            if count > 800:
                rows = rows[1:]
            return httpx.Response(200, json={"code": 0, "data": {"sz000001": {"qfqday": rows}}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            service = self.service(http_client=client)
            raw = await service._fetch_history("000001", date(2023, 10, 31), date(2026, 10, 30))
        self.assertIn("2023-10-31", raw["prices"])
        self.assertNotIn("2026-10-30", raw["prices"])

    async def test_provider_retains_old_baseline_beyond_800_rows_and_checks_overlap(self):
        days = []
        current = date(2023, 1, 2)
        while len(days) < 850:
            if current.weekday() < 5:
                days.append(current)
            current += timedelta(days=1)
        inconsistent = False
        def response(request):
            _, _, first, last, _, _ = request.url.params["param"].split(",")
            rows = [[day.isoformat(), "10", "10"] for day in days
                    if first <= day.isoformat() <= last][-800:]
            if inconsistent and last < days[-1].isoformat():
                for row in rows:
                    row[2] = "5"
            return httpx.Response(200, json={"code": 0, "data": {"sz000001": {"qfqday": rows}}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            service = self.service(http_client=client)
            raw = await service._fetch_history("000001", days[0], days[-1])
            self.assertEqual(len(raw["prices"]), 850)
            self.assertIn(days[0].isoformat(), raw["prices"])
            inconsistent = True
            with self.assertRaises(ValueError):
                await service._fetch_history("000001", days[0], days[-1])

    async def test_after_close_missing_entire_latest_session_is_an_explicit_tail_gap(self):
        self.now = datetime(2026, 10, 30, 16, 0)
        service = self.service()
        await service.ensure_series(70, self.items())
        result = await service.get_series(70)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["points"][-1], {
            "date": "2026-10-30", "value": None, "change_pct": None, "covered_count": 0})

    async def test_source_coverage_regression_preserves_old_successful_revision(self):
        service = self.service()
        await service.ensure_series(71, self.items())
        before = await service.get_series(71)
        for prices in self.payloads.values():
            del prices["2026-10-29"]
        await service.ensure_series(71, self.items(), refresh=True)
        after = await service.get_series(71)
        self.assertEqual(after["status"], "stale")
        self.assertEqual(after["revision_id"], before["revision_id"])
        self.assertEqual(after["points"], before["points"])

    async def test_two_service_cold_stock_writes_retry_unique_conflict_without_failed_index(self):
        source_ready = asyncio.Event()
        source_count = 0
        read_ready = asyncio.Event()
        read_count = 0
        async def synchronized_source(code, start, end):
            nonlocal source_count
            source_count += 1
            if source_count == 2:
                source_ready.set()
            await source_ready.wait()
            return await self.fetch(code, start, end)

        class SynchronizedSession(self.session_class):
            async def scalar(inner, statement, *args, **kwargs):
                nonlocal read_count
                result = await super().scalar(statement, *args, **kwargs)
                if (result is None and read_count < 2 and
                        "FROM dynamic_group_stock_history_revisions" in str(statement)):
                    read_count += 1
                    if read_count == 2:
                        read_ready.set()
                    await read_ready.wait()
                return result

        sessions = async_sessionmaker(self.engine, class_=SynchronizedSession, expire_on_commit=False)
        first = self.service(history_fetcher=synchronized_source)
        second = self.service(history_fetcher=synchronized_source)
        first.session_factory = second.session_factory = sessions
        results = await asyncio.gather(
            first.ensure_series(72, self.items("000001")),
            second.ensure_series(73, self.items("000001")))
        self.assertEqual([result["status"] for result in results], ["ready", "ready"])
        async with self.sessions() as session:
            count = await session.scalar(select(func.count()).select_from(DynamicGroupStockHistoryRevision))
            self.assertEqual(count, 1)
