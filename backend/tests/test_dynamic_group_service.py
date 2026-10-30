"""Database behavior of permanent dynamic-group selection archives."""
import asyncio
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.database import Base


class HistoryStub:
    def request_build(self, *args, **kwargs):
        pass

    async def get_series(self, selection_id, window_days=250):
        return {"selection_id": selection_id, "status": "pending", "points": []}


class DynamicGroupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from app.services.dynamic_group_service import DynamicGroupService
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = DynamicGroupService(
            session_factory=async_sessionmaker(self.engine, expire_on_commit=False),
            history=HistoryStub(), now=lambda: datetime(2026, 10, 30, 10, 0),
        )

    async def asyncTearDown(self):
        if hasattr(self, "service"):
            await self.service.close()
            await self.engine.dispose()

    def definition(self, **kwargs):
        return dict(name="强势", rules=[{"field": "change_pct", "op": "gte", "value": 5}], **kwargs)

    def snapshot(self, minute=0, change=6, **kwargs):
        return dict(trade_date="2026-10-30", previous_trade_date="2026-10-29",
                    source_time=f"2026-10-30T10:{minute:02d}:00", status="ready", warnings=[],
                    universe_codes=["600000"], rows=[dict(stock_code="600000", stock_name="浦发", price=10 + minute,
                    change_pct=change, quote_time=f"2026-10-30T10:{minute:02d}:00")], **kwargs)

    async def test_search_paging_pin_folder_and_soft_archive(self):
        first = await self.service.create_group(self.definition(folder="观察"))
        second = await self.service.create_group(self.definition(pinned=True))
        result = await self.service.list_groups(limit=1)
        self.assertEqual((result["total"], result["items"][0]["id"]), (2, second["id"]))
        self.assertEqual((await self.service.list_groups(folder="观察"))["total"], 1)
        self.assertEqual((await self.service.list_groups(q="找不到"))["total"], 0)
        await self.service.archive_group(first["id"])
        self.assertEqual((await self.service.list_groups(state="archived"))["total"], 1)
        self.assertTrue((await self.service.get_group(first["id"]))["archived"])

    async def test_unknown_preserves_member_exit_and_old_batch_is_frozen(self):
        group = await self.service.create_group(self.definition(membership_mode="dynamic"))
        await self.service.evaluate_snapshot(self.snapshot())
        current = await self.service.get_current(group["id"])
        original_id = current["selection_id"]
        await self.service.evaluate_snapshot(self.snapshot(1, None))
        self.assertEqual(len((await self.service.get_current(group["id"]))["items"]), 1)
        await self.service.evaluate_snapshot(self.snapshot(2, 1))
        self.assertEqual((await self.service.get_current(group["id"]))["items"], [])
        old = await self.service.get_selection(group["id"], original_id)
        self.assertEqual(old["items"][0]["change_pct"], 6)
        self.assertEqual(old["items"][0]["price"], 10)
        self.assertEqual((await self.service.list_selections(group["id"]))["total"], 2)

    async def test_prices_do_not_duplicate_batch_version_and_archive_preserve_old_rules(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        first = (await self.service.list_selections(group["id"]))["items"][0]
        await self.service.evaluate_snapshot(self.snapshot(1))
        await self.service.evaluate_snapshot(self.snapshot(1))
        self.assertEqual((await self.service.list_selections(group["id"]))["total"], 1)
        edited = await self.service.update_group(group["id"], {"rules": [{"field": "change_pct", "op": "gte", "value": 8}]})
        self.assertEqual(edited["version"], 2)
        await self.service.evaluate_snapshot(self.snapshot(2))
        await self.service.archive_group(group["id"])
        old = await self.service.get_selection(group["id"], first["id"])
        self.assertEqual(old["rules"][0]["value"], 5)
        self.assertEqual(old["version"], 1)
        self.assertEqual((await self.service.list_selections(group["id"]))["total"], 2)

    async def test_foreign_selection_never_reaches_history(self):
        first = await self.service.create_group(self.definition())
        other = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        selection = (await self.service.list_selections(first["id"]))["items"][0]
        with self.assertRaises(HTTPException) as raised:
            await self.service.get_trend(other["id"], selection["id"])
        self.assertEqual(raised.exception.status_code, 404)

    async def test_failed_source_does_not_erase_and_restart_reads_database(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        failed = self.snapshot(1)
        failed.update(status="error", rows=[], universe_codes=[])
        await self.service.evaluate_snapshot(failed)
        current = await self.service.get_current(group["id"])
        self.assertEqual(len(current["items"]), 1)
        self.assertEqual(current["status"], "error")
        self.assertEqual((await self.service.get_dates(group["id"]))["dates"], ["2026-10-30"])

    async def test_concurrent_sampling_creates_one_immutable_entry(self):
        group = await self.service.create_group(self.definition())
        await asyncio.gather(*(self.service.evaluate_snapshot(self.snapshot()) for _ in range(3)))
        self.assertEqual((await self.service.list_selections(group["id"]))["total"], 1)

    async def test_close_creates_one_batch_per_actual_quote_day(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        close = self.snapshot()
        close["source_time"] = close["rows"][0]["quote_time"] = "2026-10-30T15:00:00"
        await self.service.evaluate_snapshot(close)
        await self.service.evaluate_snapshot(close)
        close["source_time"] = close["rows"][0]["quote_time"] = "2026-10-30T15:00:30"
        await self.service.evaluate_snapshot(close)
        batches = await self.service.list_selections(group["id"])
        self.assertEqual(batches["total"], 2)
        self.assertEqual(batches["items"][0]["kind"], "close")

    async def test_stale_quote_or_missing_universe_row_never_exits_dynamic_member(self):
        group = await self.service.create_group(self.definition(membership_mode="dynamic"))
        await self.service.evaluate_snapshot(self.snapshot())
        stale = self.snapshot(1, 1)
        stale["rows"][0]["quote_time"] = "2026-10-29T15:00:00"
        await self.service.evaluate_snapshot(stale)
        current = await self.service.get_current(group["id"])
        self.assertEqual(len(current["items"]), 1)
        self.assertEqual(current["status"], "partial")
        self.assertEqual(current["items"][0]["data_status"], "unknown")
        self.assertEqual(current["items"][0]["price"], 10)

    async def test_intraday_membership_change_does_not_inject_new_members_prior_gain(self):
        group = await self.service.create_group(self.definition(membership_mode="dynamic"))
        await self.service.evaluate_snapshot(self.snapshot())
        second = self.snapshot(1)
        second["rows"].append(dict(stock_code="000001", price=100, change_pct=30, quote_time=second["source_time"]))
        await self.service.evaluate_snapshot(second)
        third = self.snapshot(2)
        third["rows"][0]["price"] = 12.1
        third["rows"].append(dict(stock_code="000001", price=110, change_pct=40, quote_time=third["source_time"]))
        await self.service.evaluate_snapshot(third)
        points = (await self.service.get_intraday(group["id"]))["points"]
        self.assertEqual([round(p["value"], 2) for p in points], [1000, 1100, 1210])

    async def test_manual_exit_preserves_original_entry_and_allows_later_reentry(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        first = (await self.service.get_current(group["id"]))["selection_id"]
        await self.service.remove_member(group["id"], "600000")
        self.assertEqual((await self.service.get_current(group["id"]))["items"], [])
        await self.service.evaluate_snapshot(self.snapshot(1))
        self.assertEqual(len((await self.service.get_current(group["id"]))["items"]), 1)
        self.assertEqual((await self.service.get_selection(group["id"], first))["items"][0]["price"], 10)

    async def test_old_source_cannot_overwrite_new_current_members(self):
        group = await self.service.create_group(self.definition(membership_mode="dynamic"))
        await self.service.evaluate_snapshot(self.snapshot(2, 1))
        await self.service.evaluate_snapshot(self.snapshot())
        self.assertEqual((await self.service.get_current(group["id"]))["items"], [])

    async def test_same_day_but_stale_quote_is_unknown(self):
        group = await self.service.create_group(self.definition(membership_mode="dynamic"))
        await self.service.evaluate_snapshot(self.snapshot())
        stale = self.snapshot(10, 1)
        stale["rows"][0]["quote_time"] = "2026-10-30T10:00:00"
        await self.service.evaluate_snapshot(stale)
        self.assertEqual(len((await self.service.get_current(group["id"]))["items"]), 1)

    async def test_restart_restores_archive_without_contacting_source(self):
        from app.services.dynamic_group_service import DynamicGroupService
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        restarted = DynamicGroupService(session_factory=self.service.session_factory, history=HistoryStub())
        try:
            current = await restarted.get_current(group["id"])
            batch = await restarted.get_selection(group["id"], current["selection_id"])
            self.assertEqual(batch["items"][0]["trigger_fields"]["change_pct"], 6)
        finally:
            await restarted.close()

    async def test_intraday_is_daily_rebased_and_never_connects_raw_prices_across_days(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        next_day = self.snapshot()
        next_day.update(trade_date="2026-11-02", previous_trade_date="2026-10-30", source_time="2026-11-02T10:00:00")
        next_day["rows"][0].update(quote_time=next_day["source_time"], price=5)
        self.service.now = lambda: datetime(2026, 11, 2, 10)
        await self.service.evaluate_snapshot(next_day)
        points = await self.service.get_intraday(group["id"])
        self.assertEqual(points["points"][0]["value"], 1000)
        self.assertTrue(points["daily_rebased"])

    async def test_cold_preview_is_pending_and_does_not_block_on_source_fetch(self):
        class ColdSource:
            def peek_snapshot(self):
                return None
            def request_refresh(self):
                pass
            async def get_snapshot(self):
                await asyncio.Event().wait()
        self.service._source = ColdSource()
        result = await asyncio.wait_for(self.service.preview(self.definition()), 0.5)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["items"], [])

    async def test_tick_source_failure_records_error_and_keeps_successful_members(self):
        class FailedSource:
            async def get_snapshot(self):
                raise RuntimeError("provider unavailable")
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        self.service._source = FailedSource()
        await self.service.tick()
        current = await self.service.get_current(group["id"])
        self.assertEqual(len(current["items"]), 1)
        self.assertEqual(current["status"], "error")
        self.assertTrue(current["group"]["last_error"])

    async def test_missing_all_row_timestamps_does_not_create_successful_empty_archive(self):
        group = await self.service.create_group(self.definition())
        missing = self.snapshot()
        missing["rows"][0]["quote_time"] = None
        await self.service.evaluate_snapshot(missing)
        current = await self.service.get_current(group["id"])
        self.assertEqual(current["status"], "partial")
        self.assertIsNone(current["selection_id"])

    async def test_description_search_and_literal_wildcards(self):
        await self.service.create_group(self.definition(description="电网观察100%"))
        await self.service.create_group({"name": "other", "rules": self.definition()["rules"]})
        self.assertEqual((await self.service.list_groups(q="电网"))["total"], 1)
        self.assertEqual((await self.service.list_groups(q="%"))["total"], 1)

    async def test_separate_workers_claim_database_before_reading_and_cannot_rewind(self):
        from app.services.dynamic_group_service import DynamicGroupService
        # Windows virus/index scanners may briefly retain a closed SQLite file.
        with TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            engine = create_async_engine(f"sqlite+aiosqlite:///{(Path(directory) / 'workers.db').as_posix()}", poolclass=NullPool)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            factory = async_sessionmaker(engine, expire_on_commit=False)
            workers = [DynamicGroupService(session_factory=factory, history=HistoryStub()) for _ in range(3)]
            try:
                group = await workers[0].create_group(self.definition(membership_mode="dynamic"))
                outcomes = await asyncio.gather(workers[0].evaluate_snapshot(self.snapshot(2, 1)),
                                     workers[1].evaluate_snapshot(self.snapshot()),
                                     workers[2].evaluate_snapshot(self.snapshot(2, 1)), return_exceptions=True)
                self.assertEqual(outcomes, [None, None, None])
                current = await workers[0].get_current(group["id"])
                self.assertEqual(current["items"], [])
                self.assertEqual(current["source_time"], "2026-10-30T10:02:00")
                self.assertLessEqual((await workers[0].list_selections(group["id"]))["total"], 2)
            finally:
                for worker in workers:
                    await worker.close()
                await engine.dispose()

    async def test_intraday_gap_restarts_with_explicit_null_and_new_segment(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        await self.service.evaluate_snapshot(self.snapshot(10))
        later = self.snapshot(11)
        later["rows"][0]["price"] = 22
        await self.service.evaluate_snapshot(later)
        points = (await self.service.get_intraday(group["id"]))["points"]
        self.assertEqual([p["value"] for p in points], [1000, None, 1100])
        self.assertEqual([p["segment"] for p in points], [1, 1, 2])

    async def test_lunch_break_uses_real_endpoint_prices_without_a_false_gap(self):
        group = await self.service.create_group(self.definition())
        morning = self.snapshot()
        morning["source_time"] = morning["rows"][0]["quote_time"] = "2026-10-30T11:30:00"
        await self.service.evaluate_snapshot(morning)
        afternoon = self.snapshot(1)
        afternoon["source_time"] = afternoon["rows"][0]["quote_time"] = "2026-10-30T13:00:00"
        await self.service.evaluate_snapshot(afternoon)
        points = (await self.service.get_intraday(group["id"]))["points"]
        self.assertEqual([p["value"] for p in points], [1000, 1100])

    async def test_manual_exit_does_not_earn_removed_members_later_price_gain(self):
        group = await self.service.create_group(self.definition())
        initial = self.snapshot()
        initial["rows"].append(dict(stock_code="000001", price=100, change_pct=6, quote_time=initial["source_time"]))
        await self.service.evaluate_snapshot(initial)
        await self.service.remove_member(group["id"], "600000")
        second = self.snapshot(1, 1)
        second["rows"][0]["price"] = 200
        second["rows"].append(dict(stock_code="000001", price=100, change_pct=6, quote_time=second["source_time"]))
        await self.service.evaluate_snapshot(second)
        points = (await self.service.get_intraday(group["id"]))["points"]
        self.assertIsNone(points[-1]["value"])

    async def test_weekend_detection_uses_real_selection_time_and_never_backdates_intraday(self):
        self.service.now = lambda: datetime(2026, 11, 1, 12, 0)
        group = await self.service.create_group(self.definition())
        snapshot = self.snapshot()
        snapshot["source_time"] = snapshot["rows"][0]["quote_time"] = "2026-10-30T15:00:00"
        await self.service.evaluate_snapshot(snapshot)
        current = await self.service.get_current(group["id"])
        selection = await self.service.get_selection(group["id"], current["selection_id"])
        self.assertEqual(selection["selected_at"], "2026-11-01T12:00:00")
        self.assertEqual(selection["items"][0]["selected_at"], "2026-11-01T12:00:00")
        self.assertEqual(selection["items"][0]["quote_time"], "2026-10-30T15:00:00")
        self.assertEqual(selection["trade_date"], "2026-10-30")
        self.assertEqual((await self.service.get_intraday(group["id"]))["points"], [])

    async def test_preview_counts_missing_quotes_as_unknown_coverage(self):
        snapshot = self.snapshot()
        snapshot["universe_codes"] += ["000001", "000002"]
        class CachedSource:
            def peek_snapshot(self):
                return snapshot
            def request_refresh(self):
                pass
        self.service._source = CachedSource()
        result = await self.service.preview(self.definition())
        self.assertEqual((result["total"], result["covered_count"], result["unknown_count"], result["matched_count"]), (3, 1, 2, 1))

    async def test_explicit_zero_selection_is_not_replaced_by_latest(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        with self.assertRaises(HTTPException) as raised:
            await self.service.get_trend(group["id"], 0)
        self.assertEqual(raised.exception.status_code, 404)

    async def test_source_recovery_with_same_close_quote_clears_error_without_duplicate_batch(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        await self.service.evaluate_snapshot({"status": "error", "warnings": ["offline"]})
        await self.service.evaluate_snapshot(self.snapshot())
        current = await self.service.get_current(group["id"])
        self.assertEqual(current["status"], "ready")
        self.assertIsNone(current["group"]["last_error"])
        self.assertEqual((await self.service.list_selections(group["id"]))["total"], 1)

    async def test_retain_rule_edit_exits_old_version_and_reselects_new_basket(self):
        from app.models.dynamic_group import DynamicGroupEvent
        group = await self.service.create_group(self.definition())
        initial = self.snapshot()
        initial["rows"].append(dict(stock_code="000001", price=100, change_pct=1, quote_time=initial["source_time"]))
        await self.service.evaluate_snapshot(initial)
        original = (await self.service.get_current(group["id"]))["selection_id"]
        self.service.now = lambda: datetime(2026, 10, 30, 10, 1)
        edited = await self.service.update_group(group["id"], {
            "rules": [{"field": "stock_code", "op": "eq", "value": "000001"}]})
        pending = await self.service.get_current(group["id"])
        self.assertEqual(edited["version"], 2)
        self.assertEqual((pending["items"], pending["group"]["member_count"], pending["selection_id"], pending["status"]), ([], 0, None, "pending"))
        await self.service.evaluate_snapshot({"status": "error", "warnings": ["offline"]})
        self.assertEqual((await self.service.get_current(group["id"]))["items"], [])
        await self.service.evaluate_snapshot(initial)
        current = await self.service.get_current(group["id"])
        self.assertEqual([item["stock_code"] for item in current["items"]], ["000001"])
        self.assertEqual(current["items"][0]["trigger_version"], 2)
        self.assertEqual((await self.service.get_selection(group["id"], current["selection_id"]))["kind"], "version")
        old = await self.service.get_selection(group["id"], original)
        self.assertEqual([item["stock_code"] for item in old["items"]], ["600000"])
        self.assertEqual(old["version"], 1)
        self.assertEqual(old["rules"][0]["field"], "change_pct")
        self.assertEqual(old["items"][0]["trigger_version"], 1)
        async with self.service.session_factory() as session:
            exits = (await session.scalars(select(DynamicGroupEvent).where(
                DynamicGroupEvent.group_id == group["id"], DynamicGroupEvent.kind == "rule_change_exit"))).all()
            self.assertEqual([(e.stock_code, e.version, e.observed_at) for e in exits], [("600000", 1, datetime(2026, 10, 30, 10, 1))])

    async def test_all_semantic_settings_start_a_new_empty_version(self):
        for changes in ({"match_mode": "any"}, {"universe": "limit_up"}, {"membership_mode": "dynamic"}):
            with self.subTest(changes=changes):
                group = await self.service.create_group(self.definition())
                await self.service.evaluate_snapshot(self.snapshot())
                await self.service.update_group(group["id"], changes)
                current = await self.service.get_current(group["id"])
                self.assertEqual((current["group"]["version"], current["group"]["member_count"], current["items"], current["selection_id"]), (2, 0, [], None))

    async def test_directory_edits_and_pause_preserve_current_basket_and_version(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot())
        old = await self.service.get_current(group["id"])
        edited = await self.service.update_group(group["id"], {
            "name": "重新命名", "description": "备注", "folder": "观察", "pinned": True, "enabled": False})
        current = await self.service.get_current(group["id"])
        self.assertEqual((edited["version"], edited["member_count"]), (1, 1))
        self.assertEqual(current["selection_id"], old["selection_id"])
        self.assertEqual(current["items"], old["items"])

    async def test_same_close_quote_rechecks_recovered_context_and_keeps_previous_batch_frozen(self):
        group = await self.service.create_group({"name": "昨日涨停", "rules": [
            {"field": "yesterday_limit_up", "op": "eq", "value": True}]})
        snapshot = self.snapshot()
        snapshot["source_time"] = snapshot["rows"][0]["quote_time"] = "2026-10-30T15:00:00"
        snapshot["rows"][0]["yesterday_limit_up"] = None
        snapshot["status"] = "partial"
        await self.service.evaluate_snapshot(snapshot)
        original = (await self.service.get_current(group["id"]))["selection_id"]
        snapshot["rows"][0]["yesterday_limit_up"] = True
        snapshot["status"] = "ready"
        await self.service.evaluate_snapshot(snapshot)
        await self.service.evaluate_snapshot(snapshot)
        current = await self.service.get_current(group["id"])
        self.assertEqual([item["stock_code"] for item in current["items"]], ["600000"])
        self.assertEqual(current["status"], "ready")
        self.assertEqual((await self.service.list_selections(group["id"]))["total"], 2)
        original_batch = await self.service.get_selection(group["id"], original)
        self.assertEqual((original_batch["member_count"], original_batch["items"], original_batch["data_status"]), (0, [], "partial"))

    async def test_same_quote_partial_to_ready_updates_status_without_duplicate_basket(self):
        group = await self.service.create_group(self.definition())
        snapshot = self.snapshot()
        snapshot["status"] = "partial"
        snapshot["warnings"] = ["上下文正在恢复"]
        await self.service.evaluate_snapshot(snapshot)
        snapshot["status"] = "ready"
        snapshot["warnings"] = []
        await self.service.evaluate_snapshot(snapshot)
        current = await self.service.get_current(group["id"])
        self.assertEqual((current["status"], current["warnings"]), ("ready", []))
        self.assertEqual((await self.service.list_selections(group["id"]))["total"], 1)

    async def test_unsettled_membership_change_breaks_old_basket_before_missing_quote_recovers(self):
        group = await self.service.create_group(self.definition(membership_mode="dynamic"))
        initial = self.snapshot()
        initial["rows"].append(dict(stock_code="000001", price=100, change_pct=6, quote_time=initial["source_time"]))
        await self.service.evaluate_snapshot(initial)
        missing = self.snapshot(1)
        missing["rows"] = [dict(stock_code="000001", price=100, change_pct=1, quote_time=missing["source_time"])]
        await self.service.evaluate_snapshot(missing)
        recovered = self.snapshot(2)
        recovered["rows"][0]["price"] = 10
        recovered["rows"].append(dict(stock_code="000001", price=200, change_pct=1, quote_time=recovered["source_time"]))
        await self.service.evaluate_snapshot(recovered)
        after = self.snapshot(3)
        after["rows"][0]["price"] = 11
        after["rows"].append(dict(stock_code="000001", price=400, change_pct=1, quote_time=after["source_time"]))
        await self.service.evaluate_snapshot(after)
        points = (await self.service.get_intraday(group["id"]))["points"]
        self.assertEqual([p["value"] for p in points], [1000, None, 1000, 1100])
        self.assertEqual([p["segment"] for p in points], [1, 1, 2, 2])

    async def test_rule_edit_preserves_quote_watermark_to_reject_old_inflight_snapshots(self):
        group = await self.service.create_group(self.definition())
        await self.service.evaluate_snapshot(self.snapshot(2))
        await self.service.update_group(group["id"], {"rules": [{"field": "stock_code", "op": "eq", "value": "600000"}]})
        current = await self.service.get_current(group["id"])
        self.assertEqual(current["source_time"], "2026-10-30T10:02:00")
        await self.service.evaluate_snapshot(self.snapshot())
        self.assertEqual((await self.service.get_current(group["id"]))["items"], [])
        await self.service.evaluate_snapshot(self.snapshot(2))
        self.assertEqual(len((await self.service.get_current(group["id"]))["items"]), 1)
