import json
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.dynamic_group import DynamicGroup, DynamicGroupMember, DynamicGroupSelection, DynamicGroupSelectionItem
from app.services.dynamic_group_queries import member_page, selection_page


class DynamicGroupQueryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with self.sessions() as session, session.begin():
            self.group = DynamicGroup(name="宽条件", trade_date=date(2026, 10, 30), member_count=2000)
            session.add(self.group)
            await session.flush()
            rows = [dict(group_id=self.group.id, stock_code=f"60{i:04}", entry_event_id=1,
                entered_at=datetime(2026, 10, 30, 10), data_status="ready", payload={
                    "stock_code": f"60{i:04}", "stock_name": f"观察股{i}", "change_pct": 2,
                    "quote_time": "2026-10-30T10:00:00", "current_price": 10,
                    "trigger_fields": {"large": "x" * 5000}}) for i in range(2000)]
            await session.execute(insert(DynamicGroupMember), rows)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_broad_group_is_paged_and_thin_but_summary_covers_all_members(self):
        async with self.sessions() as session:
            page = await member_page(session, self.group, offset=50, limit=50)
        self.assertEqual(len(page["items"]), 50)
        self.assertEqual(page["total"], 2000)
        self.assertEqual(page["summary"], dict(stock_count=2000, valid_change_count=2000,
            average_change_pct=2, up_count=2000, down_count=0))
        self.assertNotIn("trigger_fields", page["items"][0])
        self.assertLess(len(json.dumps(page).encode()), 50000)
        self.assertEqual(page["items"][0]["stock_code"], "600050")

    async def test_search_is_server_side_literal_and_does_not_change_cohort_summary(self):
        async with self.sessions() as session:
            page = await member_page(session, self.group, q="观察股1999", limit=10)
            empty = await member_page(session, self.group, q="%", limit=10)
        self.assertEqual(page["total"], 1)
        self.assertEqual(page["items"][0]["stock_code"], "601999")
        self.assertEqual(page["summary"]["stock_count"], 2000)
        self.assertEqual(empty["total"], 0)

    async def test_stale_prices_excluded_and_frozen_selection_is_paged(self):
        async with self.sessions() as session, session.begin():
            batch = DynamicGroupSelection(group_id=self.group.id, version=1, trade_date=date(2026, 10, 30),
                selected_at=datetime(2026, 10, 30, 10), member_count=3, kind="initial", batch_key="test", definition={})
            session.add(batch)
            await session.flush()
            for code, change, stamp in (("600001", 10, "2026-10-30T10:00:00"),
                                        ("600002", -5, "2026-10-30T10:00:00"),
                                        ("600003", 40, "2026-10-29T15:00:00")):
                session.add(DynamicGroupSelectionItem(selection_id=batch.id, stock_code=code, entry_event_id=1,
                    payload=dict(stock_code=code, change_pct=change, quote_time=stamp)))
        async with self.sessions() as session:
            page = await selection_page(session, batch, offset=1, limit=1)
        self.assertEqual(page["items"][0]["stock_code"], "600002")
        self.assertEqual(page["total"], 3)
        self.assertEqual(page["summary"], dict(stock_count=3, valid_change_count=2,
            average_change_pct=2.5, up_count=1, down_count=1))

    async def test_public_trend_does_not_repeat_thousands_of_audit_records(self):
        from app.services.dynamic_group_service import DynamicGroupService
        async with self.sessions() as session, session.begin():
            batch = DynamicGroupSelection(group_id=self.group.id, version=1, trade_date=date(2026, 10, 30),
                selected_at=datetime(2026, 10, 30, 10), member_count=2000, kind="initial", batch_key="large", definition={})
            session.add(batch)
            await session.flush()
        history = type("History", (), {})()
        history.get_series = AsyncMock(return_value=dict(status="ready", points=[], expected_count=2000,
            provenance=[dict(stock_code=f"60{i:04}", digest="x" * 64) for i in range(2000)]))
        service = DynamicGroupService(session_factory=self.sessions, history=history)
        result = await service.get_trend(self.group.id, batch.id)
        self.assertFalse("provenance" in result)
        self.assertEqual(result["expected_count"], 2000)
