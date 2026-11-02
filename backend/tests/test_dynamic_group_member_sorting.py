"""Display ordering applies to the entire filtered member set before paging."""
import unittest
from datetime import date, datetime

from sqlalchemy import insert, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.dynamic_group import DynamicGroup, DynamicGroupMember, DynamicGroupSelection, DynamicGroupSelectionItem
from app.services.dynamic_group_queries import member_page, selection_page
from app.services.dynamic_group_service import DynamicGroupService


NUMERIC_FIELDS = (
    "current_price", "change_pct", "today_open_change_pct", "yesterday_open_change_pct",
    "turnover_rate", "amount_yi", "continuous_limit_up_days", "yesterday_continuous_days",
)


class DynamicMemberSortingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.payloads = []
        for i in range(65):
            payload = dict(stock_code=f"600{i:03}", stock_name=f"{'目标' if i % 2 == 0 else '其他'}{i:02}",
                           quote_time="2026-10-30T10:00:00")
            if i < 62:
                payload.update({field: (-29 if i == 2 else i - 30) for field in NUMERIC_FIELDS})
                payload["ranking_position"] = 62 - i
            elif i < 64:
                payload.update({field: None if i == 62 else "" for field in NUMERIC_FIELDS})
            # An incoming numeric string must sort numerically too.
            if i == 9:
                payload.update({field: "-21" for field in NUMERIC_FIELDS})
            self.payloads.append(payload)
        async with self.sessions() as session, session.begin():
            self.group = DynamicGroup(name="排序", trade_date=date(2026, 10, 30), member_count=65)
            session.add(self.group)
            await session.flush()
            self.batch = DynamicGroupSelection(group_id=self.group.id, version=1, trade_date=self.group.trade_date,
                selected_at=datetime(2026, 10, 30, 10), member_count=65, kind="initial", batch_key="sort", definition={})
            session.add(self.batch)
            await session.flush()
            await session.execute(insert(DynamicGroupMember), [dict(group_id=self.group.id,
                stock_code=p["stock_code"], entry_event_id=1, entered_at=datetime(2026, 10, 30, 10),
                data_status="ready", payload=p) for p in self.payloads])
            await session.execute(insert(DynamicGroupSelectionItem), [dict(selection_id=self.batch.id,
                stock_code=p["stock_code"], entry_event_id=1, payload=p) for p in self.payloads])

    async def asyncTearDown(self):
        await self.engine.dispose()

    def readers(self):
        return ((member_page, self.group), (selection_page, self.batch))

    async def test_every_numeric_column_sorts_globally_before_paging_with_nulls_last(self):
        async with self.sessions() as session:
            for reader, target in self.readers():
                for field in NUMERIC_FIELDS:
                    with self.subTest(reader=reader.__name__, field=field):
                        asc = await reader(session, target, sort_by=field)
                        asc_second = await reader(session, target, sort_by=field, offset=50)
                        desc = await reader(session, target, sort_by=field, sort_order="desc")
                        desc_second = await reader(session, target, sort_by=field, sort_order="desc", offset=50)
                        self.assertEqual(asc["total"], 65)
                        self.assertEqual(len(asc["items"]), 50)
                        self.assertEqual([p["stock_code"] for p in asc["items"][:3]], ["600000", "600001", "600002"])
                        self.assertEqual(asc["items"][30][field], 0)
                        self.assertEqual(asc_second["items"][0]["stock_code"], "600050")
                        self.assertEqual(desc["items"][0]["stock_code"], "600061")
                        self.assertEqual(desc_second["items"][0]["stock_code"], "600011")
                        for page in (asc_second, desc_second):
                            self.assertEqual([p["stock_code"] for p in page["items"][-3:]], ["600062", "600063", "600064"])
                        self.assertEqual([p["stock_code"] for p in desc_second["items"][-6:-3]], ["600001", "600002", "600000"])

    async def test_search_then_sort_then_page_keeps_original_cohort_summary(self):
        async with self.sessions() as session:
            for reader, target in self.readers():
                baseline = await reader(session, target)
                result = await reader(session, target, q="目标", sort_by="change_pct", offset=10, limit=10)
                self.assertEqual(result["total"], 33)
                self.assertEqual([p["stock_code"] for p in result["items"]],
                    ["600020", "600022", "600024", "600026", "600028", "600030", "600032", "600034", "600036", "600038"])
                self.assertEqual(result["summary"], baseline["summary"])

    async def test_omitted_sort_retains_strategy_ranking_and_stable_unranked_tail(self):
        async with self.sessions() as session:
            for reader, target in self.readers():
                page = await reader(session, target, limit=100, sort_order="desc")
                self.assertEqual([p["stock_code"] for p in page["items"][:3]], ["600061", "600060", "600059"])
                self.assertEqual([p["stock_code"] for p in page["items"][-4:]], ["600000", "600062", "600063", "600064"])

    async def _replace_payloads(self, payloads):
        async with self.sessions() as session, session.begin():
            for model in (DynamicGroupMember, DynamicGroupSelectionItem):
                rows = (await session.scalars(select(model).order_by(model.stock_code))).all()
                for i, row in enumerate(rows):
                    row.payload = dict(stock_code=row.stock_code, **(payloads[i] if i < len(payloads) else {}))

    async def test_price_uses_visible_current_price_then_price_and_preserves_zero(self):
        await self._replace_payloads([
            {"current_price": None, "price": 9}, {"current_price": 0, "price": 99},
            {"price": -2}, {"current_price": 12, "price": 1}, {"current_price": "", "price": 99},
        ])
        async with self.sessions() as session:
            for reader, target in self.readers():
                asc = await reader(session, target, sort_by="current_price", limit=100)
                desc = await reader(session, target, sort_by="current_price", sort_order="desc", limit=100)
                self.assertEqual([p["stock_code"] for p in asc["items"][:5]], ["600002", "600001", "600000", "600003", "600004"])
                self.assertEqual([p["stock_code"] for p in desc["items"][:5]], ["600003", "600000", "600001", "600002", "600004"])

    async def test_time_columns_sort_as_clock_time_and_empty_values_stay_last(self):
        for field in ("yesterday_first_limit_up_time", "yesterday_final_seal_time"):
            await self._replace_payloads([{field: value} for value in ("10:00:00", "09:59:59", "14:50:00", "", None, "09:59:59")])
            async with self.sessions() as session:
                for reader, target in self.readers():
                    asc = await reader(session, target, sort_by=field, limit=100)
                    desc = await reader(session, target, sort_by=field, sort_order="desc", limit=100)
                    self.assertEqual([p["stock_code"] for p in asc["items"][:6]], ["600001", "600005", "600000", "600002", "600003", "600004"])
                    self.assertEqual([p["stock_code"] for p in desc["items"][:6]], ["600002", "600000", "600001", "600005", "600003", "600004"])

    async def test_selected_time_uses_member_display_fallback_not_batch_time_and_never_mutates_archive(self):
        await self._replace_payloads([
            {"selected_at": "2026-10-30T10:00:00", "first_selected_at": "2026-10-27T10:00:00"},
            {"selected_at": "", "first_selected_at": "2026-10-29T10:00:00"},
            {"first_selected_at": "2026-10-28T10:00:00", "batch_selected_at": "2026-10-31T10:00:00"},
            {"batch_selected_at": "2026-10-26T10:00:00"},
        ])
        service = DynamicGroupService(session_factory=self.sessions)
        try:
            async with self.sessions() as session:
                before = list(await session.scalars(select(DynamicGroupSelectionItem.payload).order_by(DynamicGroupSelectionItem.stock_code)))
            pages = (await service.get_current(self.group.id, sort_by="selected_at", limit=100),
                     await service.get_selection(self.group.id, self.batch.id, sort_by="selected_at", limit=100))
            for page in pages:
                self.assertEqual([p["stock_code"] for p in page["items"][:4]], ["600002", "600001", "600000", "600003"])
                self.assertEqual(page["items"][0]["first_selected_at"], "2026-10-28T10:00:00")
            descending = await service.get_selection(self.group.id, self.batch.id,
                sort_by="selected_at", sort_order="desc", limit=100)
            self.assertEqual([p["stock_code"] for p in descending["items"][:4]], ["600000", "600001", "600002", "600003"])
            # Current data can change while the archived payload keeps its own date.
            async with self.sessions() as session, session.begin():
                member = await session.scalar(select(DynamicGroupMember).where(DynamicGroupMember.stock_code == "600000"))
                member.payload = dict(member.payload, selected_at="2026-10-26T10:00:00")
            current = await service.get_current(self.group.id, sort_by="selected_at", limit=100)
            archived = await service.get_selection(self.group.id, self.batch.id, sort_by="selected_at", limit=100)
            self.assertEqual(current["items"][0]["stock_code"], "600000")
            self.assertEqual(archived["items"][0]["stock_code"], "600002")
            async with self.sessions() as session:
                after = list(await session.scalars(select(DynamicGroupSelectionItem.payload).order_by(DynamicGroupSelectionItem.stock_code)))
            self.assertEqual(after, before)
        finally:
            await service.close()

    async def test_text_columns_and_direct_service_arguments_use_allowlisted_sorting(self):
        async with self.sessions() as session:
            for reader, target in self.readers():
                names = await reader(session, target, sort_by="stock_name", sort_order="desc", limit=3)
                codes = await reader(session, target, sort_by="stock_code", sort_order="desc", limit=3)
                self.assertEqual([p["stock_code"] for p in names["items"]], ["600064", "600062", "600060"])
                self.assertEqual([p["stock_code"] for p in codes["items"]], ["600064", "600063", "600062"])
                for kwargs in ({"sort_by": "ranking_position"}, {"sort_by": "stock_code; DROP TABLE dynamic_groups"},
                               {"sort_by": "change_pct", "sort_order": "sideways"}):
                    with self.assertRaises(ValueError):
                        await reader(session, target, **kwargs)

    async def test_postgresql_can_compile_every_member_sort_statement(self):
        class CompileSession:
            def __init__(self, real):
                self.real = real

            async def execute(self, statement):
                statement.compile(dialect=postgresql.dialect())
                return await self.real.execute(statement)

            async def scalar(self, statement):
                statement.compile(dialect=postgresql.dialect())
                return await self.real.scalar(statement)

        async with self.sessions() as real:
            session = CompileSession(real)
            for reader, target in self.readers():
                for field in (*NUMERIC_FIELDS, "stock_name", "stock_code", "selected_at", "yesterday_first_limit_up_time", "yesterday_final_seal_time"):
                    await reader(session, target, sort_by=field, sort_order="desc")
