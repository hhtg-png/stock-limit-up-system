"""HTTP contracts: queued writes and local, ownership-checked archive reads."""
import asyncio
import unittest
from datetime import date, datetime
from unittest.mock import patch

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models.dynamic_group import DynamicGroupMember, DynamicGroupSelection, DynamicGroupSelectionItem
from app.api.v1 import dynamic_groups
from app.services.dynamic_group_service import DynamicGroupService


class SlowSource:
    async def get_snapshot(self):
        await asyncio.Event().wait()


class DynamicGroupApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = DynamicGroupService(session_factory=async_sessionmaker(self.engine, expire_on_commit=False), source=SlowSource())
        self.patcher = patch.object(dynamic_groups, "dynamic_group_service", self.service)
        self.patcher.start()
        app = FastAPI()
        app.include_router(dynamic_groups.router, prefix="/api/v1/dynamic-groups")
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        self.path = "/api/v1/dynamic-groups"

    async def asyncTearDown(self):
        await self.service.close()
        await self.client.aclose()
        self.patcher.stop()
        await self.engine.dispose()

    async def test_create_returns_saved_definition_while_market_scan_is_still_running(self):
        response = await asyncio.wait_for(self.client.post(self.path, json={"name": "排队", "rules": [
            {"field": "change_pct", "op": "gte", "value": 5}]}), timeout=2)
        self.assertEqual(response.status_code, 200)
        group_id = response.json()["id"]
        current = await asyncio.wait_for(self.client.get(f"{self.path}/{group_id}/current"), timeout=2)
        self.assertEqual(current.json()["status"], "pending")
        self.assertEqual(current.json()["items"], [])

    async def test_bounded_paging_unknown_id_and_rule_rejection(self):
        self.assertEqual((await self.client.get(self.path + "?limit=101")).status_code, 422)
        self.assertEqual((await self.client.get(self.path + "/999/current")).status_code, 404)
        invalid = await self.client.post(self.path, json={"name": "bad", "rules": [{"field": "sql", "op": "eq", "value": "DROP"}]})
        self.assertEqual(invalid.status_code, 422)

    async def test_invalid_rule_or_null_patch_returns_validation_error(self):
        group = await self.service.create_group({"name": "test", "rules": [{"field": "change_pct", "op": "gte", "value": 5}]})
        for payload in ({"rules": [{"field": "sql", "op": "eq", "value": "bad"}]}, {"name": None}):
            with self.subTest(payload=payload):
                response = await self.client.put(f"{self.path}/{group['id']}", json=payload)
                self.assertEqual(response.status_code, 422)

    async def test_member_sort_parameters_reject_unlisted_fields_and_directions(self):
        for path in (f"{self.path}/999/current", f"{self.path}/999/selections/999"):
            for params in ({"sort_by": "ranking_position"}, {"sort_by": "change_pct; DROP TABLE dynamic_groups"},
                           {"sort_order": "descending"}, {"sort_by": "change_pct", "sort_order": ""}):
                with self.subTest(path=path, params=params):
                    self.assertEqual((await self.client.get(path, params=params)).status_code, 422)

    async def test_current_and_history_routes_forward_display_sorting(self):
        group = await self.service.create_group({"name": "排序", "rules": [{"field": "change_pct", "op": "gte", "value": 5}]})
        async with self.service.session_factory() as session, session.begin():
            batch = DynamicGroupSelection(group_id=group["id"], version=1, trade_date=date(2026, 10, 30),
                selected_at=datetime(2026, 10, 30, 10), member_count=3, kind="initial", batch_key="sort", definition={})
            session.add(batch)
            await session.flush()
            for code, value in (("600001", 2), ("600002", 10), ("600003", -1)):
                payload = dict(stock_code=code, stock_name="目标", amount_yi=value)
                session.add(DynamicGroupMember(group_id=group["id"], stock_code=code, entry_event_id=1,
                    entered_at=datetime(2026, 10, 30, 10), payload=payload))
                session.add(DynamicGroupSelectionItem(selection_id=batch.id, stock_code=code,
                    entry_event_id=1, payload=payload))
        for path in (f"{self.path}/{group['id']}/current", f"{self.path}/{group['id']}/selections/{batch.id}"):
            response = await self.client.get(path, params=dict(q="目标", sort_by="amount_yi", sort_order="desc", offset=1, limit=1))
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["total"], 3)
            self.assertEqual(response.json()["items"][0]["stock_code"], "600001")
