import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

from app import main as app_main
from app.services.dynamic_group_service import dynamic_group_service
from app.services.dynamic_group_history_service import dynamic_group_history_service
from app.services.dynamic_group_source_service import dynamic_group_source_service


class DynamicGroupLifespanTests(unittest.IsolatedAsyncioTestCase):
    async def test_owned_background_work_is_closed_before_database_even_if_one_cleanup_fails(self):
        order = []
        def close(label, fail=False):
            async def action():
                order.append(label)
                if fail:
                    raise RuntimeError("cleanup failed")
            return AsyncMock(side_effect=action)
        scheduler = MagicMock()
        scheduler.get_trading_calendar_service.return_value.close = AsyncMock()
        with ExitStack() as stack:
            for target, name, value in ((app_main, "setup_logging", MagicMock()),
                    (app_main, "init_db", AsyncMock()), (app_main, "close_db", close("db")),
                    (app_main, "data_scheduler", scheduler), (app_main.settings, "TRADING_PLAYBOOK_ENABLED", False),
                    (app_main.event_bus, "start", AsyncMock()), (app_main.event_bus, "stop", AsyncMock()),
                    (app_main.data_init_service, "initialize", AsyncMock()),
                    (app_main.tencent_api, "close", close("quotes")),
                    (dynamic_group_service, "close", close("groups", fail=True)),
                    (dynamic_group_history_service, "close", close("history")),
                    (dynamic_group_source_service, "close", close("source"))):
                stack.enter_context(patch.object(target, name, value))
            async with app_main.lifespan(app_main.app):
                pass
        self.assertEqual(order[:3], ["groups", "history", "source"])
        self.assertEqual(order[-1], "db")
