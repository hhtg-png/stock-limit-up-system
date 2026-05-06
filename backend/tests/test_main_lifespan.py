import unittest
import sys
import types
from datetime import timedelta, tzinfo
from unittest.mock import AsyncMock, MagicMock, patch

apscheduler_module = types.ModuleType("apscheduler")
schedulers_module = types.ModuleType("apscheduler.schedulers")
asyncio_module = types.ModuleType("apscheduler.schedulers.asyncio")
triggers_module = types.ModuleType("apscheduler.triggers")
cron_module = types.ModuleType("apscheduler.triggers.cron")
interval_module = types.ModuleType("apscheduler.triggers.interval")


class StubAsyncIOScheduler:
    def add_job(self, *args, **kwargs):
        return None

    def start(self):
        return None

    def shutdown(self):
        return None


class StubCronTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class StubIntervalTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


asyncio_module.AsyncIOScheduler = StubAsyncIOScheduler
cron_module.CronTrigger = StubCronTrigger
interval_module.IntervalTrigger = StubIntervalTrigger

sys.modules.setdefault("apscheduler", apscheduler_module)
sys.modules.setdefault("apscheduler.schedulers", schedulers_module)
sys.modules.setdefault("apscheduler.schedulers.asyncio", asyncio_module)
sys.modules.setdefault("apscheduler.triggers", triggers_module)
sys.modules.setdefault("apscheduler.triggers.cron", cron_module)
sys.modules.setdefault("apscheduler.triggers.interval", interval_module)


class StubShanghaiTimezone(tzinfo):
    zone = "Asia/Shanghai"

    def utcoffset(self, dt):
        return timedelta(hours=8)

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return self.zone


pytz_module = types.ModuleType("pytz")
pytz_module.timezone = lambda _: StubShanghaiTimezone()
sys.modules.setdefault("pytz", pytz_module)

from app import main as app_main


class MainLifespanTests(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_starts_and_stops_data_scheduler(self):
        scheduler = MagicMock()

        async def noop_initialize():
            return None

        def consume_task(coro):
            coro.close()
            return MagicMock()

        with patch.object(app_main, "setup_logging"), patch.object(
            app_main, "init_db", AsyncMock()
        ), patch.object(app_main, "close_db", AsyncMock()), patch.object(
            app_main.event_bus, "start", AsyncMock()
        ), patch.object(
            app_main.event_bus, "stop", AsyncMock()
        ), patch.object(
            app_main.data_init_service, "initialize", side_effect=noop_initialize
        ), patch.object(
            app_main.asyncio, "create_task", side_effect=consume_task
        ), patch.object(
            app_main, "data_scheduler", scheduler
        ):
            async with app_main.lifespan(app_main.app):
                scheduler.start.assert_called_once_with()

        scheduler.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
