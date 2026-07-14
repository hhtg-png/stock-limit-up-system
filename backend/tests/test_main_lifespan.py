import unittest
import sys
import types
from datetime import date, timedelta, tzinfo
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

apscheduler_module = types.ModuleType("apscheduler")
schedulers_module = types.ModuleType("apscheduler.schedulers")
asyncio_module = types.ModuleType("apscheduler.schedulers.asyncio")
triggers_module = types.ModuleType("apscheduler.triggers")
cron_module = types.ModuleType("apscheduler.triggers.cron")
date_module = types.ModuleType("apscheduler.triggers.date")
interval_module = types.ModuleType("apscheduler.triggers.interval")
_installed_stub_modules = []


def _install_stub(name, module):
    if name not in sys.modules:
        sys.modules[name] = module
        _installed_stub_modules.append(name)


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


class StubDateTrigger:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


asyncio_module.AsyncIOScheduler = StubAsyncIOScheduler
cron_module.CronTrigger = StubCronTrigger
date_module.DateTrigger = StubDateTrigger
interval_module.IntervalTrigger = StubIntervalTrigger

try:
    import apscheduler  # noqa: F401
except ImportError:
    _install_stub("apscheduler", apscheduler_module)
    _install_stub("apscheduler.schedulers", schedulers_module)
    _install_stub("apscheduler.schedulers.asyncio", asyncio_module)
    _install_stub("apscheduler.triggers", triggers_module)
    _install_stub("apscheduler.triggers.cron", cron_module)
    _install_stub("apscheduler.triggers.date", date_module)
    _install_stub("apscheduler.triggers.interval", interval_module)


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
try:
    import pytz  # noqa: F401
except ImportError:
    _install_stub("pytz", pytz_module)

from app import main as app_main
from app.services.trading_playbook.runtime import trading_playbook_runtime
from app.services.trading_playbook.serialization import ValidatedPlanPayload

for _stub_name in reversed(_installed_stub_modules):
    sys.modules.pop(_stub_name, None)


class LifecycleScheduler:
    class Calendar:
        def __init__(self, *, ensure_error=None):
            self.ensure_calls = []
            self.close_calls = 0
            self.ensure_error = ensure_error

        async def ensure_date(self, value):
            self.ensure_calls.append(value)
            if self.ensure_error is not None:
                raise self.ensure_error

        def next_trade_date(self, value):
            return value + timedelta(days=1)

        async def close(self):
            self.close_calls += 1

    def __init__(self, *, start_error=None, calendar_error=None):
        self.orchestrator = None
        self.start_calls = 0
        self.stop_calls = 0
        self.reset_calls = 0
        self.start_error = start_error
        self.calendar = self.Calendar(ensure_error=calendar_error)

    def install_trading_playbook_orchestrator(self, orchestrator):
        self.orchestrator = orchestrator

    def get_trading_playbook_orchestrator(self):
        return self.orchestrator

    def reset_trading_playbook_services(self):
        self.reset_calls += 1
        self.orchestrator = None

    def get_trading_calendar_service(self):
        return self.calendar

    def start(self):
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    def stop(self):
        self.stop_calls += 1


class MainLifespanTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        trading_playbook_runtime.reset()
        if hasattr(app_main.app.state, "trading_playbook_orchestrator"):
            delattr(app_main.app.state, "trading_playbook_orchestrator")

    def tearDown(self):
        trading_playbook_runtime.reset()
        if hasattr(app_main.app.state, "trading_playbook_orchestrator"):
            delattr(app_main.app.state, "trading_playbook_orchestrator")

    @staticmethod
    def _lifecycle_patches(scheduler):
        real_create_task = __import__("asyncio").create_task

        async def noop_initialize():
            return None

        def consume_task(coro):
            return real_create_task(coro)

        return (
            patch.object(app_main, "setup_logging"),
            patch.object(app_main, "init_db", AsyncMock()),
            patch.object(app_main, "close_db", AsyncMock()),
            patch.object(app_main.event_bus, "start", AsyncMock()),
            patch.object(app_main.event_bus, "stop", AsyncMock()),
            patch.object(
                app_main.data_init_service,
                "initialize",
                side_effect=noop_initialize,
            ),
            patch.object(
                app_main.asyncio,
                "create_task",
                side_effect=consume_task,
            ),
            patch.object(app_main, "data_scheduler", scheduler),
        )

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

    async def test_real_mounted_app_resolves_same_startup_orchestrator_and_cleans_up(self):
        scheduler = LifecycleScheduler()
        sentinel = types.SimpleNamespace(
            build_stage=AsyncMock(
                return_value=ValidatedPlanPayload(
                    {"pipeline": "startup-sentinel"}
                )
            )
        )
        patches = self._lifecycle_patches(scheduler)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patch.object(
            app_main.settings,
            "TRADING_PLAYBOOK_ENABLED",
            True,
        ), patch.object(
            app_main,
            "build_production_trading_playbook_orchestrator",
            return_value=sentinel,
        ) as factory:
            async with app_main.lifespan(app_main.app):
                self.assertIs(scheduler.orchestrator, sentinel)
                self.assertIs(
                    trading_playbook_runtime.get_orchestrator(),
                    sentinel,
                )
                self.assertIs(
                    app_main.app.state.trading_playbook_orchestrator,
                    sentinel,
                )
                transport = httpx.ASGITransport(app=app_main.app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://test",
                ) as client:
                    response = await client.post(
                        "/api/v1/trading-playbook/plans/generate",
                        json={
                            "source_trade_date": "2026-07-14",
                            "stage": "after_close",
                        },
                    )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json(),
                    {"pipeline": "startup-sentinel"},
                )
                sentinel.build_stage.assert_awaited_once()

        factory.assert_called_once()
        self.assertEqual(scheduler.start_calls, 1)
        self.assertEqual(scheduler.stop_calls, 1)
        self.assertEqual(scheduler.reset_calls, 2)
        self.assertEqual(len(scheduler.calendar.ensure_calls), 1)
        self.assertEqual(scheduler.calendar.close_calls, 1)
        self.assertIsNone(trading_playbook_runtime.get_orchestrator())
        self.assertFalse(
            hasattr(app_main.app.state, "trading_playbook_orchestrator")
        )

    async def test_disabled_startup_keeps_generate_endpoint_controlled_503(self):
        scheduler = LifecycleScheduler()
        patches = self._lifecycle_patches(scheduler)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patch.object(
            app_main.settings,
            "TRADING_PLAYBOOK_ENABLED",
            False,
        ), patch.object(
            app_main,
            "build_production_trading_playbook_orchestrator",
        ) as factory:
            async with app_main.lifespan(app_main.app):
                transport = httpx.ASGITransport(app=app_main.app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://test",
                ) as client:
                    response = await client.post(
                        "/api/v1/trading-playbook/plans/generate",
                        json={
                            "source_trade_date": "2026-07-14",
                            "stage": "after_close",
                        },
                    )

        self.assertEqual(response.status_code, 503)
        factory.assert_not_called()
        self.assertIsNone(scheduler.orchestrator)
        self.assertIsNone(trading_playbook_runtime.get_orchestrator())

    async def test_calendar_warm_failure_is_logged_and_startup_continues(self):
        scheduler = LifecycleScheduler(
            calendar_error=RuntimeError("calendar offline")
        )
        sentinel = types.SimpleNamespace(build_stage=AsyncMock())
        patches = self._lifecycle_patches(scheduler)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patch.object(
            app_main.settings,
            "TRADING_PLAYBOOK_ENABLED",
            True,
        ), patch.object(
            app_main,
            "build_production_trading_playbook_orchestrator",
            return_value=sentinel,
        ) as factory, patch.object(app_main.logger, "error") as log_error:
            async with app_main.lifespan(app_main.app):
                self.assertEqual(scheduler.start_calls, 1)
                self.assertIs(scheduler.orchestrator, sentinel)

        factory.assert_called_once_with(
            next_trade_date=scheduler.calendar.next_trade_date,
        )
        self.assertTrue(
            any(
                "Trading calendar warm-up failed" in str(call.args[0])
                for call in log_error.call_args_list
            )
        )
        self.assertEqual(scheduler.calendar.close_calls, 1)

    async def test_startup_failure_still_resets_registry_scheduler_and_database(self):
        scheduler = LifecycleScheduler(start_error=RuntimeError("start failed"))
        sentinel = types.SimpleNamespace(build_stage=AsyncMock())
        patches = self._lifecycle_patches(scheduler)
        with patches[0], patches[1], patches[2] as close_db, patches[3], patches[4] as stop_bus, patches[5], patches[6], patches[7], patch.object(
            app_main.settings,
            "TRADING_PLAYBOOK_ENABLED",
            True,
        ), patch.object(
            app_main,
            "build_production_trading_playbook_orchestrator",
            return_value=sentinel,
        ):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                async with app_main.lifespan(app_main.app):
                    self.fail("lifespan must not yield after startup failure")

        self.assertEqual(scheduler.stop_calls, 1)
        self.assertEqual(scheduler.reset_calls, 2)
        self.assertIsNone(trading_playbook_runtime.get_orchestrator())
        stop_bus.assert_awaited_once()
        close_db.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
