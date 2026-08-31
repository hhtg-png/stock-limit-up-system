import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.tdx_plugins import router as tdx_plugins_router


class TdxPluginsApiTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(tdx_plugins_router, prefix="/tdx-plugins")
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()

    def test_limit_up_live_endpoint_returns_plugin_payload(self):
        payload = {
            "items": [{"stock_code": "001259", "stock_name": "利仁科技"}],
            "updated_at": datetime(2026, 5, 28, 10, 0, 0).isoformat(),
            "source_status": {"limit_up_pool": "ok"},
            "is_cache": False,
            "warnings": [],
        }

        with patch(
            "app.api.v1.tdx_plugins.tdx_plugin_service.get_limit_up_live",
            AsyncMock(return_value=payload),
        ):
            response = self.client.get("/tdx-plugins/limit-up-live", params={"trade_date": "2026-05-28"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["stock_code"], "001259")
        self.assertEqual(response.json()["source_status"]["limit_up_pool"], "ok")

    def test_limit_up_live_status_endpoint_returns_lightweight_payload(self):
        payload = {
            "items": [{"stock_code": "605177", "stock_name": "东亚药业"}],
            "updated_at": datetime(2026, 5, 29, 14, 25, 0).isoformat(),
            "source_status": {"limit_up_status": "ok", "public_attribution": "skipped"},
            "is_cache": False,
            "warnings": [],
        }

        with patch(
            "app.api.v1.tdx_plugins.tdx_plugin_service.get_limit_up_live_status",
            AsyncMock(return_value=payload),
        ):
            response = self.client.get("/tdx-plugins/limit-up-live/status")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["stock_code"], "605177")
        self.assertEqual(response.json()["source_status"]["public_attribution"], "skipped")

    def test_plate_strength_endpoint_forwards_source_and_window(self):
        payload = {
            "items": [],
            "history": [],
            "source": "ths",
            "window_days": 30,
            "updated_at": datetime(2026, 5, 28, 10, 0, 0).isoformat(),
            "source_status": {"plate_source": "ths"},
            "is_cache": False,
            "warnings": [],
        }

        with patch(
            "app.api.v1.tdx_plugins.tdx_plugin_service.get_plate_strength",
            AsyncMock(return_value=payload),
        ) as get_plate_strength:
            response = self.client.get(
                "/tdx-plugins/plate-strength",
                params={"trade_date": "2026-05-28", "source": "ths", "window_days": 30},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["source"], "ths")
        self.assertEqual(get_plate_strength.await_args.kwargs["source"], "ths")
        self.assertEqual(get_plate_strength.await_args.kwargs["window_days"], 30)

    def test_plate_constituents_endpoint_forwards_plate_and_source(self):
        payload = {
            "plate_name": "房地产",
            "source": "ths",
            "items": [{"stock_code": "000001", "change_pct": 3.2, "dragon_tag": "龙1"}],
            "updated_at": datetime(2026, 5, 28, 10, 0, 0).isoformat(),
            "source_status": {"topic_knowledge": "ok", "tencent_quote": "ok"},
            "is_cache": False,
            "warnings": [],
        }

        with patch(
            "app.api.v1.tdx_plugins.tdx_plugin_service.get_plate_constituents",
            AsyncMock(return_value=payload),
        ) as get_plate_constituents:
            response = self.client.get(
                "/tdx-plugins/plate-constituents",
                params={"plate_name": "房地产", "source": "ths", "trade_date": "2026-05-28"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["dragon_tag"], "龙1")
        self.assertEqual(get_plate_constituents.await_args.args[0], "房地产")
        self.assertEqual(get_plate_constituents.await_args.kwargs["source"], "ths")

    def test_calibration_compare_endpoint_returns_diff_report(self):
        response = self.client.post(
            "/tdx-plugins/calibration/compare",
            json={
                "key_field": "stock_code",
                "target_items": [{"stock_code": "001259", "event_label": "封死涨停"}],
                "ours_items": [{"stock_code": "002421", "event_label": "涨停打开"}],
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"]["target_count"], 1)
        self.assertEqual(payload["missing_items"][0]["stock_code"], "001259")
        self.assertEqual(payload["extra_items"][0]["stock_code"], "002421")


if __name__ == "__main__":
    unittest.main()
