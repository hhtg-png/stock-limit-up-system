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
