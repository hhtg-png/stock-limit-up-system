import unittest
from datetime import date

from app.services.realtime_limit_up_stream_tracker import RealtimeLimitUpStreamTracker


class RealtimeLimitUpStreamTrackerTests(unittest.TestCase):
    def _item(
        self,
        stock_code: str,
        stock_name: str,
        *,
        is_sealed: bool = True,
        continuous_limit_up_days: int = 1,
        first_limit_up_time: str = "09:31:25",
        final_seal_time: str | None = "09:45:00",
        seal_amount: float = 1000.0,
    ) -> dict:
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "trade_date": "2026-04-24",
            "first_limit_up_time": first_limit_up_time,
            "final_seal_time": final_seal_time,
            "limit_up_reason": "机器人",
            "reason_category": "人工智能",
            "continuous_limit_up_days": continuous_limit_up_days,
            "open_count": 0 if is_sealed else 1,
            "is_sealed": is_sealed,
            "current_status": "sealed" if is_sealed else "opened",
            "seal_amount": seal_amount,
            "limit_up_price": 10.5,
            "current_price": 10.5 if is_sealed else 10.21,
            "turnover_rate": 6.8,
            "amount": 8888.0,
            "market": "SZ",
            "industry": None,
        }

    def test_first_sync_returns_full_snapshot(self):
        tracker = RealtimeLimitUpStreamTracker()

        message = tracker.sync(
            [self._item("000001", "平安银行")],
            trade_date=date(2026, 4, 24),
        )

        self.assertIsNotNone(message)
        self.assertEqual(message["type"], "limit_up_snapshot")
        self.assertEqual(message["data"]["trade_date"], "2026-04-24")
        self.assertEqual(len(message["data"]["items"]), 1)
        self.assertEqual(message["data"]["items"][0]["stock_code"], "000001")

    def test_subsequent_sync_returns_delta_for_added_updated_and_removed_items(self):
        tracker = RealtimeLimitUpStreamTracker()
        tracker.sync(
            [
                self._item("000001", "平安银行"),
                self._item("300001", "特锐德", continuous_limit_up_days=2),
            ],
            trade_date=date(2026, 4, 24),
        )

        message = tracker.sync(
            [
                self._item("000001", "平安银行", is_sealed=False, seal_amount=0.0, final_seal_time=None),
                self._item("600001", "邯郸钢铁", continuous_limit_up_days=3),
            ],
            trade_date=date(2026, 4, 24),
        )

        self.assertIsNotNone(message)
        self.assertEqual(message["type"], "limit_up_delta")
        self.assertEqual(message["data"]["trade_date"], "2026-04-24")
        self.assertEqual(message["data"]["remove"], ["300001"])

        upsert_codes = [item["stock_code"] for item in message["data"]["upsert"]]
        self.assertEqual(upsert_codes, ["000001", "600001"])
        updated = message["data"]["upsert"][0]
        self.assertFalse(updated["is_sealed"])
        self.assertEqual(updated["current_status"], "opened")

    def test_cached_snapshot_is_available_only_for_current_trade_date(self):
        tracker = RealtimeLimitUpStreamTracker()
        tracker.sync(
            [self._item("000001", "平安银行")],
            trade_date=date(2026, 4, 24),
        )

        current = tracker.get_cached_snapshot(date(2026, 4, 24))
        previous = tracker.get_cached_snapshot(date(2026, 4, 23))

        self.assertIsNotNone(current)
        self.assertEqual(current["type"], "limit_up_snapshot")
        self.assertIsNone(previous)


if __name__ == "__main__":
    unittest.main()
