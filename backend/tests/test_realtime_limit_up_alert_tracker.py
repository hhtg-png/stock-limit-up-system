import unittest
from datetime import date

from app.services.realtime_limit_up_alert_tracker import RealtimeLimitUpAlertTracker


class RealtimeLimitUpAlertTrackerTests(unittest.TestCase):
    def test_first_snapshot_only_primes_tracker_without_alerts(self):
        tracker = RealtimeLimitUpAlertTracker()

        alerts = tracker.collect_new_alerts(
            [
                {
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "first_limit_up_time": "09:31:25",
                    "limit_up_reason": "银行",
                    "continuous_limit_up_days": 2,
                }
            ],
            trade_date=date(2026, 4, 23),
        )

        self.assertEqual(alerts, [])

    def test_only_new_codes_after_prime_produce_alerts(self):
        tracker = RealtimeLimitUpAlertTracker()
        tracker.collect_new_alerts(
            [
                {
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "first_limit_up_time": "09:31:25",
                    "limit_up_reason": "银行",
                    "continuous_limit_up_days": 2,
                }
            ],
            trade_date=date(2026, 4, 23),
        )

        alerts = tracker.collect_new_alerts(
            [
                {
                    "stock_code": "000001",
                    "stock_name": "平安银行",
                    "first_limit_up_time": "09:31:25",
                    "limit_up_reason": "银行",
                    "continuous_limit_up_days": 2,
                },
                {
                    "stock_code": "300001",
                    "stock_name": "特锐德",
                    "first_limit_up_time": "10:05:00",
                    "limit_up_reason": "充电桩",
                    "continuous_limit_up_days": 1,
                },
            ],
            trade_date=date(2026, 4, 23),
        )

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["stock_code"], "300001")
        self.assertEqual(alerts[0]["stock_name"], "特锐德")
        self.assertEqual(alerts[0]["time"], "10:05:00")
        self.assertEqual(alerts[0]["reason"], "充电桩")
        self.assertEqual(alerts[0]["continuous_days"], 1)

    def test_trade_date_change_resets_seen_codes_without_flooding(self):
        tracker = RealtimeLimitUpAlertTracker()
        tracker.collect_new_alerts(
            [{"stock_code": "000001", "stock_name": "平安银行"}],
            trade_date=date(2026, 4, 23),
        )

        alerts = tracker.collect_new_alerts(
            [{"stock_code": "000001", "stock_name": "平安银行"}],
            trade_date=date(2026, 4, 24),
        )

        self.assertEqual(alerts, [])


if __name__ == "__main__":
    unittest.main()
