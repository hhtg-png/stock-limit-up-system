import unittest
from datetime import date

from app.services.stock_suspension_service import StockSuspensionService


class StockSuspensionTests(unittest.TestCase):
    def test_excludes_future_resumed_partial_and_invalid_records(self):
        base = {"SECURITY_CODE": "600001", "SUSPEND_START_TIME": "2026-09-09 09:30:00",
                "SUSPEND_END_TIME": None, "PREDICT_RESUME_DATE": None, "SUSPEND_EXPIRE": "连续停牌"}
        rows = [base,
                {**base, "SECURITY_CODE": "600002", "SUSPEND_START_TIME": "2026-09-10 09:30:00"},
                {**base, "SECURITY_CODE": "600003", "PREDICT_RESUME_DATE": "2026-09-09 00:00:00"},
                {**base, "SECURITY_CODE": "600004", "SUSPEND_END_TIME": "2026-09-09 10:30:00"},
                {**base, "SECURITY_CODE": "600005", "SUSPEND_START_TIME": "2026-09-09 10:30:00"},
                {**base, "SECURITY_CODE": "600006", "SUSPEND_EXPIRE": "盘中临时停牌"},
                {**base, "SECURITY_CODE": "600007", "SUSPEND_START_TIME": "invalid"}]
        self.assertEqual(StockSuspensionService.full_session_codes(rows, date(2026, 9, 9)), {"600001"})

    def test_last_suspended_day_and_resume_day_boundaries(self):
        row = {"SECURITY_CODE": "603580", "SUSPEND_START_TIME": "2026-07-07 09:30:00",
               "SUSPEND_END_TIME": "2026-07-13 15:00:00", "PREDICT_RESUME_DATE": "2026-07-14 00:00:00",
               "SUSPEND_EXPIRE": "连续停牌"}
        self.assertEqual(StockSuspensionService.full_session_codes([row], date(2026, 7, 13)), {"603580"})
        self.assertEqual(StockSuspensionService.full_session_codes([row], date(2026, 7, 14)), set())
