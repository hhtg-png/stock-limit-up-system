import unittest

from sqlalchemy import UniqueConstraint

from app.database import Base
import app.models  # noqa: F401


class MarketReviewModelTests(unittest.TestCase):
    def test_market_review_tables_are_registered(self):
        stock_daily = Base.metadata.tables["market_review_stock_daily"]
        limitup_event = Base.metadata.tables["market_review_limitup_event"]
        stock_daily_uniques = {
            tuple(constraint.columns.keys())
            for constraint in stock_daily.constraints
            if isinstance(constraint, UniqueConstraint)
        }
        limitup_event_uniques = {
            tuple(constraint.columns.keys())
            for constraint in limitup_event.constraints
            if isinstance(constraint, UniqueConstraint)
        }

        self.assertIn("market_review_daily_metric", Base.metadata.tables)
        self.assertIn("market_review_stock_daily", Base.metadata.tables)
        self.assertIn("market_review_limitup_event", Base.metadata.tables)
        self.assertIn("stock_id", stock_daily.c)
        self.assertIn("stock_id", limitup_event.c)
        self.assertEqual(
            sorted(fk.target_fullname for fk in stock_daily.c.stock_id.foreign_keys),
            ["stocks.id"],
        )
        self.assertEqual(
            sorted(fk.target_fullname for fk in limitup_event.c.stock_id.foreign_keys),
            ["stocks.id"],
        )
        self.assertIn(("trade_date", "stock_code"), stock_daily_uniques)
        self.assertIn(("trade_date", "stock_code", "event_type", "event_seq"), limitup_event_uniques)
        self.assertNotIn(("trade_date", "stock_id"), stock_daily_uniques)
        self.assertNotIn(("trade_date", "stock_id", "event_type", "event_seq"), limitup_event_uniques)


if __name__ == "__main__":
    unittest.main()
