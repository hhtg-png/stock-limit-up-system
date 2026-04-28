import unittest

from app.database import Base
import app.models  # noqa: F401


class MarketReviewModelTests(unittest.TestCase):
    def test_market_review_tables_are_registered(self):
        stock_daily = Base.metadata.tables["market_review_stock_daily"]
        limitup_event = Base.metadata.tables["market_review_limitup_event"]

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


if __name__ == "__main__":
    unittest.main()
