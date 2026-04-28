import unittest

from app.database import Base
from app.models import market_review  # noqa: F401


class MarketReviewModelTests(unittest.TestCase):
    def test_market_review_tables_are_registered(self):
        self.assertIn("market_review_daily_metric", Base.metadata.tables)
        self.assertIn("market_review_stock_daily", Base.metadata.tables)
        self.assertIn("market_review_limitup_event", Base.metadata.tables)


if __name__ == "__main__":
    unittest.main()
