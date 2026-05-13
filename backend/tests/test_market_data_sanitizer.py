import unittest

from app.utils.market_data_sanitizer import normalize_change_pct


class MarketDataSanitizerTests(unittest.TestCase):
    def test_normalize_change_pct_rejects_no_price_sentinel(self):
        self.assertIsNone(
            normalize_change_pct(
                -100.0,
                price=0,
                amount=0,
            )
        )

    def test_normalize_change_pct_keeps_valid_limit_move(self):
        self.assertEqual(
            normalize_change_pct(
                10.028050422668457,
                price=11770,
                amount=123456.0,
            ),
            10.03,
        )


if __name__ == "__main__":
    unittest.main()
