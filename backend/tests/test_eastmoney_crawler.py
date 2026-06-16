import unittest

from app.crawlers.eastmoney_crawler import EastMoneyCrawler


class EastMoneyCrawlerTests(unittest.TestCase):
    def test_parse_prefers_lbc_over_zttj_window_count_for_continuous_days(self):
        crawler = EastMoneyCrawler()
        rows = crawler.parse(
            {
                "data": {
                    "pool": [
                        {
                            "c": "002585",
                            "n": "双星新材",
                            "p": 14010,
                            "amount": 859728912,
                            "ltsz": 12423461843.34,
                            "hs": 6.95,
                            "lbc": 2,
                            "fbt": 93027,
                            "lbt": 93133,
                            "fund": 210888901,
                            "zbc": 1,
                            "hybk": "塑料",
                            "zttj": {"days": 9, "ct": 5},
                        }
                    ]
                }
            },
            is_sealed=True,
        )

        self.assertEqual(rows[0]["continuous_limit_up_days"], 2)
        self.assertEqual(rows[0]["board_label"], "9天5板")

    def test_parse_leaves_continuous_days_unknown_when_only_zttj_window_exists(self):
        crawler = EastMoneyCrawler()
        rows = crawler.parse(
            {
                "data": {
                    "pool": [
                        {
                            "c": "603065",
                            "n": "宿迁联盛",
                            "p": 11000,
                            "amount": 120000000,
                            "ltsz": 2000000000,
                            "hs": 6.0,
                            "lbc": None,
                            "fbt": 95700,
                            "lbt": None,
                            "fund": 0,
                            "zbc": 7,
                            "hybk": "化学制品",
                            "zttj": {"days": 10, "ct": 5},
                        }
                    ]
                }
            },
            is_sealed=False,
        )

        self.assertIsNone(rows[0]["continuous_limit_up_days"])
        self.assertEqual(rows[0]["board_label"], "10天5板")


if __name__ == "__main__":
    unittest.main()
