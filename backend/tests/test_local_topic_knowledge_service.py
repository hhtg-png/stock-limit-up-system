import unittest

from app.services.local_topic_knowledge_service import LocalTopicKnowledgeService


class LocalTopicKnowledgeServiceTests(unittest.TestCase):
    def test_find_stocks_by_topic_includes_exact_and_related_constituents(self):
        service = LocalTopicKnowledgeService(records={
            "stocks": {
                "000001": {
                    "stock_name": "甲公司",
                    "market": "SZ",
                    "industry": "房地产开发",
                    "topics": [],
                },
                "000002": {
                    "stock_name": "乙公司",
                    "market": "SZ",
                    "topics": [{"theme": "房地产", "aliases": ["地产"]}],
                },
                "000003": {
                    "stock_name": "丙公司",
                    "market": "SZ",
                    "topics": [{"theme": "AI", "aliases": []}],
                },
                "000004": {
                    "stock_name": "丁公司",
                    "market": "SZ",
                    "topics": [{"theme": "AI电源", "aliases": []}],
                },
            }
        })

        real_estate = service.find_stocks_by_topic("房地产")
        ai_power = service.find_stocks_by_topic("AI电源")

        self.assertEqual([item["stock_code"] for item in real_estate], ["000001", "000002"])
        self.assertEqual([item["stock_code"] for item in ai_power], ["000004"])
        self.assertEqual(real_estate[0]["match_reason"], "房地产开发")

    def test_topic_lookup_cache_is_bounded(self):
        stocks = {
            f"{index:06d}": {
                "stock_name": f"公司{index}",
                "topics": [{"theme": f"题材{index:03d}", "aliases": []}],
            }
            for index in range(300)
        }
        service = LocalTopicKnowledgeService(records={"stocks": stocks})

        for index in range(300):
            service.find_stocks_by_topic(f"题材{index:03d}")

        self.assertLessEqual(len(service._topic_lookup_cache), 256)
        self.assertNotIn("题材000", service._topic_lookup_cache)
        self.assertIn("题材299", service._topic_lookup_cache)


if __name__ == "__main__":
    unittest.main()
