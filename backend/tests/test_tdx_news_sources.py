import unittest
import time

from app.services.tdx_news_sources import MarketNewsItem, PublicMarketNewsProvider


class TdxNewsSourcesTests(unittest.TestCase):
    def test_parse_ths_response_maps_realtime_tags_and_time(self):
        provider = PublicMarketNewsProvider()

        items = provider.parse_ths_response({
            "data": {
                "list": [
                    {
                        "id": "4423517",
                        "seq": "677069372",
                        "title": "闻泰科技董事长：安世中国独立运营体系已基本完成搭建",
                        "digest": "闻泰科技董事长在会议上宣布。",
                        "rtime": "1780023472",
                        "url": "https://news.10jqka.com.cn/20260529/c677069372.shtml",
                        "import": "3",
                        "stock": [{"name": "*ST闻泰", "stockCode": "600745"}],
                        "field": [{"name": "半导体", "stockCode": "885xxx"}],
                    }
                ]
            }
        })

        self.assertEqual(len(items), 1)
        self.assertIsInstance(items[0], MarketNewsItem)
        self.assertEqual(items[0].source, "同花顺")
        self.assertEqual(items[0].news_id, "ths-677069372")
        self.assertEqual(items[0].related_stocks, ["600745"])
        self.assertEqual(items[0].related_plates, ["半导体"])
        self.assertGreater(items[0].importance, 70)

    def test_parse_cls_response_maps_signed_roll_payload(self):
        provider = PublicMarketNewsProvider()

        items = provider.parse_cls_response({
            "errno": 0,
            "data": {
                "roll_data": [
                    {
                        "id": "10001",
                        "title": "财联社5月29日电，市场消息。",
                        "content": "财联社电报正文",
                        "ctime": 1780023600,
                        "level": "A",
                    }
                ]
            },
        })

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source, "财联社")
        self.assertEqual(items[0].news_id, "cls-10001")
        self.assertEqual(items[0].content, "财联社电报正文")
        self.assertGreaterEqual(items[0].importance, 80)

    def test_parse_jygs_response_maps_public_community_items(self):
        provider = PublicMarketNewsProvider()

        items = provider.parse_jygs_response({
            "errCode": "0",
            "data": {
                "result": [
                    {
                        "article_id": "abc123",
                        "title": "A股盘前纪要",
                        "content": "电力板块走高，华能蒙电涨停。",
                        "create_time": "2026-05-29 09:05:24",
                        "stock_list": [{"name": "深南电A", "code": "sz000037"}],
                    }
                ]
            },
        })

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source, "韭研公社")
        self.assertEqual(items[0].news_id, "jygs-abc123")
        self.assertEqual(items[0].related_stocks, ["000037"])
        self.assertIn("电力板块", items[0].content)

    def test_merge_sources_dedupes_by_title_and_sorts_latest_first(self):
        provider = PublicMarketNewsProvider()

        older = MarketNewsItem(
            news_id="ths-1",
            source="同花顺",
            title="同一标题",
            content="旧内容",
            published_at=1780023000,
            importance=60,
        )
        newer = MarketNewsItem(
            news_id="cls-1",
            source="财联社",
            title="同一标题",
            content="新内容",
            published_at=1780023600,
            importance=80,
        )
        latest = MarketNewsItem(
            news_id="jygs-1",
            source="韭研公社",
            title="另一个标题",
            content="最新内容",
            published_at="2026-05-29 11:10:00",
            importance=70,
        )

        items = provider.merge_sources([older], [newer], [latest], limit=10)

        self.assertEqual([item.news_id for item in items], ["jygs-1", "cls-1"])

    def test_cache_keeps_full_news_batch_when_first_request_is_small(self):
        provider = PublicMarketNewsProvider()
        cached_items = [
            {"news_id": f"ths-{index}", "source": "同花顺", "title": str(index)}
            for index in range(3)
        ]
        provider._cache = (time.time(), cached_items, {"ths": "ok"}, [])

        items, _status, _warnings = provider.get_cached_news(limit=2)

        self.assertEqual([item["news_id"] for item in items], ["ths-0", "ths-1"])
        self.assertEqual(len(provider._cache[1]), 3)


if __name__ == "__main__":
    unittest.main()
