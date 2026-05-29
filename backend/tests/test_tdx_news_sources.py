import time
import unittest
import asyncio

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

    def test_parse_stcn_response_maps_people_finance_as_target_times_news(self):
        provider = PublicMarketNewsProvider()

        items = provider.parse_stcn_response({
            "state": 1,
            "data": [
                {
                    "id": "3934743",
                    "title": "中国中铁与四川省签署深化战略合作协议",
                    "source": "人民财讯",
                    "time": 1780050408000,
                    "content": "人民财讯5月29日电，中国中铁与四川省签署深化战略合作协议。",
                    "url": "/article/detail/3934743.html",
                    "tags": [
                        [
                            {"name": "战略合作"},
                            {"name": "轨道交通"},
                        ],
                        [
                            {"name": "中国中铁", "stock_code": "sh601390", "code": "SH"},
                        ],
                    ],
                }
            ],
        })

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].source, "时报快讯")
        self.assertEqual(items[0].news_id, "stcn-3934743")
        self.assertEqual(items[0].related_stocks, ["601390"])
        self.assertEqual(items[0].related_plates, ["战略合作", "轨道交通"])
        self.assertEqual(items[0].jump_url, "https://www.stcn.com/article/detail/3934743.html")

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

    def test_parse_gelonghui_response_maps_live_news_items(self):
        provider = PublicMarketNewsProvider()

        items = provider.parse_gelonghui_response({
            "statusCode": 200,
            "result": [
                {
                    "id": 2474876,
                    "title": "罗博特科：股东宁波科骏已减持1.313%股份",
                    "content": "格隆汇5月29日｜罗博特科公告，自2026年4月7日至2026年5月29日，持股5%以上股东宁波科骏通过大宗交易减持公司股份220.15万股。",
                    "createTime": 1780052186,
                    "level": 0,
                    "stockList": [
                        {
                            "stockType": "SZ",
                            "stockCode": "300757",
                            "stockName": "罗博特科",
                        }
                    ],
                },
                {
                    "id": 2474878,
                    "title": "",
                    "content": "格隆汇5月29日｜美联储施密德：我们目前的货币政策并不十分紧缩，可能需要考虑如何使货币政策更加紧缩。",
                    "createTime": 1780052241,
                    "level": 0,
                    "stockList": None,
                }
            ],
        })

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].source, "格隆汇")
        self.assertEqual(items[0].news_id, "glh-2474876")
        self.assertEqual(items[0].title, "罗博特科：股东宁波科骏已减持1.313%股份")
        self.assertEqual(items[0].published_at, 1780052186)
        self.assertEqual(items[0].related_stocks, ["300757"])
        self.assertEqual(items[0].jump_url, "https://www.gelonghui.com/live/2474876")
        self.assertEqual(items[1].title, "美联储施密德：我们目前的货币政策并不十分紧缩，可能需要考虑如何使货币政策更加紧缩")

    def test_fetch_gelonghui_uses_public_live_channel_api(self):
        provider = PublicMarketNewsProvider()

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"statusCode": 200, "result": []}

        class FakeClient:
            def __init__(self):
                self.request = {}

            async def get(self, url, headers):
                self.request = {"url": url, "headers": headers}
                return FakeResponse()

        client = FakeClient()
        asyncio.run(provider._fetch_gelonghui(client))

        self.assertEqual(client.request["url"], provider.GLH_URL)
        self.assertEqual(client.request["headers"]["Referer"], "https://www.gelonghui.com/live/")

    def test_parse_jygs_response_skips_pinned_community_posts(self):
        provider = PublicMarketNewsProvider()

        items = provider.parse_jygs_response({
            "errCode": "0",
            "data": {
                "result": [
                    {
                        "article_id": "pinned",
                        "title": "长期置顶规则帖",
                        "content": "不是动态快讯",
                        "create_time": "2022-05-24 15:58:09",
                        "new_interaction_time": "2026-05-29 16:00:36",
                        "is_top": 1,
                    },
                    {
                        "article_id": "latest",
                        "title": "盘后新增消息",
                        "content": "真实动态内容",
                        "create_time": "2026-05-29 18:36:56",
                        "is_top": 0,
                    },
                ]
            },
        })

        self.assertEqual([item.news_id for item in items], ["jygs-latest"])

    def test_fetch_jygs_uses_study_publish_latest_params(self):
        provider = PublicMarketNewsProvider()

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"errCode": "0", "data": {"result": []}}

        class FakeClient:
            def __init__(self):
                self.request = {}

            async def post(self, url, json, headers):
                self.request = {"url": url, "json": json, "headers": headers}
                return FakeResponse()

        client = FakeClient()
        asyncio.run(provider._fetch_jygs(client))

        self.assertEqual(client.request["url"], provider.JYGS_URL)
        self.assertEqual(client.request["json"], {
            "type": 0,
            "category_id": "",
            "limit": 30,
            "start": 1,
            "order": 0,
            "back_garden": 0,
        })
        self.assertEqual(client.request["headers"]["Referer"], "https://www.jiuyangongshe.com/study_publish")

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

    def test_merge_market_feeds_uses_cls_only_when_primary_sources_are_empty(self):
        provider = PublicMarketNewsProvider()
        stcn = MarketNewsItem(
            news_id="stcn-1",
            source="时报快讯",
            title="时报快讯标题",
            content="人民财讯正文",
            published_at=1780050000,
        )
        ths = MarketNewsItem(
            news_id="ths-1",
            source="同花顺",
            title="同花顺标题",
            content="同花顺正文",
            published_at=1780050100,
        )
        glh = MarketNewsItem(
            news_id="glh-1",
            source="格隆汇",
            title="格隆汇标题",
            content="格隆汇正文",
            published_at=1780050150,
        )
        cls = MarketNewsItem(
            news_id="cls-1",
            source="财联社",
            title="财联社标题",
            content="财联社正文",
            published_at=1780050200,
        )

        primary_items = provider.merge_market_feeds({
            "stcn": [stcn],
            "ths": [ths],
            "glh": [glh],
            "jygs": [],
            "cls": [cls],
        }, limit=10)
        fallback_items = provider.merge_market_feeds({
            "stcn": [],
            "ths": [],
            "glh": [],
            "jygs": [],
            "cls": [cls],
        }, limit=10)

        self.assertEqual([item.news_id for item in primary_items], ["glh-1", "ths-1", "stcn-1"])
        self.assertEqual([item.news_id for item in fallback_items], ["cls-1"])

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
