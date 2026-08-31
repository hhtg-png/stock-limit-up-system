import unittest
from collections import Counter

from app.scripts.generate_local_stock_topic_knowledge import (
    StockProfile,
    add_or_merge_topic,
    apply_seed_payload,
    build_payload,
    has_manual_topics,
    is_valid_generated_theme,
    merge_manual_record,
)


class LocalTopicKnowledgeGenerationTests(unittest.TestCase):
    def test_seed_payload_adds_stock_and_extracts_local_topics(self):
        profiles = {}
        count = apply_seed_payload(
            profiles,
            {
                "payload": {
                    "items": [
                        {
                            "stock_code": "603186",
                            "stock_name": "华正新材",
                            "industry": "电子材料",
                            "concepts": ["ABF膜", "覆铜板"],
                            "reasons": [
                                {
                                    "title": "ABF膜+先进封装",
                                    "content": "本地seed缓存字段，不调用模型。",
                                }
                            ],
                        }
                    ]
                }
            },
        )

        self.assertEqual(count, 1)
        self.assertIn("603186", profiles)
        profile = profiles["603186"]
        self.assertEqual(profile.stock_name, "华正新材")
        self.assertEqual(profile.market, "SH")
        self.assertIn("ABF膜", profile.topics)
        self.assertIn("覆铜板", profile.topics)
        self.assertIn(profile.topics["ABF膜"].source, {"local_seed_concept", "local_seed_reason"})

    def test_manual_topics_are_preserved_with_high_confidence(self):
        profile = StockProfile(stock_code="603186", stock_name="华正新材", market="SH")
        add_or_merge_topic(
            profile,
            "覆铜板",
            source="local_seed_concept",
            confidence=0.62,
            evidence="本地seed概念：覆铜板",
        )

        merge_manual_record(
            profile,
            {
                "stock_name": "华正新材",
                "topics": [
                    {
                        "theme": "ABF膜",
                        "source": "local_codex",
                        "confidence": 0.86,
                        "evidence": "本地Codex题材知识库：人工维护。",
                    }
                ],
            },
        )

        self.assertIn("ABF膜", profile.topics)
        self.assertEqual(profile.topics["ABF膜"].source, "local_codex")
        self.assertGreaterEqual(profile.topics["ABF膜"].confidence, 0.86)
        self.assertIn("人工维护", profile.topics["ABF膜"].evidence)
        self.assertIn("覆铜板", profile.topics)

    def test_auto_generated_topics_are_not_treated_as_manual_records(self):
        self.assertFalse(
            has_manual_topics(
                {
                    "stock_name": "平安银行",
                    "topics": [
                        {
                            "theme": "全国性银行",
                            "source": "local_seed_concept",
                            "confidence": 0.62,
                        }
                    ],
                }
            )
        )
        self.assertTrue(
            has_manual_topics(
                {
                    "stock_name": "华正新材",
                    "topics": [
                        {
                            "theme": "ABF膜",
                            "source": "local_codex",
                            "confidence": 0.86,
                        }
                    ],
                }
            )
        )

    def test_build_payload_keeps_empty_topic_stocks_for_full_universe(self):
        payload = build_payload(
            {
                "000001": StockProfile(stock_code="000001", stock_name="平安银行", market="SZ"),
                "603186": StockProfile(stock_code="603186", stock_name="华正新材", market="SH"),
            },
            Counter({"db_stock_count": 1, "seed_new_stock_count": 1}),
        )

        self.assertEqual(payload["version"], 2)
        self.assertEqual(payload["universe"]["stock_count"], 2)
        self.assertEqual(payload["universe"]["topic_stock_count"], 0)
        self.assertEqual(payload["universe"]["empty_topic_stock_count"], 2)
        self.assertEqual(payload["stocks"]["000001"]["topics"], [])
        self.assertEqual(payload["stocks"]["603186"]["topics"], [])

    def test_generated_theme_filter_rejects_seed_noise(self):
        self.assertFalse(is_valid_generated_theme("1"))
        self.assertFalse(is_valid_generated_theme("公司所属行业为：电池"))
        self.assertFalse(is_valid_generated_theme("板块涨跌幅比例将从5%调整为10%"))
        self.assertTrue(is_valid_generated_theme("ABF膜"))


if __name__ == "__main__":
    unittest.main()
