import unittest

from app.services.tdx_news_realtime_tracker import TdxNewsRealtimeTracker


class TdxNewsRealtimeTrackerTests(unittest.TestCase):
    def test_first_sync_primes_current_snapshot_without_backlog(self):
        tracker = TdxNewsRealtimeTracker()

        new_items = tracker.collect_new_items([
            {"news_id": "ths-1", "title": "当前已有快讯", "time": "10:00:00"},
        ])

        self.assertEqual(new_items, [])
        self.assertTrue(tracker.has_seen("ths-1"))

    def test_subsequent_sync_returns_only_new_items_oldest_first(self):
        tracker = TdxNewsRealtimeTracker()
        tracker.collect_new_items([
            {"news_id": "ths-1", "title": "已知快讯", "time": "10:00:00"},
        ])

        new_items = tracker.collect_new_items([
            {"news_id": "glh-3", "title": "最新快讯", "time": "10:02:00"},
            {"news_id": "ths-2", "title": "较早快讯", "time": "10:01:00"},
            {"news_id": "ths-1", "title": "已知快讯", "time": "10:00:00"},
        ])

        self.assertEqual([item["news_id"] for item in new_items], ["ths-2", "glh-3"])

    def test_falls_back_to_source_time_title_key_when_id_missing(self):
        tracker = TdxNewsRealtimeTracker()
        tracker.collect_new_items([
            {"source": "格隆汇", "time": "10:00:00", "title": "标题"},
        ])

        new_items = tracker.collect_new_items([
            {"source": "格隆汇", "time": "10:01:00", "title": "新标题"},
            {"source": "格隆汇", "time": "10:00:00", "title": "标题"},
        ])

        self.assertEqual([item["title"] for item in new_items], ["新标题"])


if __name__ == "__main__":
    unittest.main()
