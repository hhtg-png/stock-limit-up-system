import unittest
import json
from datetime import date

from app.services.ths_move_analysis_source import ThsMoveAnalysisSource


class ThsMoveAnalysisSourceTests(unittest.TestCase):
    def test_parse_zhangting_html_extracts_stock_and_ths_evidence(self):
        html = """
        <div class="item">
          <a class="dlink" href="http://yuanchuang.10jqka.com.cn/20260616/c677499000.shtml">
            涨停雷达：并购重组+存储芯片+汽车零部件 迪生力触及涨停
          </a>
          <p class="arc-cont">
            影响事件：今日走势：迪生力今日触及涨停板。
            异动原因揭秘：公司拟收购广东全芯半导体30%股权，
            标的公司主营存储芯片封装测试；公司原有汽车零部件业务。
            [详细内容]
          </p>
          <div class="bot-bar">
            <a href="http://stockpage.10jqka.com.cn/603335/">迪生力</a>
            <span>2026-06-16 10:17:00</span>
          </div>
        </div>
        """

        items = ThsMoveAnalysisSource.parse_list_html(html, date(2026, 6, 16))

        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.stock_code, "603335")
        self.assertEqual(item.stock_name, "迪生力")
        self.assertEqual(item.trade_date, date(2026, 6, 16))
        self.assertIn("涨停雷达", item.title)
        self.assertIn("拟收购广东全芯半导体30%股权", item.evidence)
        self.assertNotIn("详细内容", item.evidence)
        self.assertEqual(item.article_url, "http://yuanchuang.10jqka.com.cn/20260616/c677499000.shtml")
        self.assertEqual(item.published_at, "2026-06-16 10:17:00")

    def test_parse_jsonp_response_unwraps_embedded_html(self):
        html = """
        <div class="item">
          <a class="dlink" href="http://yuanchuang.10jqka.com.cn/20260616/c677499001.shtml">
            涨停雷达：PCB铜箔+AI电源 铜箔科技触及涨停
          </a>
          <p class="arc-cont">异动原因揭秘：公司产品用于AI服务器电源和PCB铜箔方向。</p>
          <a href="http://stockpage.10jqka.com.cn/600001/">铜箔科技</a>
          <span>2026-06-16 09:36:00</span>
        </div>
        """.replace("\n", "")
        jsonp = f"callback({json.dumps({'code': 0, 'data': {'html': html}}, ensure_ascii=False)})"

        items = ThsMoveAnalysisSource.parse_jsonp_response(jsonp, date(2026, 6, 16))

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].stock_code, "600001")
        self.assertIn("AI服务器电源", items[0].evidence)


if __name__ == "__main__":
    unittest.main()
