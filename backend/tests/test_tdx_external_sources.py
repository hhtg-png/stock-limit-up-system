import unittest
from datetime import date

from app.services.tdx_external_sources import ExternalStockMove, LwwhyStockMoveProvider


class LwwhyStockMoveProviderTests(unittest.TestCase):
    def test_parse_review_action_card_extracts_move_title_content_and_board_label(self):
        html = """
        <div class="p-2 space-y-1">
          <p>
            <a class="underline text-primary font-medium" href="https://quote.eastmoney.com/sh603989.html?jump_to_web=true">603989</a>
            <a class="underline text-primary font-medium" href="/stock/detail/SH603989">艾华集团</a>
            <span class="badge badge-outline badge-sm badge-error">9天5板</span>
          </p>
          <p class="text-sm">电阻电容+数据中心（MLPC）+算力+充电桩</p>
          <p class="text-sm text-secondary line-clamp-2 break-words" title="1、AI服务器供电及VRM电源。\n2、产品定向配套新能源。">省略内容</p>
        </div>
        """

        moves = LwwhyStockMoveProvider.parse_review_action_html(html, date(2026, 5, 28))

        self.assertEqual(len(moves), 1)
        self.assertEqual(moves[0].stock_code, "603989")
        self.assertEqual(moves[0].stock_name, "艾华集团")
        self.assertEqual(moves[0].board_label, "9天5板")
        self.assertEqual(moves[0].title, "电阻电容+数据中心（MLPC）+算力+充电桩")
        self.assertIn("AI服务器供电", moves[0].content)

    def test_parse_stock_detail_html_extracts_latest_move_section(self):
        html = """
        <div class="border rounded">
          <div class="border-b px-4 py-3">
            <span class="font-bold text-primary">最新异动解析</span>
            <span class="text-secondary text-sm ml-2">(2026-05-28)</span>
          </div>
          <div class="p-4 space-y-1">
            <p>字节算力+算力租赁+数据中心</p>
            <p class="text-secondary" title="1、字节资本开支扩建数据中心。\n2、公司提供租赁服务。\n3、森华易腾提供IDC服务。">省略内容</p>
          </div>
        </div>
        """

        move = LwwhyStockMoveProvider.parse_stock_detail_html(html, "600589")

        self.assertIsInstance(move, ExternalStockMove)
        self.assertEqual(move.stock_code, "600589")
        self.assertEqual(move.trade_date, date(2026, 5, 28))
        self.assertEqual(move.title, "字节算力+算力租赁+数据中心")
        self.assertIn("森华易腾", move.content)

